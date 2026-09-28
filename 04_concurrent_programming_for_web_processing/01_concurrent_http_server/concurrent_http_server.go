package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net/http"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

// HTTPRequest 処理対象のHTTPリクエストです
type HTTPRequest struct {
	ID        string
	Method    string
	Path      string
	Headers   map[string]string
	Body      []byte
	ClientIP  string
	UserAgent string
	Timestamp time.Time
}

// HTTPResponse 処理済みHTTPレスポンスです
type HTTPResponse struct {
	RequestID   string
	StatusCode  int
	Headers     map[string]string
	Body        []byte
	ProcessTime time.Duration
	HandlerName string
	Success     bool
	Error       error
	CompletedAt time.Time
}

// ConcurrentHTTPServer ワーカープールでリクエストを処理する並行HTTPサーバーです
type ConcurrentHTTPServer struct {
	// サーバー設定
	address       string
	port          int
	maxWorkers    int
	requestBuffer int

	// HTTP処理
	server *http.Server

	// 並行処理制御
	requestQueue  chan *HTTPRequestContext
	responseQueue chan *HTTPResponse
	workerPool    []*HTTPWorker

	// コンテキスト制御
	ctx    context.Context
	cancel context.CancelFunc

	// 同期制御
	wg sync.WaitGroup

	// 統計・監視
	stats      *ServerStats
	middleware *MiddlewareManager

	// 制御フラグ
	isRunning atomic.Bool
	startTime time.Time
}

// HTTPRequestContext リクエストコンテキストです
type HTTPRequestContext struct {
	Request     *HTTPRequest
	HTTPRequest *http.Request
	Context     context.Context
	StartTime   time.Time
	// result 処理結果をServeHTTPへ渡すチャネルです。ResponseWriterへの書き込みは
	// ServeHTTPを呼び出したgoroutineだけが行い、ワーカーは直接触りません。
	result chan *HTTPResponse
}

// HTTPWorker 並行HTTPワーカーです
type HTTPWorker struct {
	ID       int
	server   *ConcurrentHTTPServer
	stats    *WorkerStats
	handlers map[string]HandlerFunc
}

// HandlerFunc ハンドラー関数です
type HandlerFunc func(ctx context.Context, req *HTTPRequest) (*HTTPResponse, error)

// ServerStats サーバー統計です
type ServerStats struct {
	mu                  sync.RWMutex
	totalRequests       atomic.Int64
	activeRequests      atomic.Int64
	completedRequests   atomic.Int64
	errorRequests       atomic.Int64
	averageResponseTime time.Duration
	statusCodeCounts    map[int]int64
	pathCounts          map[string]int64
	startTime           time.Time
}

// WorkerStats ワーカー統計です。フィールドはワーカーgoroutineと監視・API側から
// 並行に読み書きされるため、すべて型付きatomicで保持します。
type WorkerStats struct {
	WorkerID          int
	ProcessedRequests atomic.Int64
	SuccessRequests   atomic.Int64
	ErrorRequests     atomic.Int64
	TotalProcessTime  atomic.Int64 // ナノ秒
}

// WorkerStatsSnapshot ある時点のワーカー統計の値コピーです
type WorkerStatsSnapshot struct {
	WorkerID           int
	ProcessedRequests  int64
	SuccessRequests    int64
	ErrorRequests      int64
	TotalProcessTime   time.Duration
	AverageProcessTime time.Duration
}

// Snapshot 現在の値をプレーンな構造体へコピーします
func (s *WorkerStats) Snapshot() WorkerStatsSnapshot {
	processed := s.ProcessedRequests.Load()
	total := time.Duration(s.TotalProcessTime.Load())

	var avg time.Duration
	if processed > 0 {
		avg = total / time.Duration(processed)
	}

	return WorkerStatsSnapshot{
		WorkerID:           s.WorkerID,
		ProcessedRequests:  processed,
		SuccessRequests:    s.SuccessRequests.Load(),
		ErrorRequests:      s.ErrorRequests.Load(),
		TotalProcessTime:   total,
		AverageProcessTime: avg,
	}
}

// MiddlewareManager ミドルウェア管理です
type MiddlewareManager struct {
	middlewares []MiddlewareFunc
	mu          sync.RWMutex
}

// MiddlewareFunc ミドルウェア関数です
type MiddlewareFunc func(next HandlerFunc) HandlerFunc

// ServerConfig サーバー設定です
type ServerConfig struct {
	Address        string
	Port           int
	MaxWorkers     int
	RequestBuffer  int
	ReadTimeout    time.Duration
	WriteTimeout   time.Duration
	IdleTimeout    time.Duration
	MaxHeaderBytes int
	// MaxConcurrentStreams HTTP/2の1接続あたりの同時ストリーム数の上限（0ならGoの既定値）
	MaxConcurrentStreams int
	EnableMetrics        bool
	MetricsInterval      time.Duration
}

// NewServerConfig デフォルトサーバー設定を作成します
func NewServerConfig() *ServerConfig {
	return &ServerConfig{
		Address:              "localhost",
		Port:                 8080,
		MaxWorkers:           runtime.GOMAXPROCS(0) * 2,
		RequestBuffer:        1000,
		ReadTimeout:          30 * time.Second,
		WriteTimeout:         30 * time.Second,
		IdleTimeout:          60 * time.Second,
		MaxHeaderBytes:       1 << 20, // 1MB
		MaxConcurrentStreams: 250,
		EnableMetrics:        true,
		MetricsInterval:      10 * time.Second,
	}
}

// NewConcurrentHTTPServer 新しい並行HTTPサーバーを作成します
func NewConcurrentHTTPServer(config *ServerConfig) *ConcurrentHTTPServer {
	ctx, cancel := context.WithCancel(context.Background())

	server := &ConcurrentHTTPServer{
		address:       config.Address,
		port:          config.Port,
		maxWorkers:    config.MaxWorkers,
		requestBuffer: config.RequestBuffer,
		requestQueue:  make(chan *HTTPRequestContext, config.RequestBuffer),
		responseQueue: make(chan *HTTPResponse, config.RequestBuffer),
		workerPool:    make([]*HTTPWorker, config.MaxWorkers),
		ctx:           ctx,
		cancel:        cancel,
		stats: &ServerStats{
			statusCodeCounts: make(map[int]int64),
			pathCounts:       make(map[string]int64),
			startTime:        time.Now(),
		},
		middleware: &MiddlewareManager{
			middlewares: make([]MiddlewareFunc, 0),
		},
	}

	// HTTP/1.1と平文のHTTP/2（h2c）を同じポートで受ける（Go 1.24以降）
	var protocols http.Protocols
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)

	// HTTPサーバーを設定
	server.server = &http.Server{
		Addr:           fmt.Sprintf("%s:%d", config.Address, config.Port),
		Handler:        server,
		ReadTimeout:    config.ReadTimeout,
		WriteTimeout:   config.WriteTimeout,
		IdleTimeout:    config.IdleTimeout,
		MaxHeaderBytes: config.MaxHeaderBytes,
		Protocols:      &protocols,
		HTTP2:          &http.HTTP2Config{MaxConcurrentStreams: config.MaxConcurrentStreams},
	}

	// ワーカーを初期化
	for i := range config.MaxWorkers {
		worker := &HTTPWorker{
			ID:     i,
			server: server,
			stats: &WorkerStats{
				WorkerID: i,
			},
			handlers: make(map[string]HandlerFunc),
		}
		server.workerPool[i] = worker
	}

	// デフォルトハンドラーを登録
	server.registerDefaultHandlers()

	return server
}

// registerDefaultHandlers デフォルトハンドラーを登録します
func (s *ConcurrentHTTPServer) registerDefaultHandlers() {
	s.RegisterHandler(http.MethodGet, "/health", s.healthCheckHandler)
	s.RegisterHandler(http.MethodGet, "/metrics", s.metricsHandler)
	s.RegisterHandler(http.MethodGet, "/status", s.statusHandler)
	s.RegisterHandler(http.MethodPost, "/api/echo", s.echoHandler)
	s.RegisterHandler(http.MethodGet, "/api/slow", s.slowHandler)
	s.RegisterHandler(http.MethodGet, "/api/cpu", s.cpuIntensiveHandler)
}

// ServeHTTP HTTPリクエストを処理します。レスポンスの書き込みはこのgoroutineだけが行います。
func (s *ConcurrentHTTPServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if s.ctx.Err() != nil || !s.isRunning.Load() {
		http.Error(w, "Server is not running", http.StatusServiceUnavailable)
		return
	}

	// リクエストコンテキストを作成
	requestCtx := &HTTPRequestContext{
		Request: &HTTPRequest{
			ID:        s.generateRequestID(),
			Method:    r.Method,
			Path:      r.URL.Path,
			Headers:   s.extractHeaders(r),
			ClientIP:  s.getClientIP(r),
			UserAgent: r.UserAgent(),
			Timestamp: time.Now(),
		},
		HTTPRequest: r,
		Context:     r.Context(),
		StartTime:   time.Now(),
		result:      make(chan *HTTPResponse, 1),
	}

	// リクエストボディを読み取り
	// Readを1回呼ぶだけでは全体が返らないことがあるので、ReadAllで読み切る
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 10<<20))
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}
	if len(body) > 0 {
		requestCtx.Request.Body = body
	}

	// 統計更新
	s.stats.totalRequests.Add(1)
	s.stats.activeRequests.Add(1)

	// リクエストをワーカーキューに送信
	select {
	case s.requestQueue <- requestCtx:
		// 正常にキューイング
	case <-s.ctx.Done():
		http.Error(w, "Server is shutting down", http.StatusServiceUnavailable)
		s.stats.activeRequests.Add(-1)
		return
	default:
		// キューが満杯
		http.Error(w, "Server is busy, please try again later", http.StatusTooManyRequests)
		s.stats.activeRequests.Add(-1)
		s.stats.errorRequests.Add(1)
		return
	}

	// ワーカーが処理した結果を待ってから書き込む
	select {
	case response := <-requestCtx.result:
		s.writeResponse(w, response)
	case <-s.ctx.Done():
		http.Error(w, "Server is shutting down", http.StatusServiceUnavailable)
	case <-r.Context().Done():
		// クライアント切断。ワーカー側のsendResponseは非ブロッキング送信なので残留しない。
	}
}

// writeResponse ワーカーが計算したレスポンスをResponseWriterへ書き込みます
func (s *ConcurrentHTTPServer) writeResponse(w http.ResponseWriter, response *HTTPResponse) {
	for key, value := range response.Headers {
		w.Header().Set(key, value)
	}

	w.WriteHeader(response.StatusCode)

	if response.Body != nil {
		if _, err := w.Write(response.Body); err != nil {
			log.Printf("Failed to write response body: %v", err)
		}
	}
}

// Start サーバーを開始します
func (s *ConcurrentHTTPServer) Start() error {
	if !s.isRunning.CompareAndSwap(false, true) {
		return errors.New("server is already running")
	}

	s.startTime = time.Now()
	s.stats.startTime = s.startTime

	log.Printf("Starting concurrent HTTP server on %s:%d with %d workers",
		s.address, s.port, s.maxWorkers)

	// ワーカーを開始
	for _, worker := range s.workerPool {
		s.wg.Go(worker.run)
	}

	// レスポンス処理を開始
	s.wg.Go(s.handleResponses)

	// メトリクス監視を開始
	s.wg.Go(s.monitorMetrics)

	// HTTPサーバーを開始
	s.wg.Go(func() {
		log.Printf("HTTP server listening on %s", s.server.Addr)
		if err := s.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("HTTP server error: %v", err)
		}
	})

	log.Printf("Concurrent HTTP server started successfully")
	return nil
}

// run ワーカーのメインループです
func (w *HTTPWorker) run() {
	log.Printf("HTTP worker %d started", w.ID)

	for {
		select {
		case <-w.server.ctx.Done():
			log.Printf("Worker %d stopping due to context cancellation", w.ID)
			return
		case requestCtx, ok := <-w.server.requestQueue:
			if !ok {
				log.Printf("Worker %d stopping due to request queue closure", w.ID)
				return
			}

			// リクエストを処理
			w.processRequest(requestCtx)
		}
	}
}

// processRequest リクエストを処理します
func (w *HTTPWorker) processRequest(requestCtx *HTTPRequestContext) {
	start := time.Now()

	// アクティブリクエスト数を減少
	defer func() {
		w.server.stats.activeRequests.Add(-1)
	}()

	// 適切なハンドラーを見つける
	handler := w.findHandler(requestCtx.Request.Method, requestCtx.Request.Path)
	if handler == nil {
		w.sendErrorResponse(requestCtx, http.StatusNotFound, "Not Found")
		return
	}

	// ミドルウェアを適用
	finalHandler := w.server.middleware.apply(handler)

	// サーバー停止時にハンドラーも中断できるようにする
	ctx, cancel := context.WithCancel(requestCtx.Context)
	stop := context.AfterFunc(w.server.ctx, cancel)
	defer stop()

	// ハンドラーを実行
	response, err := finalHandler(ctx, requestCtx.Request)
	if err != nil {
		w.sendErrorResponse(requestCtx, http.StatusInternalServerError, err.Error())
		return
	}

	// 統計更新。ServeHTTP側がresultを受け取る前に確定させる。
	processingTime := time.Since(start)
	w.stats.ProcessedRequests.Add(1)
	w.stats.TotalProcessTime.Add(int64(processingTime))

	if response != nil && response.Success {
		w.stats.SuccessRequests.Add(1)
	} else {
		w.stats.ErrorRequests.Add(1)
	}

	// レスポンスを送信
	w.sendResponse(requestCtx, response)
}

// findHandler 適切なハンドラーを見つけます
func (w *HTTPWorker) findHandler(method, path string) HandlerFunc {
	handlerKey := method + " " + path
	if handler, exists := w.handlers[handlerKey]; exists {
		return handler
	}

	// デフォルトハンドラーをチェック
	return w.server.getDefaultHandler(method, path)
}

// sendResponse レスポンスを結果チャネルへ届けます。ResponseWriterはここでは触りません。
func (w *HTTPWorker) sendResponse(requestCtx *HTTPRequestContext, response *HTTPResponse) {
	// 統計更新
	w.server.stats.completedRequests.Add(1)
	w.server.stats.mu.Lock()
	w.server.stats.statusCodeCounts[response.StatusCode]++
	w.server.stats.pathCounts[requestCtx.Request.Path]++
	w.server.stats.mu.Unlock()

	response.RequestID = requestCtx.Request.ID
	response.ProcessTime = time.Since(requestCtx.StartTime)
	response.CompletedAt = time.Now()

	// ServeHTTP側が待っていれば届ける。クライアント切断等で離脱済みなら破棄する。
	select {
	case requestCtx.result <- response:
	default:
	}

	// レスポンスを監視用キューへ送信
	select {
	case w.server.responseQueue <- response:
	case <-w.server.ctx.Done():
	default:
		log.Printf("Response queue is full, dropping response for request %s", requestCtx.Request.ID)
	}
}

// sendErrorResponse エラーレスポンスを送信します
func (w *HTTPWorker) sendErrorResponse(requestCtx *HTTPRequestContext, statusCode int, message string) {
	response := &HTTPResponse{
		StatusCode:  statusCode,
		Headers:     map[string]string{"Content-Type": "text/plain"},
		Body:        []byte(message),
		Success:     false,
		Error:       errors.New(message),
		HandlerName: "error_handler",
	}

	w.sendResponse(requestCtx, response)
}

// RegisterHandler ハンドラーを登録します。呼び出しはStart前だけにしてください
// （workerPool[].handlersはロックなしで共有されるためです）。
func (s *ConcurrentHTTPServer) RegisterHandler(method, path string, handler HandlerFunc) {
	handlerKey := method + " " + path

	for _, worker := range s.workerPool {
		worker.handlers[handlerKey] = handler
	}

	log.Printf("Handler registered: %s %s", method, path)
}

// getDefaultHandler デフォルトハンドラーを取得します
func (s *ConcurrentHTTPServer) getDefaultHandler(method, path string) HandlerFunc {
	// メソッドとパスに基づいてデフォルトハンドラーを返す
	return func(ctx context.Context, req *HTTPRequest) (*HTTPResponse, error) {
		var statusCode int
		var message string

		// メソッドに基づいてレスポンスを調整
		switch method {
		case http.MethodGet, http.MethodHead:
			statusCode = http.StatusNotFound
			message = fmt.Sprintf("Resource not found: %s %s", method, path)
		case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
			statusCode = http.StatusMethodNotAllowed
			message = fmt.Sprintf("Method %s not allowed for path: %s", method, path)
		default:
			statusCode = http.StatusNotImplemented
			message = fmt.Sprintf("Method %s not implemented for path: %s", method, path)
		}

		return &HTTPResponse{
			RequestID:   req.ID,
			StatusCode:  statusCode,
			Headers:     map[string]string{"Content-Type": "application/json"},
			Body:        []byte(fmt.Sprintf(`{"error": "%s", "path": "%s", "method": "%s"}`, message, path, method)),
			ProcessTime: time.Since(req.Timestamp),
			HandlerName: "default_handler",
			Success:     false,
			CompletedAt: time.Now(),
		}, nil
	}
}

// AddMiddleware ミドルウェアを追加します
func (s *ConcurrentHTTPServer) AddMiddleware(middleware MiddlewareFunc) {
	s.middleware.mu.Lock()
	defer s.middleware.mu.Unlock()

	s.middleware.middlewares = append(s.middleware.middlewares, middleware)
}

// apply ミドルウェアを適用します
func (mm *MiddlewareManager) apply(handler HandlerFunc) HandlerFunc {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	result := handler

	// ミドルウェアを逆順で適用
	for _, v := range slices.Backward(mm.middlewares) {
		result = v(result)
	}

	return result
}

// handleResponses レスポンスを処理します
func (s *ConcurrentHTTPServer) handleResponses() {
	log.Println("Response handler started")

	for {
		select {
		case <-s.ctx.Done():
			log.Println("Response handler stopping due to context cancellation")
			return
		case response, ok := <-s.responseQueue:
			if !ok {
				log.Println("Response handler stopping due to response queue closure")
				return
			}

			// レスポンス処理（ログ、メトリクス等）
			s.processResponse(response)
		}
	}
}

// processResponse レスポンスを処理します
func (s *ConcurrentHTTPServer) processResponse(response *HTTPResponse) {
	// 平均レスポンス時間を更新
	completed := s.stats.completedRequests.Load()
	if completed > 0 {
		s.stats.mu.Lock()
		s.stats.averageResponseTime = time.Duration(
			(int64(s.stats.averageResponseTime)*completed + int64(response.ProcessTime)) / (completed + 1))
		s.stats.mu.Unlock()
	}

	// エラーの場合はログ出力
	if response.Error != nil {
		log.Printf("Request %s failed: %v (ProcessTime=%v)",
			response.RequestID, response.Error, response.ProcessTime)
	}
}

// monitorMetrics メトリクスを監視します
func (s *ConcurrentHTTPServer) monitorMetrics() {
	// フィールドに保持せずStop/Resetも使わないため、Go 1.23以降はGCが回収できるtime.Tickでよい
	tick := time.Tick(15 * time.Second)

	log.Println("Metrics monitor started")

	for {
		select {
		case <-s.ctx.Done():
			log.Println("Metrics monitor stopping")
			return
		case <-tick:
			s.reportMetrics()
		}
	}
}

// reportMetrics メトリクスを報告します
func (s *ConcurrentHTTPServer) reportMetrics() {
	s.stats.mu.RLock()
	defer s.stats.mu.RUnlock()

	totalRequests := s.stats.totalRequests.Load()
	activeRequests := s.stats.activeRequests.Load()
	completedRequests := s.stats.completedRequests.Load()
	errorRequests := s.stats.errorRequests.Load()

	uptime := time.Since(s.stats.startTime)
	var rps float64
	if uptime.Seconds() > 0 {
		rps = float64(totalRequests) / uptime.Seconds()
	}

	var successRate float64
	if totalRequests > 0 {
		successRate = float64(completedRequests-errorRequests) / float64(totalRequests) * 100
	}

	log.Printf("Server Metrics: Total=%d, Active=%d, Completed=%d, Errors=%d, RPS=%.2f, Success=%.1f%%",
		totalRequests, activeRequests, completedRequests, errorRequests, rps, successRate)
	log.Printf("Performance: AvgResponseTime=%v, Uptime=%v", s.stats.averageResponseTime, uptime)

	// ワーカー別統計
	for _, worker := range s.workerPool {
		snap := worker.stats.Snapshot()

		if snap.ProcessedRequests > 0 {
			successRate := float64(snap.SuccessRequests) / float64(snap.ProcessedRequests) * 100
			log.Printf("Worker %d: Processed=%d, Success=%.1f%%, Errors=%d, AvgTime=%v",
				snap.WorkerID, snap.ProcessedRequests, successRate, snap.ErrorRequests, snap.AverageProcessTime)
		}
	}
}

// デフォルトハンドラー実装

// healthCheckHandler ヘルスチェックハンドラーです
func (s *ConcurrentHTTPServer) healthCheckHandler(ctx context.Context, req *HTTPRequest) (*HTTPResponse, error) {
	healthData := map[string]any{
		"status":    "healthy",
		"timestamp": time.Now().Unix(),
		"uptime":    time.Since(s.startTime).String(),
		"workers":   s.maxWorkers,
	}

	body, err := json.Marshal(healthData)
	if err != nil {
		return nil, err
	}

	return &HTTPResponse{
		StatusCode:  http.StatusOK,
		Headers:     map[string]string{"Content-Type": "application/json"},
		Body:        body,
		Success:     true,
		HandlerName: "health_check",
	}, nil
}

// metricsHandler メトリクスハンドラーです
func (s *ConcurrentHTTPServer) metricsHandler(ctx context.Context, req *HTTPRequest) (*HTTPResponse, error) {
	s.stats.mu.RLock()
	defer s.stats.mu.RUnlock()

	metrics := map[string]any{
		"total_requests":        s.stats.totalRequests.Load(),
		"active_requests":       s.stats.activeRequests.Load(),
		"completed_requests":    s.stats.completedRequests.Load(),
		"error_requests":        s.stats.errorRequests.Load(),
		"average_response_time": s.stats.averageResponseTime.String(),
		"uptime":                time.Since(s.stats.startTime).String(),
		"status_codes":          s.stats.statusCodeCounts,
		"path_counts":           s.stats.pathCounts,
	}

	body, err := json.Marshal(metrics)
	if err != nil {
		return nil, err
	}

	return &HTTPResponse{
		StatusCode:  http.StatusOK,
		Headers:     map[string]string{"Content-Type": "application/json"},
		Body:        body,
		Success:     true,
		HandlerName: "metrics",
	}, nil
}

// statusHandler ステータスハンドラーです
func (s *ConcurrentHTTPServer) statusHandler(ctx context.Context, req *HTTPRequest) (*HTTPResponse, error) {
	workerStats := make([]map[string]any, len(s.workerPool))

	for i, worker := range s.workerPool {
		snap := worker.stats.Snapshot()
		workerStats[i] = map[string]any{
			"worker_id":            snap.WorkerID,
			"processed_requests":   snap.ProcessedRequests,
			"success_requests":     snap.SuccessRequests,
			"error_requests":       snap.ErrorRequests,
			"average_process_time": snap.AverageProcessTime.String(),
		}
	}

	status := map[string]any{
		"server_status": "running",
		"workers":       workerStats,
		"queue_status": map[string]any{
			"request_queue_length":  len(s.requestQueue),
			"response_queue_length": len(s.responseQueue),
		},
	}

	body, err := json.Marshal(status)
	if err != nil {
		return nil, err
	}

	return &HTTPResponse{
		StatusCode:  http.StatusOK,
		Headers:     map[string]string{"Content-Type": "application/json"},
		Body:        body,
		Success:     true,
		HandlerName: "status",
	}, nil
}

// echoHandler エコーハンドラーです
func (s *ConcurrentHTTPServer) echoHandler(ctx context.Context, req *HTTPRequest) (*HTTPResponse, error) {
	echoData := map[string]any{
		"method":     req.Method,
		"path":       req.Path,
		"headers":    req.Headers,
		"body":       string(req.Body),
		"client_ip":  req.ClientIP,
		"user_agent": req.UserAgent,
		"timestamp":  req.Timestamp.Unix(),
	}

	body, err := json.Marshal(echoData)
	if err != nil {
		return nil, err
	}

	return &HTTPResponse{
		StatusCode:  http.StatusOK,
		Headers:     map[string]string{"Content-Type": "application/json"},
		Body:        body,
		Success:     true,
		HandlerName: "echo",
	}, nil
}

// slowHandler 遅延ハンドラーです
func (s *ConcurrentHTTPServer) slowHandler(ctx context.Context, req *HTTPRequest) (*HTTPResponse, error) {
	// 遅延をシミュレート（1〜3秒）
	delay := time.Second + rand.N(2*time.Second)

	select {
	case <-time.After(delay):
		// 正常処理
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	result := map[string]any{
		"message":   "Slow operation completed",
		"delay":     delay.String(),
		"timestamp": time.Now().Unix(),
	}

	body, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}

	return &HTTPResponse{
		StatusCode:  http.StatusOK,
		Headers:     map[string]string{"Content-Type": "application/json"},
		Body:        body,
		Success:     true,
		HandlerName: "slow",
	}, nil
}

// cpuIntensiveHandler CPU集約的ハンドラーです
func (s *ConcurrentHTTPServer) cpuIntensiveHandler(ctx context.Context, req *HTTPRequest) (*HTTPResponse, error) {
	// CPU集約的処理をシミュレート
	iterations := rand.IntN(1000000) + 500000
	result := 0

	for i := range iterations {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			result += i * i
		}

		// 定期的にコンテキストをチェック
		if i%10000 == 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
			}
		}
	}

	responseData := map[string]any{
		"message":    "CPU intensive operation completed",
		"iterations": iterations,
		"result":     result,
		"timestamp":  time.Now().Unix(),
	}

	body, err := json.Marshal(responseData)
	if err != nil {
		return nil, err
	}

	return &HTTPResponse{
		StatusCode:  http.StatusOK,
		Headers:     map[string]string{"Content-Type": "application/json"},
		Body:        body,
		Success:     true,
		HandlerName: "cpu_intensive",
	}, nil
}

// ユーティリティメソッド

// generateRequestID リクエストIDを生成します
func (s *ConcurrentHTTPServer) generateRequestID() string {
	return fmt.Sprintf("req_%d_%d", time.Now().UnixNano(), rand.Int64())
}

// extractHeaders ヘッダーを抽出します
func (s *ConcurrentHTTPServer) extractHeaders(r *http.Request) map[string]string {
	headers := make(map[string]string)
	for key, values := range r.Header {
		if len(values) > 0 {
			headers[key] = values[0]
		}
	}
	return headers
}

// getClientIP クライアントIPを取得します
func (s *ConcurrentHTTPServer) getClientIP(r *http.Request) string {
	// X-Forwarded-Forヘッダーをチェック
	if forwarded := r.Header.Get("X-Forwarded-For"); forwarded != "" {
		return forwarded
	}

	// X-Real-IPヘッダーをチェック
	if realIP := r.Header.Get("X-Real-IP"); realIP != "" {
		return realIP
	}

	// RemoteAddrを使用
	return r.RemoteAddr
}

// Shutdown サーバーを停止します
func (s *ConcurrentHTTPServer) Shutdown(timeout time.Duration) error {
	if !s.isRunning.CompareAndSwap(true, false) {
		return errors.New("server is not running")
	}

	log.Println("Shutting down concurrent HTTP server...")

	// 1. HTTPサーバーを停止
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if err := s.server.Shutdown(ctx); err != nil {
		log.Printf("HTTP server shutdown error: %v", err)
	}

	// 2. ワーカー・監視ループを止める。requestQueue closeしない。
	// ServeHTTPからの投入とcloseが競合すると送信側がpanicするため、
	// 停止はctx.Done()だけに頼り、投入側はServeHTTP冒頭のs.ctx.Err()チェックで弾く。
	s.cancel()

	// 3. ワーカーの終了を待機
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("All workers stopped gracefully")
	case <-time.After(timeout):
		log.Println("Timeout reached waiting for workers")
		select {
		case <-done:
			log.Println("Workers stopped after extra wait")
		case <-time.After(2 * time.Second):
			// ワーカーがまだresponseQueueへ送信中かもしれないのでcloseしない
			return errors.New("shutdown timed out waiting for workers to stop")
		}
	}

	// 4. すべてのワーカーが終わっているのでレスポンスキューを閉じる
	close(s.responseQueue)

	log.Println("Concurrent HTTP server shutdown completed")
	return nil
}

// GetStats 統計情報を取得します
func (s *ConcurrentHTTPServer) GetStats() map[string]any {
	s.stats.mu.RLock()
	defer s.stats.mu.RUnlock()

	totalRequests := s.stats.totalRequests.Load()
	activeRequests := s.stats.activeRequests.Load()
	completedRequests := s.stats.completedRequests.Load()
	errorRequests := s.stats.errorRequests.Load()

	uptime := time.Since(s.stats.startTime)
	var rps float64
	if uptime.Seconds() > 0 {
		rps = float64(totalRequests) / uptime.Seconds()
	}

	workerStats := make(map[string]any)
	for i, worker := range s.workerPool {
		snap := worker.stats.Snapshot()
		workerStats[fmt.Sprintf("worker_%d", i)] = map[string]any{
			"processed_requests":   snap.ProcessedRequests,
			"success_requests":     snap.SuccessRequests,
			"error_requests":       snap.ErrorRequests,
			"average_process_time": snap.AverageProcessTime,
		}
	}

	return map[string]any{
		"total_requests":        totalRequests,
		"active_requests":       activeRequests,
		"completed_requests":    completedRequests,
		"error_requests":        errorRequests,
		"average_response_time": s.stats.averageResponseTime,
		"requests_per_second":   rps,
		"uptime":                uptime,
		"worker_count":          len(s.workerPool),
		"worker_stats":          workerStats,
		"status_code_counts":    s.stats.statusCodeCounts,
		"path_counts":           s.stats.pathCounts,
	}
}

func main() {
	// サーバー設定
	config := NewServerConfig()
	config.Port = 8080
	config.MaxWorkers = 8
	config.RequestBuffer = 2000

	// サーバー作成
	server := NewConcurrentHTTPServer(config)

	// ミドルウェアを追加
	server.AddMiddleware(LoggingMiddleware)
	server.AddMiddleware(CORSMiddleware)

	// カスタムハンドラーを登録
	server.RegisterHandler(http.MethodGet, "/api/test", TestHandler)
	server.RegisterHandler(http.MethodPost, "/api/data", DataHandler)

	// サーバー開始
	if err := server.Start(); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}

	// 30秒間実行
	time.Sleep(30 * time.Second)

	// 最終統計表示
	stats := server.GetStats()
	log.Printf("Final Stats: %+v", stats)

	// グレースフルシャットダウン
	if err := server.Shutdown(10 * time.Second); err != nil {
		log.Printf("Shutdown error: %v", err)
	}
}

// ミドルウェア実装

// LoggingMiddleware ログミドルウェアです
func LoggingMiddleware(next HandlerFunc) HandlerFunc {
	return func(ctx context.Context, req *HTTPRequest) (*HTTPResponse, error) {
		start := time.Now()

		response, err := next(ctx, req)

		duration := time.Since(start)
		status := "SUCCESS"
		if err != nil {
			status = "ERROR"
		}

		log.Printf("[%s] %s %s - %s (%v)", status, req.Method, req.Path, req.ClientIP, duration)

		return response, err
	}
}

// CORSMiddleware CORSミドルウェアです
func CORSMiddleware(next HandlerFunc) HandlerFunc {
	return func(ctx context.Context, req *HTTPRequest) (*HTTPResponse, error) {
		response, err := next(ctx, req)

		if response != nil {
			if response.Headers == nil {
				response.Headers = make(map[string]string)
			}
			response.Headers["Access-Control-Allow-Origin"] = "*"
			response.Headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, OPTIONS"
			response.Headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
		}

		return response, err
	}
}

// カスタムハンドラー実装

// TestHandler テストハンドラーです
func TestHandler(ctx context.Context, req *HTTPRequest) (*HTTPResponse, error) {
	data := map[string]any{
		"message":    "Test handler response",
		"request_id": req.ID,
		"timestamp":  time.Now().Unix(),
	}

	body, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}

	return &HTTPResponse{
		StatusCode:  http.StatusOK,
		Headers:     map[string]string{"Content-Type": "application/json"},
		Body:        body,
		Success:     true,
		HandlerName: "test",
	}, nil
}

// DataHandler データハンドラーです
func DataHandler(ctx context.Context, req *HTTPRequest) (*HTTPResponse, error) {
	// リクエストボディを解析
	var requestData map[string]any
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &requestData); err != nil {
			return &HTTPResponse{
				StatusCode:  http.StatusBadRequest,
				Headers:     map[string]string{"Content-Type": "application/json"},
				Body:        []byte(`{"error": "Invalid JSON"}`),
				Success:     false,
				HandlerName: "data",
			}, nil
		}
	}

	// レスポンスデータを作成
	responseData := map[string]any{
		"message":       "Data processed successfully",
		"received_data": requestData,
		"processed_at":  time.Now().Unix(),
		"request_id":    req.ID,
	}

	body, err := json.Marshal(responseData)
	if err != nil {
		return nil, err
	}

	return &HTTPResponse{
		StatusCode:  http.StatusOK,
		Headers:     map[string]string{"Content-Type": "application/json"},
		Body:        body,
		Success:     true,
		HandlerName: "data",
	}, nil
}
