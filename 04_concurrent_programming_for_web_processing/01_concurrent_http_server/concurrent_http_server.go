package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// HTTPRequest は処理対象のHTTPリクエストです
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

// HTTPResponse は処理済みHTTPレスポンスです
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

// ConcurrentHTTPServer は並行HTTPサーバーです
type ConcurrentHTTPServer struct {
	// サーバー設定
	address       string
	port          int
	maxWorkers    int
	requestBuffer int

	// HTTP処理
	server *http.Server
	mux    *http.ServeMux

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
	isRunning int32
	startTime time.Time
}

// HTTPRequestContext はリクエストコンテキストです
type HTTPRequestContext struct {
	Request        *HTTPRequest
	ResponseWriter http.ResponseWriter
	HTTPRequest    *http.Request
	Context        context.Context
	StartTime      time.Time
}

// HTTPWorker は並行HTTPワーカーです
type HTTPWorker struct {
	ID       int
	server   *ConcurrentHTTPServer
	stats    *WorkerStats
	handlers map[string]HandlerFunc
}

// HandlerFunc はハンドラー関数です
type HandlerFunc func(ctx context.Context, req *HTTPRequest) (*HTTPResponse, error)

// ServerStats はサーバー統計です
type ServerStats struct {
	mu                  sync.RWMutex
	totalRequests       int64
	activeRequests      int64
	completedRequests   int64
	errorRequests       int64
	averageResponseTime time.Duration
	statusCodeCounts    map[int]int64
	pathCounts          map[string]int64
	startTime           time.Time
}

// WorkerStats はワーカー統計です
type WorkerStats struct {
	WorkerID           int
	ProcessedRequests  int64
	SuccessRequests    int64
	ErrorRequests      int64
	TotalProcessTime   time.Duration
	AverageProcessTime time.Duration
}

// MiddlewareManager はミドルウェア管理です
type MiddlewareManager struct {
	middlewares []MiddlewareFunc
	mu          sync.RWMutex
}

// MiddlewareFunc はミドルウェア関数です
type MiddlewareFunc func(next HandlerFunc) HandlerFunc

// ServerConfig はサーバー設定です
type ServerConfig struct {
	Address         string
	Port            int
	MaxWorkers      int
	RequestBuffer   int
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	IdleTimeout     time.Duration
	MaxHeaderBytes  int
	EnableMetrics   bool
	MetricsInterval time.Duration
}

// NewServerConfig はデフォルトサーバー設定を作成します
func NewServerConfig() *ServerConfig {
	return &ServerConfig{
		Address:         "localhost",
		Port:            8080,
		MaxWorkers:      runtime.NumCPU() * 2,
		RequestBuffer:   1000,
		ReadTimeout:     30 * time.Second,
		WriteTimeout:    30 * time.Second,
		IdleTimeout:     60 * time.Second,
		MaxHeaderBytes:  1 << 20, // 1MB
		EnableMetrics:   true,
		MetricsInterval: 10 * time.Second,
	}
}

// NewConcurrentHTTPServer は新しい並行HTTPサーバーを作成します
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

	// HTTPサーバーを設定
	server.mux = http.NewServeMux()
	server.server = &http.Server{
		Addr:           fmt.Sprintf("%s:%d", config.Address, config.Port),
		Handler:        server,
		ReadTimeout:    config.ReadTimeout,
		WriteTimeout:   config.WriteTimeout,
		IdleTimeout:    config.IdleTimeout,
		MaxHeaderBytes: config.MaxHeaderBytes,
	}

	// ワーカーを初期化
	for i := 0; i < config.MaxWorkers; i++ {
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

// registerDefaultHandlers はデフォルトハンドラーを登録します
func (s *ConcurrentHTTPServer) registerDefaultHandlers() {
	s.RegisterHandler("GET", "/health", s.healthCheckHandler)
	s.RegisterHandler("GET", "/metrics", s.metricsHandler)
	s.RegisterHandler("GET", "/status", s.statusHandler)
	s.RegisterHandler("POST", "/api/echo", s.echoHandler)
	s.RegisterHandler("GET", "/api/slow", s.slowHandler)
	s.RegisterHandler("GET", "/api/cpu", s.cpuIntensiveHandler)
}

// ServeHTTP はHTTPリクエストを処理します
func (s *ConcurrentHTTPServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if atomic.LoadInt32(&s.isRunning) == 0 {
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
		ResponseWriter: w,
		HTTPRequest:    r,
		Context:        r.Context(),
		StartTime:      time.Now(),
	}

	// リクエストボディを読み取り
	if r.ContentLength > 0 {
		body := make([]byte, r.ContentLength)
		_, err := r.Body.Read(body)
		if err != nil && err.Error() != "EOF" {
			http.Error(w, "Failed to read request body", http.StatusBadRequest)
			return
		}
		requestCtx.Request.Body = body
	}

	// 統計更新
	atomic.AddInt64(&s.stats.totalRequests, 1)
	atomic.AddInt64(&s.stats.activeRequests, 1)

	// リクエストをワーカーキューに送信
	select {
	case s.requestQueue <- requestCtx:
		// 正常にキューイング
	case <-s.ctx.Done():
		http.Error(w, "Server is shutting down", http.StatusServiceUnavailable)
		atomic.AddInt64(&s.stats.activeRequests, -1)
		return
	default:
		// キューが満杯
		http.Error(w, "Server is busy, please try again later", http.StatusTooManyRequests)
		atomic.AddInt64(&s.stats.activeRequests, -1)
		atomic.AddInt64(&s.stats.errorRequests, 1)
		return
	}
}

// Start はサーバーを開始します
func (s *ConcurrentHTTPServer) Start() error {
	if !atomic.CompareAndSwapInt32(&s.isRunning, 0, 1) {
		return fmt.Errorf("server is already running")
	}

	s.startTime = time.Now()
	s.stats.startTime = s.startTime

	log.Printf("Starting concurrent HTTP server on %s:%d with %d workers",
		s.address, s.port, s.maxWorkers)

	// ワーカーを開始
	for _, worker := range s.workerPool {
		s.wg.Add(1)
		go worker.run()
	}

	// レスポンス処理を開始
	s.wg.Add(1)
	go s.handleResponses()

	// メトリクス監視を開始
	s.wg.Add(1)
	go s.monitorMetrics()

	// HTTPサーバーを開始
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		log.Printf("HTTP server listening on %s", s.server.Addr)
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("HTTP server error: %v", err)
		}
	}()

	log.Printf("Concurrent HTTP server started successfully")
	return nil
}

// run はワーカーのメインループです
func (w *HTTPWorker) run() {
	defer w.server.wg.Done()

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

// processRequest はリクエストを処理します
func (w *HTTPWorker) processRequest(requestCtx *HTTPRequestContext) {
	start := time.Now()

	// アクティブリクエスト数を減少
	defer func() {
		atomic.AddInt64(&w.server.stats.activeRequests, -1)
	}()

	// 適切なハンドラーを見つける
	handler := w.findHandler(requestCtx.Request.Method, requestCtx.Request.Path)
	if handler == nil {
		w.sendErrorResponse(requestCtx, http.StatusNotFound, "Not Found")
		return
	}

	// ミドルウェアを適用
	finalHandler := w.server.middleware.apply(handler)

	// ハンドラーを実行
	response, err := finalHandler(requestCtx.Context, requestCtx.Request)
	if err != nil {
		w.sendErrorResponse(requestCtx, http.StatusInternalServerError, err.Error())
		return
	}

	// レスポンスを送信
	w.sendResponse(requestCtx, response)

	// 統計更新
	processingTime := time.Since(start)
	atomic.AddInt64(&w.stats.ProcessedRequests, 1)
	w.stats.TotalProcessTime += processingTime

	if w.stats.ProcessedRequests > 0 {
		w.stats.AverageProcessTime = w.stats.TotalProcessTime / time.Duration(w.stats.ProcessedRequests)
	}

	if response != nil && response.Success {
		atomic.AddInt64(&w.stats.SuccessRequests, 1)
	} else {
		atomic.AddInt64(&w.stats.ErrorRequests, 1)
	}
}

// findHandler は適切なハンドラーを見つけます
func (w *HTTPWorker) findHandler(method, path string) HandlerFunc {
	handlerKey := method + " " + path
	if handler, exists := w.handlers[handlerKey]; exists {
		return handler
	}

	// デフォルトハンドラーをチェック
	return w.server.getDefaultHandler(method, path)
}

// sendResponse はレスポンスを送信します
func (w *HTTPWorker) sendResponse(requestCtx *HTTPRequestContext, response *HTTPResponse) {
	responseWriter := requestCtx.ResponseWriter

	// ヘッダーを設定
	for key, value := range response.Headers {
		responseWriter.Header().Set(key, value)
	}

	// ステータスコードを設定
	responseWriter.WriteHeader(response.StatusCode)

	// ボディを書き込み
	if response.Body != nil {
		if _, err := responseWriter.Write(response.Body); err != nil {
			log.Printf("Failed to write response body: %v", err)
		}
	}

	// 統計更新
	atomic.AddInt64(&w.server.stats.completedRequests, 1)
	w.server.stats.mu.Lock()
	w.server.stats.statusCodeCounts[response.StatusCode]++
	w.server.stats.pathCounts[requestCtx.Request.Path]++
	w.server.stats.mu.Unlock()

	// レスポンスを結果キューに送信
	response.RequestID = requestCtx.Request.ID
	response.ProcessTime = time.Since(requestCtx.StartTime)
	response.CompletedAt = time.Now()

	select {
	case w.server.responseQueue <- response:
	case <-w.server.ctx.Done():
	default:
		log.Printf("Response queue is full, dropping response for request %s", requestCtx.Request.ID)
	}
}

// sendErrorResponse はエラーレスポンスを送信します
func (w *HTTPWorker) sendErrorResponse(requestCtx *HTTPRequestContext, statusCode int, message string) {
	response := &HTTPResponse{
		StatusCode:  statusCode,
		Headers:     map[string]string{"Content-Type": "text/plain"},
		Body:        []byte(message),
		Success:     false,
		Error:       fmt.Errorf("%s", message),
		HandlerName: "error_handler",
	}

	w.sendResponse(requestCtx, response)
}

// RegisterHandler はハンドラーを登録します
func (s *ConcurrentHTTPServer) RegisterHandler(method, path string, handler HandlerFunc) {
	handlerKey := method + " " + path

	for _, worker := range s.workerPool {
		worker.handlers[handlerKey] = handler
	}

	log.Printf("Handler registered: %s %s", method, path)
}

// getDefaultHandler はデフォルトハンドラーを取得します
func (s *ConcurrentHTTPServer) getDefaultHandler(method, path string) HandlerFunc {
	// メソッドとパスに基づいてデフォルトハンドラーを返す
	return func(ctx context.Context, req *HTTPRequest) (*HTTPResponse, error) {
		var statusCode int
		var message string

		// メソッドに基づいてレスポンスを調整
		switch method {
		case "GET", "HEAD":
			statusCode = http.StatusNotFound
			message = fmt.Sprintf("Resource not found: %s %s", method, path)
		case "POST", "PUT", "PATCH", "DELETE":
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

// AddMiddleware はミドルウェアを追加します
func (s *ConcurrentHTTPServer) AddMiddleware(middleware MiddlewareFunc) {
	s.middleware.mu.Lock()
	defer s.middleware.mu.Unlock()

	s.middleware.middlewares = append(s.middleware.middlewares, middleware)
}

// apply はミドルウェアを適用します
func (mm *MiddlewareManager) apply(handler HandlerFunc) HandlerFunc {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	result := handler

	// ミドルウェアを逆順で適用
	for i := len(mm.middlewares) - 1; i >= 0; i-- {
		result = mm.middlewares[i](result)
	}

	return result
}

// handleResponses はレスポンスを処理します
func (s *ConcurrentHTTPServer) handleResponses() {
	defer s.wg.Done()

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

// processResponse はレスポンスを処理します
func (s *ConcurrentHTTPServer) processResponse(response *HTTPResponse) {
	// 平均レスポンス時間を更新
	completed := atomic.LoadInt64(&s.stats.completedRequests)
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

// monitorMetrics はメトリクスを監視します
func (s *ConcurrentHTTPServer) monitorMetrics() {
	defer s.wg.Done()

	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	log.Println("Metrics monitor started")

	for {
		select {
		case <-s.ctx.Done():
			log.Println("Metrics monitor stopping")
			return
		case <-ticker.C:
			s.reportMetrics()
		}
	}
}

// reportMetrics はメトリクスを報告します
func (s *ConcurrentHTTPServer) reportMetrics() {
	s.stats.mu.RLock()
	defer s.stats.mu.RUnlock()

	totalRequests := atomic.LoadInt64(&s.stats.totalRequests)
	activeRequests := atomic.LoadInt64(&s.stats.activeRequests)
	completedRequests := atomic.LoadInt64(&s.stats.completedRequests)
	errorRequests := atomic.LoadInt64(&s.stats.errorRequests)

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
		processed := atomic.LoadInt64(&worker.stats.ProcessedRequests)
		success := atomic.LoadInt64(&worker.stats.SuccessRequests)
		errors := atomic.LoadInt64(&worker.stats.ErrorRequests)

		if processed > 0 {
			successRate := float64(success) / float64(processed) * 100
			log.Printf("Worker %d: Processed=%d, Success=%.1f%%, Errors=%d, AvgTime=%v",
				worker.ID, processed, successRate, errors, worker.stats.AverageProcessTime)
		}
	}
}

// デフォルトハンドラー実装

// healthCheckHandler はヘルスチェックハンドラーです
func (s *ConcurrentHTTPServer) healthCheckHandler(ctx context.Context, req *HTTPRequest) (*HTTPResponse, error) {
	healthData := map[string]interface{}{
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

// metricsHandler はメトリクスハンドラーです
func (s *ConcurrentHTTPServer) metricsHandler(ctx context.Context, req *HTTPRequest) (*HTTPResponse, error) {
	s.stats.mu.RLock()
	defer s.stats.mu.RUnlock()

	metrics := map[string]interface{}{
		"total_requests":        atomic.LoadInt64(&s.stats.totalRequests),
		"active_requests":       atomic.LoadInt64(&s.stats.activeRequests),
		"completed_requests":    atomic.LoadInt64(&s.stats.completedRequests),
		"error_requests":        atomic.LoadInt64(&s.stats.errorRequests),
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

// statusHandler はステータスハンドラーです
func (s *ConcurrentHTTPServer) statusHandler(ctx context.Context, req *HTTPRequest) (*HTTPResponse, error) {
	workerStats := make([]map[string]interface{}, len(s.workerPool))

	for i, worker := range s.workerPool {
		workerStats[i] = map[string]interface{}{
			"worker_id":            worker.ID,
			"processed_requests":   atomic.LoadInt64(&worker.stats.ProcessedRequests),
			"success_requests":     atomic.LoadInt64(&worker.stats.SuccessRequests),
			"error_requests":       atomic.LoadInt64(&worker.stats.ErrorRequests),
			"average_process_time": worker.stats.AverageProcessTime.String(),
		}
	}

	status := map[string]interface{}{
		"server_status": "running",
		"workers":       workerStats,
		"queue_status": map[string]interface{}{
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

// echoHandler はエコーハンドラーです
func (s *ConcurrentHTTPServer) echoHandler(ctx context.Context, req *HTTPRequest) (*HTTPResponse, error) {
	echoData := map[string]interface{}{
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

// slowHandler は遅延ハンドラーです
func (s *ConcurrentHTTPServer) slowHandler(ctx context.Context, req *HTTPRequest) (*HTTPResponse, error) {
	// 遅延をシミュレート
	delay := time.Duration(rand.Intn(2000)+1000) * time.Millisecond

	select {
	case <-time.After(delay):
		// 正常処理
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	result := map[string]interface{}{
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

// cpuIntensiveHandler はCPU集約的ハンドラーです
func (s *ConcurrentHTTPServer) cpuIntensiveHandler(ctx context.Context, req *HTTPRequest) (*HTTPResponse, error) {
	// CPU集約的処理をシミュレート
	iterations := rand.Intn(1000000) + 500000
	result := 0

	for i := 0; i < iterations; i++ {
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

	responseData := map[string]interface{}{
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

// generateRequestID はリクエストIDを生成します
func (s *ConcurrentHTTPServer) generateRequestID() string {
	return fmt.Sprintf("req_%d_%d", time.Now().UnixNano(), rand.Int63())
}

// extractHeaders はヘッダーを抽出します
func (s *ConcurrentHTTPServer) extractHeaders(r *http.Request) map[string]string {
	headers := make(map[string]string)
	for key, values := range r.Header {
		if len(values) > 0 {
			headers[key] = values[0]
		}
	}
	return headers
}

// getClientIP はクライアントIPを取得します
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

// Shutdown はサーバーを停止します
func (s *ConcurrentHTTPServer) Shutdown(timeout time.Duration) error {
	if !atomic.CompareAndSwapInt32(&s.isRunning, 1, 0) {
		return fmt.Errorf("server is not running")
	}

	log.Println("Shutting down concurrent HTTP server...")

	// 1. HTTPサーバーを停止
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if err := s.server.Shutdown(ctx); err != nil {
		log.Printf("HTTP server shutdown error: %v", err)
	}

	// 2. リクエストキューを閉じる
	close(s.requestQueue)

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
		log.Println("Timeout reached, forcing shutdown...")
		s.cancel()

		// 追加の待機時間
		select {
		case <-done:
			log.Println("Workers stopped after cancellation")
		case <-time.After(2 * time.Second):
			log.Println("Some workers may not have stopped properly")
		}
	}

	// 4. レスポンスキューを閉じる
	close(s.responseQueue)

	log.Println("Concurrent HTTP server shutdown completed")
	return nil
}

// GetStats は統計情報を取得します
func (s *ConcurrentHTTPServer) GetStats() map[string]interface{} {
	s.stats.mu.RLock()
	defer s.stats.mu.RUnlock()

	totalRequests := atomic.LoadInt64(&s.stats.totalRequests)
	activeRequests := atomic.LoadInt64(&s.stats.activeRequests)
	completedRequests := atomic.LoadInt64(&s.stats.completedRequests)
	errorRequests := atomic.LoadInt64(&s.stats.errorRequests)

	uptime := time.Since(s.stats.startTime)
	var rps float64
	if uptime.Seconds() > 0 {
		rps = float64(totalRequests) / uptime.Seconds()
	}

	workerStats := make(map[string]interface{})
	for i, worker := range s.workerPool {
		workerStats[fmt.Sprintf("worker_%d", i)] = map[string]interface{}{
			"processed_requests":   atomic.LoadInt64(&worker.stats.ProcessedRequests),
			"success_requests":     atomic.LoadInt64(&worker.stats.SuccessRequests),
			"error_requests":       atomic.LoadInt64(&worker.stats.ErrorRequests),
			"average_process_time": worker.stats.AverageProcessTime,
		}
	}

	return map[string]interface{}{
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
	server.RegisterHandler("GET", "/api/test", TestHandler)
	server.RegisterHandler("POST", "/api/data", DataHandler)

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

// LoggingMiddleware はログミドルウェアです
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

// CORSMiddleware はCORSミドルウェアです
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

// TestHandler はテストハンドラーです
func TestHandler(ctx context.Context, req *HTTPRequest) (*HTTPResponse, error) {
	data := map[string]interface{}{
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

// DataHandler はデータハンドラーです
func DataHandler(ctx context.Context, req *HTTPRequest) (*HTTPResponse, error) {
	// リクエストボディを解析
	var requestData map[string]interface{}
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
	responseData := map[string]interface{}{
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
