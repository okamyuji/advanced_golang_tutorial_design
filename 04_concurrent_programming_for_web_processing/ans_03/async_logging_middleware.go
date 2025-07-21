package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// LogEntry 構造化ログエントリ
type LogEntry struct {
	Timestamp    time.Time `json:"timestamp"`
	Method       string    `json:"method"`
	URL          string    `json:"url"`
	RemoteAddr   string    `json:"remote_addr"`
	UserAgent    string    `json:"user_agent"`
	StatusCode   int       `json:"status_code"`
	ResponseTime int64     `json:"response_time_ms"`
	RequestID    string    `json:"request_id"`
	ContentType  string    `json:"content_type"`
	ResponseSize int64     `json:"response_size"`
}

// LogMetrics ログ処理のメトリクス
type LogMetrics struct {
	TotalRequests     int64 `json:"total_requests"`
	ProcessedLogs     int64 `json:"processed_logs"`
	DroppedLogs       int64 `json:"dropped_logs"`
	QueueSize         int64 `json:"queue_size"`
	BatchWrites       int64 `json:"batch_writes"`
	FailedWrites      int64 `json:"failed_writes"`
	AverageLatency    int64 `json:"average_latency_ms"`
	CurrentQueueDepth int64 `json:"current_queue_depth"`
}

// AsyncLoggingMiddleware 非同期ログ収集ミドルウェア
type AsyncLoggingMiddleware struct {
	logQueue      chan LogEntry
	batchSize     int
	flushInterval time.Duration
	maxQueueSize  int
	outputFile    string
	metrics       LogMetrics
	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup
	mu            sync.RWMutex
	retryAttempts int
	retryDelay    time.Duration
	backupQueue   []LogEntry
	backupMu      sync.Mutex
}

// NewAsyncLoggingMiddleware 新しい非同期ログミドルウェアを作成
func NewAsyncLoggingMiddleware(config LoggingConfig) *AsyncLoggingMiddleware {
	ctx, cancel := context.WithCancel(context.Background())

	middleware := &AsyncLoggingMiddleware{
		logQueue:      make(chan LogEntry, config.MaxQueueSize),
		batchSize:     config.BatchSize,
		flushInterval: config.FlushInterval,
		maxQueueSize:  config.MaxQueueSize,
		outputFile:    config.OutputFile,
		ctx:           ctx,
		cancel:        cancel,
		retryAttempts: config.RetryAttempts,
		retryDelay:    config.RetryDelay,
		backupQueue:   make([]LogEntry, 0, config.MaxQueueSize),
	}

	// ログ処理ワーカーを開始
	middleware.wg.Add(1)
	go middleware.logProcessor()

	// メトリクス更新ワーカーを開始
	middleware.wg.Add(1)
	go middleware.metricsUpdater()

	return middleware
}

// LoggingConfig ログ設定
type LoggingConfig struct {
	BatchSize     int
	FlushInterval time.Duration
	MaxQueueSize  int
	OutputFile    string
	RetryAttempts int
	RetryDelay    time.Duration
}

// Middleware HTTPミドルウェア関数
func (alm *AsyncLoggingMiddleware) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		// レスポンスライターをラップして詳細情報を取得
		wrapped := &responseWriter{
			ResponseWriter: w,
			statusCode:     200,
			responseSize:   0,
		}

		// リクエストIDを生成
		requestID := fmt.Sprintf("%d-%s", time.Now().UnixNano(), r.RemoteAddr)

		// 次のハンドラーを実行
		next.ServeHTTP(wrapped, r)

		// ログエントリを作成
		logEntry := LogEntry{
			Timestamp:    time.Now(),
			Method:       r.Method,
			URL:          r.URL.String(),
			RemoteAddr:   r.RemoteAddr,
			UserAgent:    r.UserAgent(),
			StatusCode:   wrapped.statusCode,
			ResponseTime: time.Since(start).Milliseconds(),
			RequestID:    requestID,
			ContentType:  wrapped.Header().Get("Content-Type"),
			ResponseSize: wrapped.responseSize,
		}

		// 非同期でログキューに送信
		select {
		case alm.logQueue <- logEntry:
			atomic.AddInt64(&alm.metrics.TotalRequests, 1)
		default:
			// キューが満杯の場合はドロップ
			atomic.AddInt64(&alm.metrics.DroppedLogs, 1)

			// バックアップキューに保存（ベストエフォート）
			alm.backupMu.Lock()
			if len(alm.backupQueue) < alm.maxQueueSize {
				alm.backupQueue = append(alm.backupQueue, logEntry)
			}
			alm.backupMu.Unlock()
		}
	})
}

// responseWriter HTTPレスポンスライターのラッパー
type responseWriter struct {
	http.ResponseWriter
	statusCode   int
	responseSize int64
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

func (rw *responseWriter) Write(b []byte) (int, error) {
	size, err := rw.ResponseWriter.Write(b)
	rw.responseSize += int64(size)
	return size, err
}

// logProcessor ログを並行処理してバッチ書き込み
func (alm *AsyncLoggingMiddleware) logProcessor() {
	defer alm.wg.Done()

	ticker := time.NewTicker(alm.flushInterval)
	defer ticker.Stop()

	batch := make([]LogEntry, 0, alm.batchSize)

	for {
		select {
		case <-alm.ctx.Done():
			// 残っているログを処理してから終了
			alm.processBatch(batch)
			alm.processBackupQueue()
			return

		case logEntry := <-alm.logQueue:
			batch = append(batch, logEntry)
			atomic.AddInt64(&alm.metrics.QueueSize, -1)

			// バッチサイズに達したら書き込み
			if len(batch) >= alm.batchSize {
				alm.processBatch(batch)
				batch = batch[:0] // スライスをリセット
			}

		case <-ticker.C:
			// 定期的なフラッシュ
			if len(batch) > 0 {
				alm.processBatch(batch)
				batch = batch[:0]
			}

			// バックアップキューも処理
			alm.processBackupQueue()
		}
	}
}

// processBatch バッチでログを書き込み
func (alm *AsyncLoggingMiddleware) processBatch(batch []LogEntry) {
	if len(batch) == 0 {
		return
	}

	// リトライ付きで書き込み処理
	for attempt := 0; attempt <= alm.retryAttempts; attempt++ {
		if err := alm.writeLogs(batch); err != nil {
			if attempt == alm.retryAttempts {
				// 最終的に失敗した場合
				atomic.AddInt64(&alm.metrics.FailedWrites, 1)
				log.Printf("ログ書き込み失敗（最終試行）: %v", err)

				// バックアップキューに保存
				alm.backupMu.Lock()
				for _, entry := range batch {
					if len(alm.backupQueue) < alm.maxQueueSize {
						alm.backupQueue = append(alm.backupQueue, entry)
					}
				}
				alm.backupMu.Unlock()
				return
			}

			// リトライ前に待機
			time.Sleep(alm.retryDelay)
			continue
		}

		// 成功した場合
		atomic.AddInt64(&alm.metrics.ProcessedLogs, int64(len(batch)))
		atomic.AddInt64(&alm.metrics.BatchWrites, 1)
		return
	}
}

// writeLogs ログをファイルに書き込み
func (alm *AsyncLoggingMiddleware) writeLogs(logs []LogEntry) error {
	file, err := os.OpenFile(alm.outputFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("ファイルオープンエラー: %w", err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			log.Printf("Failed to close log file: %v", err)
		}
	}()

	encoder := json.NewEncoder(file)
	for _, logEntry := range logs {
		if err := encoder.Encode(logEntry); err != nil {
			return fmt.Errorf("ログエンコードエラー: %w", err)
		}
	}

	return nil
}

// processBackupQueue バックアップキューを処理
func (alm *AsyncLoggingMiddleware) processBackupQueue() {
	alm.backupMu.Lock()
	defer alm.backupMu.Unlock()

	if len(alm.backupQueue) == 0 {
		return
	}

	// バックアップキューのログを再試行
	if err := alm.writeLogs(alm.backupQueue); err == nil {
		atomic.AddInt64(&alm.metrics.ProcessedLogs, int64(len(alm.backupQueue)))
		alm.backupQueue = alm.backupQueue[:0] // クリア
		log.Printf("バックアップキューからログ復旧完了: %d件", len(alm.backupQueue))
	}
}

// metricsUpdater メトリクスを定期更新
func (alm *AsyncLoggingMiddleware) metricsUpdater() {
	defer alm.wg.Done()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-alm.ctx.Done():
			return
		case <-ticker.C:
			alm.updateMetrics()
		}
	}
}

// updateMetrics メトリクスを更新
func (alm *AsyncLoggingMiddleware) updateMetrics() {
	alm.mu.Lock()
	defer alm.mu.Unlock()

	// 現在のキュー深度を更新
	atomic.StoreInt64(&alm.metrics.CurrentQueueDepth, int64(len(alm.logQueue)))

	// メトリクスを定期的にログ出力
	log.Printf("ログメトリクス - 処理済み: %d, ドロップ: %d, キュー深度: %d, バッチ書き込み: %d, 失敗: %d",
		atomic.LoadInt64(&alm.metrics.ProcessedLogs),
		atomic.LoadInt64(&alm.metrics.DroppedLogs),
		atomic.LoadInt64(&alm.metrics.CurrentQueueDepth),
		atomic.LoadInt64(&alm.metrics.BatchWrites),
		atomic.LoadInt64(&alm.metrics.FailedWrites),
	)
}

// GetMetrics 現在のメトリクスを取得
func (alm *AsyncLoggingMiddleware) GetMetrics() LogMetrics {
	alm.mu.RLock()
	defer alm.mu.RUnlock()

	return LogMetrics{
		TotalRequests:     atomic.LoadInt64(&alm.metrics.TotalRequests),
		ProcessedLogs:     atomic.LoadInt64(&alm.metrics.ProcessedLogs),
		DroppedLogs:       atomic.LoadInt64(&alm.metrics.DroppedLogs),
		QueueSize:         atomic.LoadInt64(&alm.metrics.QueueSize),
		BatchWrites:       atomic.LoadInt64(&alm.metrics.BatchWrites),
		FailedWrites:      atomic.LoadInt64(&alm.metrics.FailedWrites),
		CurrentQueueDepth: atomic.LoadInt64(&alm.metrics.CurrentQueueDepth),
	}
}

// MetricsHandler メトリクス取得用のHTTPハンドラー
func (alm *AsyncLoggingMiddleware) MetricsHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		metrics := alm.GetMetrics()
		if err := json.NewEncoder(w).Encode(metrics); err != nil {
			log.Printf("Failed to encode metrics: %v", err)
		}
	}
}

// Shutdown ミドルウェアを正常に停止
func (alm *AsyncLoggingMiddleware) Shutdown(ctx context.Context) error {
	// 新しいログの受け付けを停止
	alm.cancel()

	// 完了を待機
	done := make(chan struct{})
	go func() {
		alm.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("非同期ログミドルウェアが正常に停止しました")
		return nil
	case <-ctx.Done():
		log.Println("非同期ログミドルウェアの停止がタイムアウトしました")
		return ctx.Err()
	}
}

// 使用例とテスト用のmain関数
func main() {
	// 設定
	config := LoggingConfig{
		BatchSize:     100,
		FlushInterval: 5 * time.Second,
		MaxQueueSize:  10000,
		OutputFile:    "access.log",
		RetryAttempts: 3,
		RetryDelay:    1 * time.Second,
	}

	// ミドルウェア作成
	loggingMiddleware := NewAsyncLoggingMiddleware(config)

	// テスト用のHTTPハンドラー
	testHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// レスポンス時間をシミュレート
		time.Sleep(time.Duration(10+len(r.URL.Path)) * time.Millisecond)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)

		response := map[string]interface{}{
			"message": "成功",
			"path":    r.URL.Path,
			"method":  r.Method,
		}
		if err := json.NewEncoder(w).Encode(response); err != nil {
			log.Printf("Failed to encode response: %v", err)
		}
	})

	// ミドルウェアを適用
	mux := http.NewServeMux()
	mux.Handle("/api/", loggingMiddleware.Middleware(testHandler))
	mux.Handle("/health", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write([]byte("OK")); err != nil {
			log.Printf("Failed to write health response: %v", err)
		}
	}))
	mux.Handle("/metrics", loggingMiddleware.MetricsHandler())

	// サーバー起動
	server := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	log.Println("サーバーを開始します: http://localhost:8080")
	log.Println("メトリクス: http://localhost:8080/metrics")
	log.Println("テストAPI: http://localhost:8080/api/test")

	// グレースフルシャットダウンの設定
	go func() {
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("サーバー起動エラー: %v", err)
		}
	}()

	// 負荷テスト用のサンプルリクエスト生成
	go func() {
		time.Sleep(2 * time.Second) // サーバー起動まで待機

		for i := 0; i < 1000; i++ {
			go func(id int) {
				resp, err := http.Get(fmt.Sprintf("http://localhost:8080/api/test/%d", id))
				if err == nil {
					if err := resp.Body.Close(); err != nil {
						log.Printf("Failed to close response body: %v", err)
					}
				}
			}(i)

			if i%100 == 0 {
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()

	// 10秒後に停止
	time.Sleep(10 * time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	log.Println("サーバーを停止しています...")
	if err := server.Shutdown(ctx); err != nil {
		log.Printf("サーバー停止エラー: %v", err)
	}

	if err := loggingMiddleware.Shutdown(ctx); err != nil {
		log.Printf("ミドルウェア停止エラー: %v", err)
	}

	// 最終メトリクス表示
	finalMetrics := loggingMiddleware.GetMetrics()
	log.Printf("最終メトリクス: %+v", finalMetrics)
}
