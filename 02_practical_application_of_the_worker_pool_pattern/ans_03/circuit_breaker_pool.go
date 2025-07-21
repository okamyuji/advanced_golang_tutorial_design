package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// CircuitStateはサーキットブレーカーの状態です
type CircuitState int

const (
	CircuitClosed CircuitState = iota
	CircuitOpen
	CircuitHalfOpen
)

// HTTPTaskはHTTPリクエストタスクです
type HTTPTask struct {
	ID         int
	URL        string
	Method     string
	Headers    map[string]string
	Body       []byte
	Timeout    time.Duration
	MaxRetries int
	RetryCount int
}

// HTTPResultはHTTP結果です
type HTTPResult struct {
	TaskID     int
	StatusCode int
	Body       []byte
	Duration   time.Duration
	Success    bool
	Error      error
	RetryCount int
	WorkerID   int
}

// CircuitBreakerはサーキットブレーカーです
type CircuitBreaker struct {
	mu                  sync.RWMutex
	state               CircuitState
	failureCount        int64
	successCount        int64
	consecutiveFailures int64
	lastFailureTime     time.Time
	lastSuccessTime     time.Time

	// 設定
	failureThreshold int64
	recoveryTimeout  time.Duration
	halfOpenMaxCalls int64
	halfOpenCalls    int64
}

// HTTPWorkerPoolはサーキットブレーカー付きHTTPワーカープールです
type HTTPWorkerPool struct {
	// 基本設定
	workerCount int

	// HTTP設定
	client         *http.Client
	circuitBreaker *CircuitBreaker

	// キュー
	taskQueue   chan HTTPTask
	retryQueue  chan HTTPTask
	resultQueue chan HTTPResult

	// 制御
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// 統計
	stats *HTTPStats
}

// HTTPWorkerはHTTPワーカーです
type HTTPWorker struct {
	ID   int
	pool *HTTPWorkerPool
}

// HTTPStatsはHTTP統計です
type HTTPStats struct {
	mu                  sync.RWMutex
	totalRequests       int64
	successfulRequests  int64
	failedRequests      int64
	retryRequests       int64
	circuitOpenCount    int64
	averageResponseTime time.Duration
	statusCodeCounts    map[int]int64
}

// NewCircuitBreakerは新しいサーキットブレーカーを作成します
func NewCircuitBreaker(failureThreshold int64, recoveryTimeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		state:            CircuitClosed,
		failureThreshold: failureThreshold,
		recoveryTimeout:  recoveryTimeout,
		halfOpenMaxCalls: 5,
	}
}

// CanExecuteは実行可能かチェックします
func (cb *CircuitBreaker) CanExecute() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case CircuitClosed:
		return true
	case CircuitOpen:
		// 回復タイムアウトが過ぎた場合、Half-Openに移行
		if time.Since(cb.lastFailureTime) > cb.recoveryTimeout {
			cb.state = CircuitHalfOpen
			cb.halfOpenCalls = 0
			log.Println("Circuit Breaker: OPEN -> HALF-OPEN")
			return true
		}
		return false
	case CircuitHalfOpen:
		return cb.halfOpenCalls < cb.halfOpenMaxCalls
	}
	return false
}

// RecordSuccessは成功を記録します
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	atomic.AddInt64(&cb.successCount, 1)
	cb.consecutiveFailures = 0
	cb.lastSuccessTime = time.Now()

	if cb.state == CircuitHalfOpen {
		cb.halfOpenCalls++
		if cb.halfOpenCalls >= cb.halfOpenMaxCalls {
			cb.state = CircuitClosed
			log.Println("Circuit Breaker: HALF-OPEN -> CLOSED")
		}
	}
}

// RecordFailureは失敗を記録します
func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	atomic.AddInt64(&cb.failureCount, 1)
	cb.consecutiveFailures++
	cb.lastFailureTime = time.Now()

	if cb.state == CircuitHalfOpen {
		cb.state = CircuitOpen
		log.Println("Circuit Breaker: HALF-OPEN -> OPEN (failure detected)")
		return
	}

	if cb.state == CircuitClosed && cb.consecutiveFailures >= cb.failureThreshold {
		cb.state = CircuitOpen
		log.Printf("Circuit Breaker: CLOSED -> OPEN (%d consecutive failures)", cb.consecutiveFailures)
	}
}

// GetStateは現在の状態を取得します
func (cb *CircuitBreaker) GetState() CircuitState {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.state
}

// GetStatsは統計を取得します
func (cb *CircuitBreaker) GetStats() (int64, int64, CircuitState) {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	success := atomic.LoadInt64(&cb.successCount)
	failure := atomic.LoadInt64(&cb.failureCount)

	return success, failure, cb.state
}

// NewHTTPWorkerPoolは新しいHTTPワーカープールを作成します
func NewHTTPWorkerPool(workerCount int) *HTTPWorkerPool {
	ctx, cancel := context.WithCancel(context.Background())

	// HTTPクライアントを設定
	client := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        50,
			MaxIdleConnsPerHost: 10,
			IdleConnTimeout:     60 * time.Second,
		},
	}

	// サーキットブレーカーを設定（5回失敗で開、30秒で回復試行）
	circuitBreaker := NewCircuitBreaker(5, 30*time.Second)

	return &HTTPWorkerPool{
		workerCount:    workerCount,
		client:         client,
		circuitBreaker: circuitBreaker,
		taskQueue:      make(chan HTTPTask, workerCount*2),
		retryQueue:     make(chan HTTPTask, workerCount*2),
		resultQueue:    make(chan HTTPResult, workerCount*2),
		ctx:            ctx,
		cancel:         cancel,
		stats: &HTTPStats{
			statusCodeCounts: make(map[int]int64),
		},
	}
}

// StartはHTTPワーカープールを開始します
func (hwp *HTTPWorkerPool) Start() error {
	// HTTPワーカーを開始
	for i := 0; i < hwp.workerCount; i++ {
		worker := &HTTPWorker{
			ID:   i,
			pool: hwp,
		}

		hwp.wg.Add(1)
		go worker.run()
	}

	// リトライ処理を開始
	hwp.wg.Add(1)
	go hwp.retryHandler()

	// 結果処理を開始
	hwp.wg.Add(1)
	go hwp.resultHandler()

	// 統計レポートを開始
	hwp.wg.Add(1)
	go hwp.statsReporter()

	log.Printf("HTTP worker pool started with %d workers", hwp.workerCount)
	return nil
}

// runはワーカーのメインループです
func (hw *HTTPWorker) run() {
	defer hw.pool.wg.Done()

	for {
		select {
		case <-hw.pool.ctx.Done():
			return
		case task := <-hw.pool.taskQueue:
			result := hw.executeHTTPTask(task)

			select {
			case hw.pool.resultQueue <- result:
			case <-hw.pool.ctx.Done():
				return
			}
		}
	}
}

// executeHTTPTaskはHTTPタスクを実行します
func (hw *HTTPWorker) executeHTTPTask(task HTTPTask) HTTPResult {
	start := time.Now()

	result := HTTPResult{
		TaskID:     task.ID,
		WorkerID:   hw.ID,
		RetryCount: task.RetryCount,
	}

	// サーキットブレーカーをチェック
	if !hw.pool.circuitBreaker.CanExecute() {
		result.Success = false
		result.Error = fmt.Errorf("circuit breaker is open")
		result.Duration = time.Since(start)
		atomic.AddInt64(&hw.pool.stats.circuitOpenCount, 1)

		// サーキットブレーカーが開いている場合、リトライしない
		return result
	}

	// HTTPリクエストを作成
	req, err := http.NewRequestWithContext(hw.pool.ctx, task.Method, task.URL, nil)
	if err != nil {
		result.Success = false
		result.Error = fmt.Errorf("failed to create request: %v", err)
		result.Duration = time.Since(start)
		hw.pool.circuitBreaker.RecordFailure()
		return result
	}

	// ヘッダーを設定
	for key, value := range task.Headers {
		req.Header.Set(key, value)
	}

	// タイムアウトを設定
	if task.Timeout > 0 {
		ctx, cancel := context.WithTimeout(hw.pool.ctx, task.Timeout)
		defer cancel()
		req = req.WithContext(ctx)
	}

	// リクエストを実行
	resp, err := hw.pool.client.Do(req)
	if err != nil {
		result.Success = false
		result.Error = fmt.Errorf("HTTP request failed: %v", err)
		result.Duration = time.Since(start)
		hw.pool.circuitBreaker.RecordFailure()

		// リトライ可能なエラーの場合はリトライキューに追加
		hw.scheduleRetry(task, err)
		return result
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.Printf("Failed to close response body: %v", err)
		}
	}()

	result.StatusCode = resp.StatusCode
	result.Duration = time.Since(start)

	// ステータスコードに基づいて成功/失敗を判定
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		result.Success = true
		hw.pool.circuitBreaker.RecordSuccess()
	} else if resp.StatusCode >= 500 {
		// 5xx系エラーは失敗として記録し、リトライ対象
		result.Success = false
		result.Error = fmt.Errorf("server error: %d", resp.StatusCode)
		hw.pool.circuitBreaker.RecordFailure()
		hw.scheduleRetry(task, result.Error)
	} else {
		// 4xx系エラーはクライアントエラーとして扱い、リトライしない
		result.Success = false
		result.Error = fmt.Errorf("client error: %d", resp.StatusCode)
		hw.pool.circuitBreaker.RecordFailure()
	}

	log.Printf("Worker %d: %s %s -> %d (%v)",
		hw.ID, task.Method, task.URL, resp.StatusCode, result.Duration)

	return result
}

// scheduleRetryはリトライをスケジュールします
func (hw *HTTPWorker) scheduleRetry(task HTTPTask, err error) {
	if task.RetryCount >= task.MaxRetries {
		log.Printf("Max retries exceeded for task %d (error: %v)", task.ID, err)
		return
	}

	retryTask := task
	retryTask.RetryCount++

	// 指数バックオフでリトライ遅延を計算
	backoffDelay := time.Duration(math.Pow(2, float64(retryTask.RetryCount))) * time.Second
	if backoffDelay > 30*time.Second {
		backoffDelay = 30 * time.Second
	}

	// ジッターを追加してサンダリングハード問題を回避
	jitter := time.Duration(rand.Intn(1000)) * time.Millisecond
	backoffDelay += jitter

	atomic.AddInt64(&hw.pool.stats.retryRequests, 1)

	go func() {
		select {
		case <-time.After(backoffDelay):
			select {
			case hw.pool.retryQueue <- retryTask:
				log.Printf("Scheduled retry %d for task %d after %v",
					retryTask.RetryCount, task.ID, backoffDelay)
			case <-hw.pool.ctx.Done():
			}
		case <-hw.pool.ctx.Done():
		}
	}()
}

// retryHandlerはリトライを処理します
func (hwp *HTTPWorkerPool) retryHandler() {
	defer hwp.wg.Done()

	for {
		select {
		case <-hwp.ctx.Done():
			return
		case retryTask, ok := <-hwp.retryQueue:
			if !ok {
				// retryQueue が閉じられた
				return
			}
			// リトライタスクを通常のキューに戻す
			select {
			case hwp.taskQueue <- retryTask:
			case <-hwp.ctx.Done():
				return
			default:
				// taskQueueが満杯または閉じられている場合はドロップ
				log.Printf("Dropping retry task %d: queue full or closed", retryTask.ID)
			}
		}
	}
}

// SubmitTaskはタスクを追加します
func (hwp *HTTPWorkerPool) SubmitTask(task HTTPTask) error {
	select {
	case hwp.taskQueue <- task:
		atomic.AddInt64(&hwp.stats.totalRequests, 1)
		return nil
	case <-hwp.ctx.Done():
		return fmt.Errorf("pool is shutting down")
	default:
		return fmt.Errorf("task queue is full")
	}
}

// resultHandlerは結果を処理します
func (hwp *HTTPWorkerPool) resultHandler() {
	defer hwp.wg.Done()

	for {
		select {
		case <-hwp.ctx.Done():
			return
		case result := <-hwp.resultQueue:
			hwp.updateStats(result)
		}
	}
}

// updateStatsは統計を更新します
func (hwp *HTTPWorkerPool) updateStats(result HTTPResult) {
	hwp.stats.mu.Lock()
	defer hwp.stats.mu.Unlock()

	if result.Success {
		atomic.AddInt64(&hwp.stats.successfulRequests, 1)
	} else {
		atomic.AddInt64(&hwp.stats.failedRequests, 1)
	}

	if result.StatusCode > 0 {
		hwp.stats.statusCodeCounts[result.StatusCode]++
	}

	// 平均応答時間を更新
	total := atomic.LoadInt64(&hwp.stats.totalRequests)
	hwp.stats.averageResponseTime = time.Duration(
		(int64(hwp.stats.averageResponseTime)*total + int64(result.Duration)) / (total + 1))
}

// statsReporterは統計を定期的に報告します
func (hwp *HTTPWorkerPool) statsReporter() {
	defer hwp.wg.Done()

	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-hwp.ctx.Done():
			return
		case <-ticker.C:
			hwp.printStats()
		}
	}
}

// printStatsは統計を出力します
func (hwp *HTTPWorkerPool) printStats() {
	hwp.stats.mu.RLock()
	defer hwp.stats.mu.RUnlock()

	total := atomic.LoadInt64(&hwp.stats.totalRequests)
	successful := atomic.LoadInt64(&hwp.stats.successfulRequests)
	failed := atomic.LoadInt64(&hwp.stats.failedRequests)
	retries := atomic.LoadInt64(&hwp.stats.retryRequests)
	circuitOpen := atomic.LoadInt64(&hwp.stats.circuitOpenCount)

	successRate := float64(0)
	if total > 0 {
		successRate = float64(successful) / float64(total) * 100
	}

	cbSuccess, cbFailure, cbState := hwp.circuitBreaker.GetStats()
	stateStr := map[CircuitState]string{
		CircuitClosed:   "CLOSED",
		CircuitOpen:     "OPEN",
		CircuitHalfOpen: "HALF-OPEN",
	}[cbState]

	log.Printf("HTTP Pool Stats: Total=%d, Success=%d (%.1f%%), Failed=%d, Retries=%d, "+
		"CircuitOpen=%d, AvgTime=%v",
		total, successful, successRate, failed, retries, circuitOpen, hwp.stats.averageResponseTime)

	log.Printf("Circuit Breaker: State=%s, Success=%d, Failure=%d",
		stateStr, cbSuccess, cbFailure)

	// ステータスコード分布
	if len(hwp.stats.statusCodeCounts) > 0 {
		log.Printf("Status codes: %v", hwp.stats.statusCodeCounts)
	}
}

// Shutdownはプールを停止します
func (hwp *HTTPWorkerPool) Shutdown(timeout time.Duration) error {
	log.Println("Starting HTTP worker pool shutdown...")

	// 1. 新しいタスクの受付を停止
	close(hwp.taskQueue)
	close(hwp.retryQueue)

	// 2. ワーカーに停止シグナルを送信
	hwp.cancel()

	// 3. ワーカーの終了を待機
	done := make(chan struct{})
	go func() {
		hwp.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("HTTP worker pool shutdown completed")
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("shutdown timeout exceeded")
	}
}

// GetStatsは統計を取得します
func (hwp *HTTPWorkerPool) GetStats() (int64, int64, int64, float64) {
	total := atomic.LoadInt64(&hwp.stats.totalRequests)
	successful := atomic.LoadInt64(&hwp.stats.successfulRequests)
	failed := atomic.LoadInt64(&hwp.stats.failedRequests)

	successRate := float64(0)
	if total > 0 {
		successRate = float64(successful) / float64(total) * 100
	}

	return total, successful, failed, successRate
}

func main() {
	// HTTPワーカープールを作成
	pool := NewHTTPWorkerPool(4)

	if err := pool.Start(); err != nil {
		log.Fatalf("Failed to start pool: %v", err)
	}

	// テスト用のURL群（一部は意図的にエラーを返す）
	testURLs := []string{
		"https://httpbin.org/status/200",
		"https://httpbin.org/status/500", // サーバーエラー
		"https://httpbin.org/delay/1",
		"https://httpbin.org/status/503", // サーバーエラー
		"https://httpbin.org/json",
		"https://httpbin.org/status/500", // サーバーエラー
		"https://example.com",
		"https://httpbin.org/status/502", // サーバーエラー
	}

	// タスクを継続的に送信
	go func() {
		taskID := 1
		for {
			select {
			case <-pool.ctx.Done():
				return
			default:
				url := testURLs[taskID%len(testURLs)]
				task := HTTPTask{
					ID:         taskID,
					URL:        url,
					Method:     "GET",
					Headers:    map[string]string{"User-Agent": "CircuitBreakerPool/1.0"},
					Timeout:    10 * time.Second,
					MaxRetries: 3,
				}

				if err := pool.SubmitTask(task); err != nil {
					log.Printf("Failed to submit task %d: %v", taskID, err)
				}

				taskID++
				time.Sleep(500 * time.Millisecond)
			}
		}
	}()

	// 90秒間動作させる（サーキットブレーカーの動作を観察）
	time.Sleep(90 * time.Second)

	// 最終統計を表示
	total, successful, failed, successRate := pool.GetStats()
	log.Printf("Final Results: Total=%d, Successful=%d, Failed=%d, Success Rate=%.1f%%",
		total, successful, failed, successRate)

	if err := pool.Shutdown(10 * time.Second); err != nil {
		log.Printf("Shutdown error: %v", err)
	}
}
