package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// CircuitBreakerState はサーキットブレーカーの状態を表します
type CircuitBreakerState int

const (
	Closed CircuitBreakerState = iota
	Open
	HalfOpen
)

func (s CircuitBreakerState) String() string {
	switch s {
	case Closed:
		return "CLOSED"
	case Open:
		return "OPEN"
	case HalfOpen:
		return "HALF_OPEN"
	default:
		return "UNKNOWN"
	}
}

// CircuitBreakerMetrics はサーキットブレーカーのメトリクスです
type CircuitBreakerMetrics struct {
	TotalRequests       uint64        `json:"total_requests"`
	SuccessfulRequests  uint64        `json:"successful_requests"`
	FailedRequests      uint64        `json:"failed_requests"`
	ConsecutiveFailures uint64        `json:"consecutive_failures"`
	LastFailureTime     time.Time     `json:"last_failure_time"`
	LastSuccessTime     time.Time     `json:"last_success_time"`
	AverageResponseTime time.Duration `json:"average_response_time"`
	TotalResponseTime   time.Duration `json:"total_response_time"`
	SlowRequestCount    uint64        `json:"slow_request_count"`
	CircuitOpenCount    uint64        `json:"circuit_open_count"`
	LastStateChange     time.Time     `json:"last_state_change"`
}

// AdvancedCircuitBreakerConfig はサーキットブレーカーの設定です
type AdvancedCircuitBreakerConfig struct {
	Name                  string
	FailureThreshold      uint64
	FailureRateThreshold  float64
	SlowCallThreshold     time.Duration
	SlowCallRateThreshold float64
	MinimumRequestCount   uint64
	OpenTimeout           time.Duration
	MaxConcurrentRequests uint64
	SlidingWindowSize     int
	OnStateChange         func(name string, from, to CircuitBreakerState)
	OnRequestResult       func(success bool, duration time.Duration)
}

// AdvancedCircuitBreaker は高度なサーキットブレーカーです
type AdvancedCircuitBreaker struct {
	config      AdvancedCircuitBreakerConfig
	state       CircuitBreakerState
	metrics     CircuitBreakerMetrics
	stateExpiry time.Time

	// スライディングウィンドウ
	requestWindow []RequestResult
	windowIndex   int
	windowFull    bool

	// 並行制御
	activeRequests int64
	mutex          sync.RWMutex

	// 状態管理
	generation uint64
}

// RequestResult はリクエスト結果を表します
type RequestResult struct {
	Success    bool
	Duration   time.Duration
	Timestamp  time.Time
	IsSlowCall bool
}

var (
	ErrCircuitOpen     = errors.New("circuit breaker is open")
	ErrTooManyRequests = errors.New("too many concurrent requests")
	ErrRequestTimeout  = errors.New("request timeout")
)

// NewAdvancedCircuitBreaker は新しい高度なサーキットブレーカーを作成します
func NewAdvancedCircuitBreaker(config AdvancedCircuitBreakerConfig) *AdvancedCircuitBreaker {
	// デフォルト値設定
	if config.FailureThreshold == 0 {
		config.FailureThreshold = 3
	}
	if config.FailureRateThreshold == 0 {
		config.FailureRateThreshold = 0.5
	}
	if config.SlowCallThreshold == 0 {
		config.SlowCallThreshold = 5 * time.Second
	}
	if config.SlowCallRateThreshold == 0 {
		config.SlowCallRateThreshold = 0.5
	}
	if config.MinimumRequestCount == 0 {
		config.MinimumRequestCount = 10
	}
	if config.OpenTimeout == 0 {
		config.OpenTimeout = 30 * time.Second
	}
	if config.MaxConcurrentRequests == 0 {
		config.MaxConcurrentRequests = 100
	}
	if config.SlidingWindowSize == 0 {
		config.SlidingWindowSize = 100
	}

	return &AdvancedCircuitBreaker{
		config:        config,
		state:         Closed,
		requestWindow: make([]RequestResult, config.SlidingWindowSize),
		metrics: CircuitBreakerMetrics{
			LastStateChange: time.Now(),
		},
	}
}

// Execute はサーキットブレーカーを通してリクエストを実行します
func (acb *AdvancedCircuitBreaker) Execute(request func() error) error {
	// 実行前チェック
	if err := acb.beforeRequest(); err != nil {
		return err
	}

	// アクティブリクエスト数をインクリメント
	defer atomic.AddInt64(&acb.activeRequests, -1)

	start := time.Now()
	var requestErr error

	// パニック対応
	defer func() {
		if r := recover(); r != nil {
			requestErr = fmt.Errorf("panic recovered: %v", r)
		}

		duration := time.Since(start)
		success := requestErr == nil && duration <= acb.config.SlowCallThreshold
		acb.afterRequest(success, duration, requestErr)
	}()

	requestErr = request()
	return requestErr
}

// ExecuteWithTimeout はタイムアウト付きでリクエストを実行します
func (acb *AdvancedCircuitBreaker) ExecuteWithTimeout(ctx context.Context, timeout time.Duration, request func() error) error {
	if err := acb.beforeRequest(); err != nil {
		return err
	}

	defer atomic.AddInt64(&acb.activeRequests, -1)

	type result struct {
		err      error
		duration time.Duration
	}

	resultChan := make(chan result, 1)
	start := time.Now()

	go func() {
		defer func() {
			if r := recover(); r != nil {
				resultChan <- result{
					err:      fmt.Errorf("panic recovered: %v", r),
					duration: time.Since(start),
				}
			}
		}()

		err := request()
		resultChan <- result{
			err:      err,
			duration: time.Since(start),
		}
	}()

	select {
	case res := <-resultChan:
		success := res.err == nil && res.duration <= acb.config.SlowCallThreshold
		acb.afterRequest(success, res.duration, res.err)
		return res.err

	case <-time.After(timeout):
		duration := time.Since(start)
		acb.afterRequest(false, duration, ErrRequestTimeout)
		return ErrRequestTimeout

	case <-ctx.Done():
		duration := time.Since(start)
		acb.afterRequest(false, duration, ctx.Err())
		return ctx.Err()
	}
}

// beforeRequest はリクエスト実行前の処理を行います
func (acb *AdvancedCircuitBreaker) beforeRequest() error {
	acb.mutex.Lock()
	defer acb.mutex.Unlock()

	// 状態更新
	acb.updateState()

	// オープン状態チェック
	if acb.state == Open {
		return ErrCircuitOpen
	}

	// 並行リクエスト数チェック
	if atomic.LoadInt64(&acb.activeRequests) >= int64(acb.config.MaxConcurrentRequests) {
		return ErrTooManyRequests
	}

	// ハーフオープン状態での制限
	if acb.state == HalfOpen {
		// ハーフオープン時は1つのリクエストのみ許可
		if atomic.LoadInt64(&acb.activeRequests) > 0 {
			return ErrTooManyRequests
		}
	}

	atomic.AddInt64(&acb.activeRequests, 1)
	atomic.AddUint64(&acb.metrics.TotalRequests, 1)

	return nil
}

// afterRequest はリクエスト実行後の処理を行います
func (acb *AdvancedCircuitBreaker) afterRequest(success bool, duration time.Duration, err error) {
	acb.mutex.Lock()
	defer acb.mutex.Unlock()

	now := time.Now()

	// エラー情報をログに記録
	if err != nil {
		log.Printf("Circuit Breaker [%s] - Request failed: %v (duration: %v)",
			acb.config.Name, err, duration)
	}

	// メトリクス更新
	acb.updateMetrics(success, duration, now)

	// リクエスト結果をウィンドウに追加
	result := RequestResult{
		Success:    success,
		Duration:   duration,
		Timestamp:  now,
		IsSlowCall: duration > acb.config.SlowCallThreshold,
	}
	acb.addToWindow(result)

	// 状態遷移判定
	acb.evaluateStateTransition(success)

	// コールバック実行
	if acb.config.OnRequestResult != nil {
		go acb.config.OnRequestResult(success, duration)
	}
}

// updateMetrics はメトリクスを更新します
func (acb *AdvancedCircuitBreaker) updateMetrics(success bool, duration time.Duration, timestamp time.Time) {
	acb.metrics.TotalResponseTime += duration

	if success {
		atomic.AddUint64(&acb.metrics.SuccessfulRequests, 1)
		atomic.StoreUint64(&acb.metrics.ConsecutiveFailures, 0)
		acb.metrics.LastSuccessTime = timestamp
	} else {
		atomic.AddUint64(&acb.metrics.FailedRequests, 1)
		atomic.AddUint64(&acb.metrics.ConsecutiveFailures, 1)
		acb.metrics.LastFailureTime = timestamp
	}

	if duration > acb.config.SlowCallThreshold {
		atomic.AddUint64(&acb.metrics.SlowRequestCount, 1)
	}

	// 平均応答時間の更新
	totalRequests := atomic.LoadUint64(&acb.metrics.TotalRequests)
	if totalRequests > 0 {
		acb.metrics.AverageResponseTime = acb.metrics.TotalResponseTime / time.Duration(totalRequests)
	}
}

// addToWindow はリクエスト結果をスライディングウィンドウに追加します
func (acb *AdvancedCircuitBreaker) addToWindow(result RequestResult) {
	acb.requestWindow[acb.windowIndex] = result
	acb.windowIndex = (acb.windowIndex + 1) % len(acb.requestWindow)

	if acb.windowIndex == 0 {
		acb.windowFull = true
	}
}

// evaluateStateTransition は状態遷移を評価します
func (acb *AdvancedCircuitBreaker) evaluateStateTransition(success bool) {
	switch acb.state {
	case Closed:
		if acb.shouldTripToOpen() {
			acb.transitionTo(Open)
		}

	case HalfOpen:
		if success {
			acb.transitionTo(Closed)
		} else {
			acb.transitionTo(Open)
		}

	case Open:
		// Open状態の更新はupdateStateで処理
	}
}

// shouldTripToOpen はオープン状態に遷移すべきかを判定します
func (acb *AdvancedCircuitBreaker) shouldTripToOpen() bool {
	windowSize := acb.getEffectiveWindowSize()
	if windowSize < int(acb.config.MinimumRequestCount) {
		return false
	}

	// 連続失敗チェック
	if acb.metrics.ConsecutiveFailures >= acb.config.FailureThreshold {
		return true
	}

	// 失敗率チェック
	failureRate := acb.calculateFailureRate()
	if failureRate >= acb.config.FailureRateThreshold {
		return true
	}

	// スロー呼び出し率チェック
	slowCallRate := acb.calculateSlowCallRate()
	return slowCallRate >= acb.config.SlowCallRateThreshold
}

// calculateFailureRate は失敗率を計算します
func (acb *AdvancedCircuitBreaker) calculateFailureRate() float64 {
	windowSize := acb.getEffectiveWindowSize()
	if windowSize == 0 {
		return 0.0
	}

	failures := 0
	for i := 0; i < windowSize; i++ {
		if !acb.requestWindow[i].Success {
			failures++
		}
	}

	return float64(failures) / float64(windowSize)
}

// calculateSlowCallRate はスロー呼び出し率を計算します
func (acb *AdvancedCircuitBreaker) calculateSlowCallRate() float64 {
	windowSize := acb.getEffectiveWindowSize()
	if windowSize == 0 {
		return 0.0
	}

	slowCalls := 0
	for i := 0; i < windowSize; i++ {
		if acb.requestWindow[i].IsSlowCall {
			slowCalls++
		}
	}

	return float64(slowCalls) / float64(windowSize)
}

// getEffectiveWindowSize は有効なウィンドウサイズを取得します
func (acb *AdvancedCircuitBreaker) getEffectiveWindowSize() int {
	if acb.windowFull {
		return len(acb.requestWindow)
	}
	return acb.windowIndex
}

// updateState は状態を更新します
func (acb *AdvancedCircuitBreaker) updateState() {
	if acb.state == Open && time.Now().After(acb.stateExpiry) {
		acb.transitionTo(HalfOpen)
	}
}

// transitionTo は指定された状態に遷移します
func (acb *AdvancedCircuitBreaker) transitionTo(newState CircuitBreakerState) {
	if acb.state == newState {
		return
	}

	oldState := acb.state
	acb.state = newState
	acb.generation++
	acb.metrics.LastStateChange = time.Now()

	// 状態固有の処理
	switch newState {
	case Open:
		acb.stateExpiry = time.Now().Add(acb.config.OpenTimeout)
		atomic.AddUint64(&acb.metrics.CircuitOpenCount, 1)
	case HalfOpen:
		acb.stateExpiry = time.Time{}
	case Closed:
		acb.stateExpiry = time.Time{}
		// 連続失敗カウンターをリセット
		atomic.StoreUint64(&acb.metrics.ConsecutiveFailures, 0)
	}

	log.Printf("Circuit Breaker [%s]: %s -> %s", acb.config.Name, oldState, newState)

	// コールバック実行
	if acb.config.OnStateChange != nil {
		go acb.config.OnStateChange(acb.config.Name, oldState, newState)
	}
}

// GetState は現在の状態を取得します
func (acb *AdvancedCircuitBreaker) GetState() CircuitBreakerState {
	acb.mutex.RLock()
	defer acb.mutex.RUnlock()

	acb.updateState()
	return acb.state
}

// GetMetrics はメトリクスを取得します
func (acb *AdvancedCircuitBreaker) GetMetrics() CircuitBreakerMetrics {
	acb.mutex.RLock()
	defer acb.mutex.RUnlock()

	// コピーを返す
	return acb.metrics
}

// Reset はサーキットブレーカーをリセットします
func (acb *AdvancedCircuitBreaker) Reset() {
	acb.mutex.Lock()
	defer acb.mutex.Unlock()

	acb.state = Closed
	acb.stateExpiry = time.Time{}
	acb.metrics = CircuitBreakerMetrics{
		LastStateChange: time.Now(),
	}
	acb.requestWindow = make([]RequestResult, acb.config.SlidingWindowSize)
	acb.windowIndex = 0
	acb.windowFull = false
	atomic.StoreInt64(&acb.activeRequests, 0)
}

// HTTPClientWithCircuitBreaker はサーキットブレーカー付きHTTPクライアントです
type HTTPClientWithCircuitBreaker struct {
	client         *http.Client
	circuitBreaker *AdvancedCircuitBreaker
}

// NewHTTPClientWithCircuitBreaker は新しいHTTPクライアントを作成します
func NewHTTPClientWithCircuitBreaker(client *http.Client, cb *AdvancedCircuitBreaker) *HTTPClientWithCircuitBreaker {
	if client == nil {
		client = &http.Client{
			Timeout: 30 * time.Second,
		}
	}

	return &HTTPClientWithCircuitBreaker{
		client:         client,
		circuitBreaker: cb,
	}
}

// Get はサーキットブレーカー付きのGETリクエストを実行します
func (hc *HTTPClientWithCircuitBreaker) Get(url string) (*http.Response, error) {
	var resp *http.Response

	err := hc.circuitBreaker.Execute(func() error {
		var err error
		resp, err = hc.client.Get(url)
		return err
	})

	return resp, err
}

// Post はサーキットブレーカー付きのPOSTリクエストを実行します
func (hc *HTTPClientWithCircuitBreaker) Post(url, contentType string, body interface{}) (*http.Response, error) {
	var resp *http.Response

	err := hc.circuitBreaker.Execute(func() error {
		var err error
		// body処理は簡略化
		resp, err = hc.client.Post(url, contentType, nil)
		return err
	})

	return resp, err
}

// GetCircuitBreaker はサーキットブレーカーを取得します
func (hc *HTTPClientWithCircuitBreaker) GetCircuitBreaker() *AdvancedCircuitBreaker {
	return hc.circuitBreaker
}

// 使用例とテスト用のmain関数
func main() {
	// サーキットブレーカー設定
	config := AdvancedCircuitBreakerConfig{
		Name:                  "external-api-breaker",
		FailureThreshold:      3,                // 連続3回失敗でオープン
		FailureRateThreshold:  0.5,              // 失敗率50%でオープン
		SlowCallThreshold:     5 * time.Second,  // 5秒でスロー呼び出し
		SlowCallRateThreshold: 0.5,              // スロー呼び出し率50%でオープン
		MinimumRequestCount:   10,               // 最小リクエスト数
		OpenTimeout:           30 * time.Second, // 30秒後にハーフオープン
		MaxConcurrentRequests: 50,               // 最大並行リクエスト数
		SlidingWindowSize:     100,              // スライディングウィンドウサイズ
		OnStateChange: func(name string, from, to CircuitBreakerState) {
			log.Printf("Circuit Breaker [%s]: %s -> %s", name, from, to)
		},
		OnRequestResult: func(success bool, duration time.Duration) {
			status := "SUCCESS"
			if !success {
				status = "FAILURE"
			}
			log.Printf("Request: %s (duration: %v)", status, duration)
		},
	}

	// サーキットブレーカー作成
	cb := NewAdvancedCircuitBreaker(config)

	// HTTPクライアント作成
	httpClient := NewHTTPClientWithCircuitBreaker(nil, cb)

	log.Println("=== Advanced Circuit Breaker Demo ===")

	// テストURL群
	testURLs := []string{
		"http://httpbin.org/status/200",      // 成功
		"http://httpbin.org/status/500",      // 失敗
		"http://httpbin.org/delay/6",         // スロー呼び出し
		"http://invalid-domain-for-test.com", // DNS失敗
	}

	// テスト実行
	for i := 0; i < 20; i++ {
		url := testURLs[i%len(testURLs)]

		log.Printf("\n--- Request %d: %s ---", i+1, url)

		start := time.Now()
		resp, err := httpClient.Get(url)
		duration := time.Since(start)

		if err != nil {
			log.Printf("Error: %v (Duration: %v)", err, duration)
		} else {
			log.Printf("Success: %d (Duration: %v)", resp.StatusCode, duration)
			if err := resp.Body.Close(); err != nil {
				log.Printf("Failed to close response body: %v", err)
			}
		}

		// 現在の状態とメトリクスを表示
		state := cb.GetState()
		metrics := cb.GetMetrics()

		log.Printf("State: %s", state)
		log.Printf("Metrics: Success=%d, Failed=%d, Consecutive=%d",
			metrics.SuccessfulRequests, metrics.FailedRequests, metrics.ConsecutiveFailures)
		log.Printf("Failure Rate: %.2f%%, Slow Rate: %.2f%%",
			cb.calculateFailureRate()*100, cb.calculateSlowCallRate()*100)

		time.Sleep(2 * time.Second)
	}

	// 最終統計
	finalMetrics := cb.GetMetrics()
	log.Printf("\n=== Final Statistics ===")
	log.Printf("Total Requests: %d", finalMetrics.TotalRequests)
	log.Printf("Successful Requests: %d", finalMetrics.SuccessfulRequests)
	log.Printf("Failed Requests: %d", finalMetrics.FailedRequests)
	log.Printf("Slow Requests: %d", finalMetrics.SlowRequestCount)
	log.Printf("Circuit Opened: %d times", finalMetrics.CircuitOpenCount)
	log.Printf("Average Response Time: %v", finalMetrics.AverageResponseTime)

	log.Println("Demo completed")
}
