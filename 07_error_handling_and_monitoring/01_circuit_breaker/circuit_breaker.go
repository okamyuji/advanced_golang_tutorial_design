package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
)

// State サーキットブレーカーの状態を表します
type State int

const (
	StateClosed State = iota
	StateHalfOpen
	StateOpen
)

func (s State) String() string {
	switch s {
	case StateClosed:
		return "CLOSED"
	case StateHalfOpen:
		return "HALF_OPEN"
	case StateOpen:
		return "OPEN"
	default:
		return "UNKNOWN"
	}
}

// Counts サーキットブレーカーの統計情報です
type Counts struct {
	Requests             uint64
	TotalSuccesses       uint64
	TotalFailures        uint64
	ConsecutiveSuccesses uint64
	ConsecutiveFailures  uint64
	TotalDuration        time.Duration
}

// Settings サーキットブレーカーの設定です
type Settings struct {
	Name                string
	MaxRequests         uint32
	Interval            time.Duration
	Timeout             time.Duration
	ReadyToTrip         func(counts Counts) bool
	OnStateChange       func(name string, from State, to State)
	IsSuccessful        func(err error) bool
	Fallback            func(err error) (interface{}, error)
	SlowCallThreshold   time.Duration
	FailureThreshold    float64
	MinRequestThreshold uint32
}

// CircuitBreaker 高可用性サーキットブレーカーです
type CircuitBreaker struct {
	name                string
	maxRequests         uint32
	interval            time.Duration
	timeout             time.Duration
	readyToTrip         func(counts Counts) bool
	onStateChange       func(name string, from State, to State)
	isSuccessful        func(err error) bool
	fallback            func(err error) (interface{}, error)
	slowCallThreshold   time.Duration
	failureThreshold    float64
	minRequestThreshold uint32

	mutex      sync.Mutex
	state      State
	generation uint64
	counts     Counts
	expiry     time.Time
}

var (
	ErrCircuitBreakerOpen = errors.New("circuit breaker is open")
	ErrTooManyRequests    = errors.New("too many requests")
	ErrOpenTimeout        = errors.New("circuit breaker open timeout")
)

// NewCircuitBreaker 新しいサーキットブレーカーを作成します
func NewCircuitBreaker(st Settings) *CircuitBreaker {
	cb := &CircuitBreaker{
		name:                st.Name,
		interval:            st.Interval,
		timeout:             st.Timeout,
		readyToTrip:         st.ReadyToTrip,
		onStateChange:       st.OnStateChange,
		isSuccessful:        st.IsSuccessful,
		fallback:            st.Fallback,
		slowCallThreshold:   st.SlowCallThreshold,
		failureThreshold:    st.FailureThreshold,
		minRequestThreshold: st.MinRequestThreshold,
	}

	if st.MaxRequests == 0 {
		cb.maxRequests = 1
	} else {
		cb.maxRequests = st.MaxRequests
	}

	if cb.interval <= 0 {
		cb.interval = time.Duration(60) * time.Second
	}

	if cb.timeout <= 0 {
		cb.timeout = time.Duration(60) * time.Second
	}

	if cb.readyToTrip == nil {
		cb.readyToTrip = cb.defaultReadyToTrip
	}

	if cb.isSuccessful == nil {
		cb.isSuccessful = func(err error) bool { return err == nil }
	}

	if cb.slowCallThreshold <= 0 {
		cb.slowCallThreshold = 5 * time.Second
	}

	if cb.failureThreshold <= 0 {
		cb.failureThreshold = 0.5
	}

	if cb.minRequestThreshold == 0 {
		cb.minRequestThreshold = 10
	}

	cb.toNewGeneration(time.Now())

	return cb
}

// defaultReadyToTrip デフォルトのTrip判定ロジックです
func (cb *CircuitBreaker) defaultReadyToTrip(counts Counts) bool {
	return counts.Requests >= uint64(cb.minRequestThreshold) &&
		float64(counts.TotalFailures)/float64(counts.Requests) >= cb.failureThreshold
}

// Name サーキットブレーカーの名前を返します
func (cb *CircuitBreaker) Name() string {
	return cb.name
}

// State 現在の状態を返します
func (cb *CircuitBreaker) State() State {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	now := time.Now()
	state, _ := cb.currentState(now)
	return state
}

// Counts 現在の統計情報を返します
func (cb *CircuitBreaker) Counts() Counts {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	return cb.counts
}

// Execute サーキットブレーカーを通してリクエストを実行します
func (cb *CircuitBreaker) Execute(req func() (interface{}, error)) (interface{}, error) {
	generation, err := cb.beforeRequest()
	if err != nil {
		if cb.fallback != nil {
			return cb.fallback(err)
		}
		return nil, err
	}

	defer func() {
		if r := recover(); r != nil {
			cb.afterRequest(generation, false, 0)
			panic(r)
		}
	}()

	start := time.Now()
	result, err := req()
	duration := time.Since(start)

	// スローコールは失敗として扱う
	isSuccess := cb.isSuccessful(err)
	if isSuccess && duration > cb.slowCallThreshold {
		isSuccess = false
	}

	cb.afterRequest(generation, isSuccess, duration)

	return result, err
}

// ExecuteWithTimeout タイムアウト付きでリクエストを実行します
func (cb *CircuitBreaker) ExecuteWithTimeout(ctx context.Context, timeout time.Duration, req func() (interface{}, error)) (interface{}, error) {
	generation, err := cb.beforeRequest()
	if err != nil {
		if cb.fallback != nil {
			return cb.fallback(err)
		}
		return nil, err
	}

	type result struct {
		value    interface{}
		err      error
		duration time.Duration
	}

	resultChan := make(chan result, 1)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				resultChan <- result{
					value:    nil,
					err:      fmt.Errorf("panic recovered: %v", r),
					duration: 0,
				}
			}
		}()

		start := time.Now()
		value, err := req()
		duration := time.Since(start)

		resultChan <- result{
			value:    value,
			err:      err,
			duration: duration,
		}
	}()

	select {
	case res := <-resultChan:
		cb.afterRequest(generation, cb.isSuccessful(res.err) && res.duration <= cb.slowCallThreshold, res.duration)
		return res.value, res.err
	case <-time.After(timeout):
		cb.afterRequest(generation, false, timeout)
		if cb.fallback != nil {
			return cb.fallback(ErrOpenTimeout)
		}
		return nil, ErrOpenTimeout
	case <-ctx.Done():
		cb.afterRequest(generation, false, 0)
		return nil, ctx.Err()
	}
}

// beforeRequest リクエスト実行前の処理を行います
func (cb *CircuitBreaker) beforeRequest() (uint64, error) {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	now := time.Now()
	state, generation := cb.currentState(now)

	if state == StateOpen {
		return generation, ErrCircuitBreakerOpen
	} else if state == StateHalfOpen && cb.counts.Requests >= uint64(cb.maxRequests) {
		return generation, ErrTooManyRequests
	}

	cb.counts.Requests++
	return generation, nil
}

// afterRequest リクエスト実行後の処理を行います
func (cb *CircuitBreaker) afterRequest(before uint64, success bool, duration time.Duration) {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	now := time.Now()
	state, generation := cb.currentState(now)
	if generation != before {
		return
	}

	if success {
		cb.onSuccess(state, now)
	} else {
		cb.onFailure(state, now)
	}

	// 実行時間を累積
	cb.counts.TotalDuration += duration
}

// onSuccess 成功時の処理を行います
func (cb *CircuitBreaker) onSuccess(state State, now time.Time) {
	cb.counts.TotalSuccesses++
	cb.counts.ConsecutiveSuccesses++
	cb.counts.ConsecutiveFailures = 0

	if state == StateHalfOpen {
		cb.setState(StateClosed, now)
	}
}

// onFailure 失敗時の処理を行います
func (cb *CircuitBreaker) onFailure(state State, now time.Time) {
	cb.counts.TotalFailures++
	cb.counts.ConsecutiveFailures++
	cb.counts.ConsecutiveSuccesses = 0

	switch state {
	case StateClosed:
		if cb.readyToTrip(cb.counts) {
			cb.setState(StateOpen, now)
		}
	case StateHalfOpen:
		cb.setState(StateOpen, now)
	}
}

// currentState 現在の状態を返します
func (cb *CircuitBreaker) currentState(now time.Time) (State, uint64) {
	switch cb.state {
	case StateClosed:
		if !cb.expiry.IsZero() && cb.expiry.Before(now) {
			cb.toNewGeneration(now)
		}
	case StateOpen:
		if cb.expiry.Before(now) {
			cb.setState(StateHalfOpen, now)
		}
	}
	return cb.state, cb.generation
}

// setState 状態を変更します
func (cb *CircuitBreaker) setState(state State, now time.Time) {
	if cb.state == state {
		return
	}

	prev := cb.state
	cb.state = state

	cb.toNewGeneration(now)

	if cb.onStateChange != nil {
		cb.onStateChange(cb.name, prev, state)
	}
}

// toNewGeneration 新しい世代に移行します
func (cb *CircuitBreaker) toNewGeneration(now time.Time) {
	cb.generation++
	cb.counts = Counts{}

	var zero time.Time
	switch cb.state {
	case StateClosed:
		if cb.interval == 0 {
			cb.expiry = zero
		} else {
			cb.expiry = now.Add(cb.interval)
		}
	case StateOpen:
		cb.expiry = now.Add(cb.timeout)
	default: // StateHalfOpen
		cb.expiry = zero
	}
}

// Reset サーキットブレーカーをリセットします
func (cb *CircuitBreaker) Reset() {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	cb.toNewGeneration(time.Now())
	cb.state = StateClosed
}

// HTTPClientWrapper HTTPクライアント用のラッパーです
type HTTPClientWrapper struct {
	client         *http.Client
	circuitBreaker *CircuitBreaker
}

// NewHTTPClientWrapper 新しいHTTPクライアントラッパーを作成します
func NewHTTPClientWrapper(client *http.Client, cb *CircuitBreaker) *HTTPClientWrapper {
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	return &HTTPClientWrapper{
		client:         client,
		circuitBreaker: cb,
	}
}

// Get サーキットブレーカー付きのGETリクエストを実行します
func (w *HTTPClientWrapper) Get(url string) (*http.Response, error) {
	result, err := w.circuitBreaker.Execute(func() (interface{}, error) {
		return w.client.Get(url)
	})

	if err != nil {
		return nil, err
	}

	return result.(*http.Response), nil
}

// Post サーキットブレーカー付きのPOSTリクエストを実行します
func (w *HTTPClientWrapper) Post(url, contentType string, body interface{}) (*http.Response, error) {
	result, err := w.circuitBreaker.Execute(func() (interface{}, error) {
		// bodyをio.Readerに変換する処理は簡略化
		return w.client.Post(url, contentType, nil)
	})

	if err != nil {
		return nil, err
	}

	return result.(*http.Response), nil
}

// 使用例とテスト用のmain関数
func main() {
	// サーキットブレーカー設定
	settings := Settings{
		Name:                "api-circuit-breaker",
		MaxRequests:         5,
		Interval:            10 * time.Second,
		Timeout:             30 * time.Second,
		SlowCallThreshold:   2 * time.Second,
		FailureThreshold:    0.5,
		MinRequestThreshold: 3,
		ReadyToTrip: func(counts Counts) bool {
			failureRate := float64(counts.TotalFailures) / float64(counts.Requests)
			return counts.Requests >= 3 &&
				(counts.ConsecutiveFailures >= 3 || failureRate >= 0.5)
		},
		OnStateChange: func(name string, from State, to State) {
			log.Printf("Circuit Breaker [%s]: %s -> %s", name, from, to)
		},
		IsSuccessful: func(err error) bool {
			return err == nil
		},
		Fallback: func(err error) (interface{}, error) {
			log.Printf("Using fallback due to: %v", err)
			return "fallback response", nil
		},
	}

	cb := NewCircuitBreaker(settings)

	// HTTPクライアントラッパーの使用例
	httpWrapper := NewHTTPClientWrapper(nil, cb)

	// シミュレーション用のテストリクエスト
	testURLs := []string{
		"http://httpbin.org/status/200",
		"http://httpbin.org/status/500",
		"http://httpbin.org/delay/3", // スロークエリ
		"http://invalid-url-that-will-fail",
	}

	log.Println("=== サーキットブレーカーテスト開始 ===")

	for i := 0; i < 20; i++ {
		url := testURLs[i%len(testURLs)]

		log.Printf("Request %d: %s", i+1, url)

		start := time.Now()
		resp, err := httpWrapper.Get(url)
		duration := time.Since(start)

		if err != nil {
			log.Printf("  Error: %v (Duration: %v)", err, duration)
		} else {
			log.Printf("  Success: %d (Duration: %v)", resp.StatusCode, duration)
			if err := resp.Body.Close(); err != nil {
				log.Printf("Failed to close response body: %v", err)
			}
		}

		// 現在の状態と統計を表示
		state := cb.State()
		counts := cb.Counts()
		log.Printf("  State: %s, Success: %d, Failure: %d, Consecutive Failures: %d",
			state, counts.TotalSuccesses, counts.TotalFailures, counts.ConsecutiveFailures)

		time.Sleep(1 * time.Second)
		log.Println()
	}

	log.Println("=== テスト完了 ===")

	// 最終統計
	finalCounts := cb.Counts()
	log.Printf("Final Statistics:")
	log.Printf("  Total Requests: %d", finalCounts.Requests)
	log.Printf("  Total Successes: %d", finalCounts.TotalSuccesses)
	log.Printf("  Total Failures: %d", finalCounts.TotalFailures)
	log.Printf("  Success Rate: %.2f%%",
		float64(finalCounts.TotalSuccesses)/float64(finalCounts.Requests)*100)
	log.Printf("  Average Response Time: %.2fms",
		float64(finalCounts.TotalDuration.Nanoseconds())/float64(finalCounts.Requests)/1000000)
}
