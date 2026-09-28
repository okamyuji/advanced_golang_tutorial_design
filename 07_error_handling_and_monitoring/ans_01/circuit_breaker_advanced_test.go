package main

import (
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

var errTest = errors.New("test failure")

func testConfig() AdvancedCircuitBreakerConfig {
	return AdvancedCircuitBreakerConfig{
		Name:                "test",
		FailureThreshold:    3,
		MinimumRequestCount: 3,
		OpenTimeout:         time.Second,
		SlidingWindowSize:   10,
	}
}

func failN(t *testing.T, acb *AdvancedCircuitBreaker, n int) {
	t.Helper()
	for range n {
		if err := acb.Execute(func() error { return errTest }); !errors.Is(err, errTest) {
			t.Fatalf("Expected the request error, got %v", err)
		}
	}
}

func TestAdvancedCircuitBreaker_OpensAndRecovers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		acb := NewAdvancedCircuitBreaker(testConfig())

		// 連続失敗でオープンになり、以降は呼び出しを通さない
		failN(t, acb, 3)
		if got := acb.GetState(); got != Open {
			t.Fatalf("Expected OPEN after 3 failures, got %s", got)
		}
		called := false
		if err := acb.Execute(func() error { called = true; return nil }); !errors.Is(err, ErrCircuitOpen) {
			t.Errorf("Expected ErrCircuitOpen, got %v", err)
		}
		if called {
			t.Error("Expected request not to run while OPEN")
		}

		// OpenTimeout が過ぎるとハーフオープンになり、成功すればクローズに戻る
		time.Sleep(time.Second + time.Millisecond)
		if got := acb.GetState(); got != HalfOpen {
			t.Fatalf("Expected HALF_OPEN after open timeout, got %s", got)
		}
		if err := acb.Execute(func() error { return nil }); err != nil {
			t.Fatalf("Expected success in HALF_OPEN, got %v", err)
		}
		if got := acb.GetState(); got != Closed {
			t.Fatalf("Expected CLOSED after success, got %s", got)
		}

		metrics := acb.GetMetrics()
		if metrics.CircuitOpenCount != 1 || metrics.TotalRequests != 4 || metrics.FailedRequests != 3 {
			t.Errorf("Unexpected metrics: %+v", metrics)
		}
		synctest.Wait() // OnStateChange などの goroutine を待つ
	})
}

func TestAdvancedCircuitBreaker_HalfOpenAllowsOneRequest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		acb := NewAdvancedCircuitBreaker(testConfig())
		failN(t, acb, 3)
		time.Sleep(time.Second + time.Millisecond)

		release := make(chan struct{})
		var wg sync.WaitGroup
		wg.Go(func() {
			_ = acb.Execute(func() error { <-release; return nil })
		})
		synctest.Wait()

		// 試行中の1件が終わるまで、ハーフオープンでは次の呼び出しを断る
		if err := acb.Execute(func() error { return nil }); !errors.Is(err, ErrTooManyRequests) {
			t.Errorf("Expected ErrTooManyRequests in HALF_OPEN, got %v", err)
		}

		close(release)
		wg.Wait()
		if got := acb.GetState(); got != Closed {
			t.Errorf("Expected CLOSED after the trial request succeeded, got %s", got)
		}
	})
}

func TestAdvancedCircuitBreaker_ConcurrentGetState(t *testing.T) {
	config := testConfig()
	config.OpenTimeout = time.Nanosecond // すぐにハーフオープンへ遷移できる状態にする
	for range 50 {
		acb := NewAdvancedCircuitBreaker(config)
		failN(t, acb, 3)

		// GetState は遷移を伴うことがあるので、並行に呼んでも状態の書き込みが競合しない
		var wg sync.WaitGroup
		for range 4 {
			wg.Go(func() { _ = acb.GetState() })
		}
		wg.Wait()

		if got := acb.GetState(); got != HalfOpen {
			t.Fatalf("Expected HALF_OPEN, got %s", got)
		}
	}
}

func TestAdvancedCircuitBreaker_ResetKeepsInFlightCount(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		acb := NewAdvancedCircuitBreaker(testConfig())

		release := make(chan struct{})
		var wg sync.WaitGroup
		wg.Go(func() {
			_ = acb.Execute(func() error { <-release; return nil })
		})
		synctest.Wait()

		// 実行中のリクエストがある間に Reset しても、並行数の数え方は狂わない
		acb.Reset()
		close(release)
		wg.Wait()

		if got := acb.activeRequests.Load(); got != 0 {
			t.Errorf("Expected 0 active requests after the request finished, got %d", got)
		}
	})
}
