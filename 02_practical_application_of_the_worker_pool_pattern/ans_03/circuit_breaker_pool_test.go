package main

import (
	"testing"
	"testing/synctest"
	"time"
)

func TestCircuitBreaker_StateTransitions(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cb := NewCircuitBreaker(3, 30*time.Second)

		// 連続3回の失敗でClosedからOpenへ
		for range 3 {
			if !cb.CanExecute() {
				t.Fatal("closed breaker must allow execution")
			}
			cb.RecordFailure()
		}
		if got := cb.GetState(); got != CircuitOpen {
			t.Fatalf("expected OPEN, got %v", got)
		}
		if cb.CanExecute() {
			t.Fatal("open breaker must reject execution before recovery timeout")
		}

		// 回復時間を過ぎるとHalf-Openで試行を許す（仮想時計なので実時間は待たない）
		time.Sleep(31 * time.Second)
		if !cb.CanExecute() || cb.GetState() != CircuitHalfOpen {
			t.Fatalf("expected HALF-OPEN after recovery timeout, got %v", cb.GetState())
		}

		// Half-Openで規定回数成功するとClosedへ戻る
		for range cb.halfOpenMaxCalls {
			cb.RecordSuccess()
		}
		if got := cb.GetState(); got != CircuitClosed {
			t.Fatalf("expected CLOSED, got %v", got)
		}
	})
}

func TestCircuitBreaker_HalfOpenFailureReopens(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cb := NewCircuitBreaker(1, time.Second)
		cb.RecordFailure()
		time.Sleep(2 * time.Second)
		if !cb.CanExecute() {
			t.Fatal("expected HALF-OPEN to allow a trial call")
		}
		cb.RecordFailure()
		if got := cb.GetState(); got != CircuitOpen {
			t.Fatalf("expected OPEN after half-open failure, got %v", got)
		}
	})
}

func TestHTTPWorkerPool_SubmitDuringShutdownDoesNotPanic(t *testing.T) {
	for range 50 {
		pool := NewHTTPWorkerPool(2)
		if err := pool.Start(); err != nil {
			t.Fatal(err)
		}
		stop := make(chan struct{})
		submitted := make(chan struct{})
		go func() {
			defer close(submitted)
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				_ = pool.SubmitTask(HTTPTask{ID: i, URL: "http://127.0.0.1:1/", Method: "GET"})
			}
		}()
		if err := pool.Shutdown(time.Second); err != nil {
			t.Fatal(err)
		}
		close(stop)
		<-submitted
		if err := pool.SubmitTask(HTTPTask{ID: -1}); err == nil {
			t.Fatal("expected error when submitting after shutdown")
		}
	}
}
