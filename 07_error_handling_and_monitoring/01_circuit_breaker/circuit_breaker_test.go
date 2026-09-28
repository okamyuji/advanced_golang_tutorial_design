package main

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestCircuitBreaker_InitialState(t *testing.T) {
	cb := NewCircuitBreaker(Settings{
		Name:     "test",
		Interval: time.Minute,
		Timeout:  30 * time.Second,
	})

	if cb.State() != StateClosed {
		t.Errorf("Expected initial state to be CLOSED, got %s", cb.State())
	}

	counts := cb.Counts()
	if counts.Requests != 0 {
		t.Errorf("Expected initial requests to be 0, got %d", counts.Requests)
	}
}

func TestCircuitBreaker_SuccessfulRequests(t *testing.T) {
	cb := NewCircuitBreaker(Settings{
		Name:     "test-success",
		Interval: time.Minute,
		Timeout:  30 * time.Second,
	})

	// 成功リクエストを実行
	for range 5 {
		result, err := cb.Execute(func() (any, error) {
			return "success", nil
		})

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		if result != "success" {
			t.Errorf("Expected 'success', got %v", result)
		}
	}

	// 状態確認
	if cb.State() != StateClosed {
		t.Errorf("Expected state to remain CLOSED, got %s", cb.State())
	}

	counts := cb.Counts()
	if counts.Requests != 5 {
		t.Errorf("Expected 5 requests, got %d", counts.Requests)
	}
	if counts.TotalSuccesses != 5 {
		t.Errorf("Expected 5 successes, got %d", counts.TotalSuccesses)
	}
	if counts.TotalFailures != 0 {
		t.Errorf("Expected 0 failures, got %d", counts.TotalFailures)
	}
}

func TestCircuitBreaker_FailureTrip(t *testing.T) {
	cb := NewCircuitBreaker(Settings{
		Name:     "test-failure",
		Interval: time.Minute,
		Timeout:  30 * time.Second,
		ReadyToTrip: func(counts Counts) bool {
			return counts.ConsecutiveFailures >= 3
		},
	})

	testError := errors.New("test error")

	// 3回連続で失敗させる
	for range 3 {
		_, err := cb.Execute(func() (any, error) {
			return nil, testError
		})

		if !errors.Is(err, testError) {
			t.Errorf("Expected test error, got %v", err)
		}
	}

	// 4回目のリクエストでサーキットブレーカーがオープンになることを確認
	_, err := cb.Execute(func() (any, error) {
		return "should not execute", nil
	})

	if !errors.Is(err, ErrCircuitBreakerOpen) {
		t.Errorf("Expected circuit breaker open error, got %v", err)
	}

	if cb.State() != StateOpen {
		t.Errorf("Expected state to be OPEN, got %s", cb.State())
	}
}

func TestCircuitBreaker_HalfOpenRecovery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cb := NewCircuitBreaker(Settings{
			Name:     "test-recovery",
			Interval: time.Minute,
			Timeout:  100 * time.Millisecond, // 短いタイムアウト
			ReadyToTrip: func(counts Counts) bool {
				return counts.ConsecutiveFailures >= 2
			},
		})

		testError := errors.New("test error")

		// サーキットブレーカーをオープン状態にする
		for range 2 {
			if _, err := cb.Execute(func() (any, error) {
				return nil, testError
			}); err == nil {
				t.Errorf("Expected error but got none")
			}
		}

		// オープン状態確認
		if cb.State() != StateOpen {
			t.Errorf("Expected state to be OPEN, got %s", cb.State())
		}

		// タイムアウト待機
		time.Sleep(150 * time.Millisecond)
		if cb.State() != StateHalfOpen {
			t.Errorf("Expected state to be HALF_OPEN after timeout, got %s", cb.State())
		}

		// ハーフオープン状態での成功リクエスト
		result, err := cb.Execute(func() (any, error) {
			return "recovery success", nil
		})

		if err != nil {
			t.Errorf("Expected no error during recovery, got %v", err)
		}

		if result != "recovery success" {
			t.Errorf("Expected 'recovery success', got %v", result)
		}

		// クローズ状態に戻ることを確認
		if cb.State() != StateClosed {
			t.Errorf("Expected state to be CLOSED after recovery, got %s", cb.State())
		}
	})
}

func TestCircuitBreaker_MaxRequestsInHalfOpen(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cb := NewCircuitBreaker(Settings{
			Name:        "test-max-requests",
			MaxRequests: 2,
			Interval:    time.Minute,
			Timeout:     100 * time.Millisecond,
			ReadyToTrip: func(counts Counts) bool {
				return counts.ConsecutiveFailures >= 1
			},
		})

		// オープン状態にする
		if _, err := cb.Execute(func() (any, error) {
			return nil, errors.New("test error")
		}); err == nil {
			t.Errorf("Expected error but got none")
		}

		// タイムアウト待機してハーフオープンにする
		time.Sleep(150 * time.Millisecond)

		// ハーフオープン状態で最大リクエスト数を超えるテスト
		var wg sync.WaitGroup
		var tooManyRequestsCount atomic.Int64

		for range 5 {
			wg.Go(func() {
				_, err := cb.Execute(func() (any, error) {
					time.Sleep(50 * time.Millisecond)
					return "success", nil
				})

				if errors.Is(err, ErrTooManyRequests) {
					tooManyRequestsCount.Add(1)
				}
			})
		}

		wg.Wait()

		// 全 goroutine が Sleep に入るまで偽の時計は進まないので、MaxRequests=2 を超えた 3 件が必ず断られる
		if got := tooManyRequestsCount.Load(); got != 3 {
			t.Errorf("Expected 3 requests to be rejected with ErrTooManyRequests, got %d", got)
		}
	})
}

func TestCircuitBreaker_SlowCallsAreFailures(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cb := NewCircuitBreaker(Settings{
			Name:              "test-slow-calls",
			Interval:          time.Minute,
			Timeout:           30 * time.Second,
			SlowCallThreshold: 100 * time.Millisecond,
			ReadyToTrip: func(counts Counts) bool {
				return counts.ConsecutiveFailures >= 2
			},
		})

		// スローコールを実行
		for range 2 {
			_, err := cb.Execute(func() (any, error) {
				time.Sleep(200 * time.Millisecond) // 閾値を超える
				return "slow success", nil
			})

			if err != nil {
				t.Errorf("Expected no error for slow call, got %v", err)
			}
		}

		// サーキットブレーカーがオープンになることを確認
		// (スローコールが失敗として扱われたため)
		if cb.State() != StateOpen {
			t.Errorf("Expected state to be OPEN due to slow calls, got %s", cb.State())
		}

		// オープン状態では新しい世代に移行するためカウンターがリセットされる
		// これは正常な動作
	})
}

func TestCircuitBreaker_FallbackFunction(t *testing.T) {
	fallbackCalled := false
	cb := NewCircuitBreaker(Settings{
		Name:     "test-fallback",
		Interval: time.Minute,
		Timeout:  30 * time.Second,
		ReadyToTrip: func(counts Counts) bool {
			return counts.ConsecutiveFailures >= 1
		},
		Fallback: func(err error) (any, error) {
			fallbackCalled = true
			return "fallback result", nil
		},
	})

	// サーキットブレーカーをオープンにする
	if _, err := cb.Execute(func() (any, error) {
		return nil, errors.New("test error")
	}); err == nil {
		t.Errorf("Expected error but got none")
	}

	// オープン状態でのリクエスト（フォールバックが呼ばれるはず）
	result, err := cb.Execute(func() (any, error) {
		return "should not execute", nil
	})

	if err != nil {
		t.Errorf("Expected no error with fallback, got %v", err)
	}

	if result != "fallback result" {
		t.Errorf("Expected fallback result, got %v", result)
	}

	if !fallbackCalled {
		t.Error("Expected fallback function to be called")
	}
}

func TestCircuitBreaker_StateChangeCallback(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var stateChanges []string
		cb := NewCircuitBreaker(Settings{
			Name:     "test-callback",
			Interval: time.Minute,
			Timeout:  100 * time.Millisecond,
			ReadyToTrip: func(counts Counts) bool {
				return counts.ConsecutiveFailures >= 2
			},
			OnStateChange: func(name string, from State, to State) {
				stateChanges = append(stateChanges, fmt.Sprintf("%s->%s", from, to))
			},
		})

		// CLOSED -> OPEN
		for range 2 {
			if _, err := cb.Execute(func() (any, error) {
				return nil, errors.New("test error")
			}); err == nil {
				t.Errorf("Expected error but got none")
			}
		}

		// OPEN -> HALF_OPEN
		time.Sleep(150 * time.Millisecond)
		if _, err := cb.Execute(func() (any, error) {
			return "success", nil
		}); err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		expectedChanges := []string{"CLOSED->OPEN", "OPEN->HALF_OPEN", "HALF_OPEN->CLOSED"}
		if len(stateChanges) != len(expectedChanges) {
			t.Errorf("Expected %d state changes, got %d: %v", len(expectedChanges), len(stateChanges), stateChanges)
		}

		for i, expected := range expectedChanges {
			if i < len(stateChanges) && stateChanges[i] != expected {
				t.Errorf("Expected state change %s, got %s", expected, stateChanges[i])
			}
		}
	})
}

func TestCircuitBreaker_Reset(t *testing.T) {
	cb := NewCircuitBreaker(Settings{
		Name:     "test-reset",
		Interval: time.Minute,
		Timeout:  30 * time.Second,
		ReadyToTrip: func(counts Counts) bool {
			return counts.ConsecutiveFailures >= 1
		},
	})

	// サーキットブレーカーをオープンにする
	if _, err := cb.Execute(func() (any, error) {
		return nil, errors.New("test error")
	}); err == nil {
		t.Errorf("Expected error but got none")
	}

	if cb.State() != StateOpen {
		t.Errorf("Expected state to be OPEN, got %s", cb.State())
	}

	// リセット
	cb.Reset()

	if cb.State() != StateClosed {
		t.Errorf("Expected state to be CLOSED after reset, got %s", cb.State())
	}

	// カウンターもリセットされることを確認
	counts := cb.Counts()
	if counts.Requests != 0 || counts.TotalFailures != 0 {
		t.Errorf("Expected counts to be reset, got %+v", counts)
	}
}

func TestCircuitBreaker_ConcurrentRequests(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cb := NewCircuitBreaker(Settings{
			Name:     "test-concurrent",
			Interval: time.Minute,
			Timeout:  30 * time.Second,
			ReadyToTrip: func(counts Counts) bool {
				// 並行テストでサーキットブレーカーがオープンになりにくくする
				return counts.ConsecutiveFailures >= 50
			},
		})

		const numGoroutines = 100
		var wg sync.WaitGroup
		var successCount, failureCount atomic.Int64

		for id := range numGoroutines {
			wg.Go(func() {
				_, err := cb.Execute(func() (any, error) {
					if id%10 == 0 {
						return nil, errors.New("simulated failure")
					}
					time.Sleep(1 * time.Millisecond)
					return "success", nil
				})

				if err != nil {
					failureCount.Add(1)
				} else {
					successCount.Add(1)
				}
			})
		}

		wg.Wait()

		if got := successCount.Load() + failureCount.Load(); got != numGoroutines {
			t.Errorf("Expected %d total requests, got %d", numGoroutines, got)
		}

		if got := successCount.Load(); got != 90 {
			t.Errorf("Expected 90 successful requests, got %d", got)
		}

		if got := failureCount.Load(); got != 10 {
			t.Errorf("Expected 10 failed requests, got %d", got)
		}

		counts := cb.Counts()
		if counts.Requests != uint64(numGoroutines) {
			t.Errorf("Expected %d requests in counts, got %d", numGoroutines, counts.Requests)
		}
	})
}

// ベンチマークテスト
func BenchmarkCircuitBreaker_Execute(b *testing.B) {
	cb := NewCircuitBreaker(Settings{
		Name:     "benchmark",
		Interval: time.Minute,
		Timeout:  30 * time.Second,
	})

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := cb.Execute(func() (any, error) {
				return "success", nil
			}); err != nil {
				b.Errorf("Unexpected error: %v", err)
			}
		}
	})
}

func BenchmarkCircuitBreaker_ExecuteWithFailures(b *testing.B) {
	cb := NewCircuitBreaker(Settings{
		Name:     "benchmark-failures",
		Interval: time.Minute,
		Timeout:  30 * time.Second,
		ReadyToTrip: func(counts Counts) bool {
			return counts.ConsecutiveFailures >= 1000 // 高い閾値
		},
	})

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if _, err := cb.Execute(func() (any, error) {
				i++
				if i%10 == 0 {
					return nil, errors.New("simulated failure")
				}
				return "success", nil
			}); err != nil {
				// Expected occasional errors in benchmark
				continue
			}
		}
	})
}
