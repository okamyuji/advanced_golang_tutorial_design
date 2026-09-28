package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

// stubService 失敗の有無と同時実行数をテストから制御できる外部サービスです
type stubService struct {
	fail      atomic.Bool
	block     chan struct{} // nil でなければ、閉じられるまで Call を止める
	active    atomic.Int64
	maxActive atomic.Int64
}

func (s *stubService) Name() string { return "stub" }

func (s *stubService) Call(ctx context.Context, request any) (any, error) {
	n := s.active.Add(1)
	defer s.active.Add(-1)
	for {
		m := s.maxActive.Load()
		if n <= m || s.maxActive.CompareAndSwap(m, n) {
			break
		}
	}

	if s.block != nil {
		select {
		case <-s.block:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if s.fail.Load() {
		return nil, errors.New("stub failure")
	}
	return fmt.Sprintf("ok-%v", request), nil
}

func testConfig() FallbackManagerConfig {
	return FallbackManagerConfig{
		Name:                 "test",
		ErrorRateThreshold:   0.5,
		RecoveryThreshold:    0.1,
		MinimumRequestCount:  5,
		SlidingWindowSize:    10,
		EvaluationInterval:   time.Second,
		TransitionDuration:   2 * time.Second,
		MaxConcurrentWorkers: 10,
	}
}

func executeN(t *testing.T, fm *FallbackManager, from, n int) {
	t.Helper()
	for i := range n {
		// 別々のキーにしてキャッシュに当たらないようにする
		if _, err := fm.Execute(t.Context(), from+i); err != nil {
			t.Fatalf("Execute(%d) failed: %v", from+i, err)
		}
	}
}

func TestFallbackManager_ModeCycle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		svc := &stubService{}
		fm := NewFallbackManager(testConfig(), svc, NewMockFallbackProvider())
		if err := fm.Start(); err != nil {
			t.Fatalf("Start failed: %v", err)
		}
		defer func() {
			if err := fm.Stop(); err != nil {
				t.Errorf("Stop failed: %v", err)
			}
		}()

		// 外部サービスが失敗し続けると、フォールバックモードへ切り替わる
		svc.fail.Store(true)
		executeN(t, fm, 0, 10)
		synctest.Sleep(time.Second)
		if got := fm.GetCurrentMode(); got != Fallback {
			t.Fatalf("Expected FALLBACK after failures, got %s", got)
		}

		// フォールバックで成功が続くと、エラー率が下がって移行モードへ進む
		svc.fail.Store(false)
		executeN(t, fm, 100, 10)
		synctest.Sleep(time.Second)
		if got := fm.GetCurrentMode(); got != TransitionMode {
			t.Fatalf("Expected TRANSITION after recovery, got %s", got)
		}

		// 移行期間が過ぎても低いエラー率のままなら、通常モードへ戻る
		synctest.Sleep(2 * time.Second)
		if got := fm.GetCurrentMode(); got != NormalMode {
			t.Fatalf("Expected NORMAL after transition period, got %s", got)
		}

		metrics := fm.GetMetrics()
		if metrics.ModeChangeCount != 3 {
			t.Errorf("Expected 3 mode changes, got %d", metrics.ModeChangeCount)
		}
		if metrics.FallbackActivations != 1 {
			t.Errorf("Expected 1 fallback activation, got %d", metrics.FallbackActivations)
		}
		if metrics.TotalRequests != 20 {
			t.Errorf("Expected 20 requests, got %d", metrics.TotalRequests)
		}
	})
}

func TestFallbackManager_MetricsDuringExecute(t *testing.T) {
	fm := NewFallbackManager(testConfig(), &stubService{}, NewMockFallbackProvider())

	// Execute のカウンター更新と GetMetrics のコピーが同時に走っても競合しない
	var wg sync.WaitGroup
	for w := range 4 {
		wg.Go(func() {
			for i := range 100 {
				if _, err := fm.Execute(t.Context(), w*1000+i); err != nil {
					t.Errorf("Execute failed: %v", err)
					return
				}
			}
		})
	}
	wg.Go(func() {
		for range 100 {
			_ = fm.GetMetrics()
		}
	})
	wg.Wait()

	if got := fm.GetMetrics().TotalRequests; got != 400 {
		t.Errorf("Expected 400 requests, got %d", got)
	}
}

func TestFallbackManager_MaxConcurrentWorkers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const maxWorkers, callers = 2, 20
		config := testConfig()
		config.MaxConcurrentWorkers = maxWorkers
		svc := &stubService{block: make(chan struct{})}
		fm := NewFallbackManager(config, svc, NewMockFallbackProvider())

		var rejected atomic.Int64
		var wg sync.WaitGroup
		for i := range callers {
			wg.Go(func() {
				if _, err := fm.Execute(t.Context(), i); errors.Is(err, ErrTooManyWorkers) {
					rejected.Add(1)
				}
			})
		}

		// 受け付けた呼び出しがすべて外部サービスで止まるまで進めてから解放する
		synctest.Wait()
		close(svc.block)
		wg.Wait()

		if got := svc.maxActive.Load(); got > maxWorkers {
			t.Errorf("Expected at most %d concurrent calls, got %d", maxWorkers, got)
		}
		if got := rejected.Load(); got != callers-maxWorkers {
			t.Errorf("Expected %d rejected calls, got %d", callers-maxWorkers, got)
		}
	})
}

func TestFallbackManager_ExecuteDuringStop(t *testing.T) {
	for range 50 {
		fm := NewFallbackManager(testConfig(), &stubService{}, NewMockFallbackProvider())
		if err := fm.Start(); err != nil {
			t.Fatalf("Start failed: %v", err)
		}

		var wg sync.WaitGroup
		wg.Go(func() {
			for i := range 20 {
				_, _ = fm.Execute(t.Context(), i)
			}
		})
		wg.Go(func() {
			if err := fm.Stop(); err != nil {
				t.Errorf("Stop failed: %v", err)
			}
		})
		wg.Wait()

		if _, err := fm.Execute(t.Context(), "late"); !errors.Is(err, ErrStopped) {
			t.Fatalf("Expected ErrStopped from Execute after stop, got %v", err)
		}
		if err := fm.Start(); !errors.Is(err, ErrStopped) {
			t.Fatalf("Expected ErrStopped from Start after stop, got %v", err)
		}
	}
}

func TestFallbackManager_ClearCache(t *testing.T) {
	fm := NewFallbackManager(testConfig(), &stubService{}, NewMockFallbackProvider())
	if _, err := fm.Execute(t.Context(), "key"); err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if _, ok := fm.getFromCache("key"); !ok {
		t.Fatal("Expected a successful call to be cached")
	}

	fm.ClearCache()

	if _, ok := fm.getFromCache("key"); ok {
		t.Error("Expected cache to be empty after ClearCache")
	}
}
