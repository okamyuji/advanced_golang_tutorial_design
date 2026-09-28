package main

import (
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

// SafeCounter Mutexで保護したカウンターです
type SafeCounter struct {
	mu    sync.Mutex
	value int64
}

// Increment カウンターを安全にインクリメントします
func (sc *SafeCounter) Increment() {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.value++
}

// Value 現在の値を取得します
func (sc *SafeCounter) Value() int64 {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.value
}

// AtomicCounter 型付きatomicを使うカウンターです
type AtomicCounter struct {
	value atomic.Int64
}

// Increment カウンターを原子的にインクリメントします
func (ac *AtomicCounter) Increment() {
	ac.value.Add(1)
}

// Value 現在の値を取得します
func (ac *AtomicCounter) Value() int64 {
	return ac.value.Load()
}

// UnsafeCounter 同期しないカウンターです（比較用）
type UnsafeCounter struct {
	value int64
}

// Increment 同期せずにインクリメントします
func (uc *UnsafeCounter) Increment() {
	uc.value++ // データレースになる
}

// Value 現在の値を取得します
func (uc *UnsafeCounter) Value() int64 {
	return uc.value
}

type counter interface {
	Increment()
	Value() int64
}

// runIncrements goroutinesごとにincrements回インクリメントし、合計を返します
func runIncrements(c counter, goroutines, increments int) int64 {
	var wg sync.WaitGroup
	for range goroutines {
		wg.Go(func() {
			for range increments {
				c.Increment()
				// 仮想時計の上で他のgoroutineと交互に進める
				time.Sleep(time.Millisecond)
			}
		})
	}
	wg.Wait()
	return c.Value()
}

// TestCounters_WithSynctest Mutex版とatomic版が並行更新で値を失わないことを確かめます
func TestCounters_WithSynctest(t *testing.T) {
	tests := []struct {
		name    string
		counter counter
	}{
		{"mutex", &SafeCounter{}},
		{"atomic", &AtomicCounter{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				const goroutines, increments = 100, 10
				start := time.Now()

				got := runIncrements(tt.counter, goroutines, increments)

				if want := int64(goroutines * increments); got != want {
					t.Errorf("expected %d, got %d", want, got)
				}
				// 1msのSleepを10回重ねても、仮想時計はちょうど10ms進むだけで実時間は待たない
				if elapsed := time.Since(start); elapsed != 10*time.Millisecond {
					t.Errorf("expected virtual elapsed 10ms, got %v", elapsed)
				}
			})
		})
	}
}

// TestUnsafeCounter_DetectRaceCondition 同期しないカウンターの結果を記録します
// go test -race ではレース検出器がこの競合を報告してテストを失敗させるため、raceビルドではスキップします
func TestUnsafeCounter_DetectRaceCondition(t *testing.T) {
	if raceEnabled {
		t.Skip("UnsafeCounterは意図的なデータレースを含むため、-raceでは実行しない")
	}
	synctest.Test(t, func(t *testing.T) {
		const goroutines, increments = 50, 20
		got := runIncrements(&UnsafeCounter{}, goroutines, increments)
		want := int64(goroutines * increments)
		t.Logf("unsafe counter: expected %d, got %d (一致しても正しさは保証されない)", want, got)
	})
}

// TestConcurrentReadWrite 読み取りと書き込みを同時に行っても最終値が一致することを確かめます
func TestConcurrentReadWrite(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		safeCounter := &SafeCounter{}
		atomicCounter := &AtomicCounter{}
		const readers, writers, ops = 10, 10, 5

		var wg sync.WaitGroup
		for range writers {
			wg.Go(func() {
				for range ops {
					safeCounter.Increment()
					atomicCounter.Increment()
					time.Sleep(time.Millisecond)
				}
			})
		}
		for id := range readers {
			wg.Go(func() {
				for range ops {
					if s, a := safeCounter.Value(), atomicCounter.Value(); s < 0 || a < 0 {
						t.Errorf("reader %d: negative value: safe=%d atomic=%d", id, s, a)
					}
					time.Sleep(time.Millisecond)
				}
			})
		}
		wg.Wait()

		want := int64(writers * ops)
		if got := safeCounter.Value(); got != want {
			t.Errorf("safe counter: expected %d, got %d", want, got)
		}
		if got := atomicCounter.Value(); got != want {
			t.Errorf("atomic counter: expected %d, got %d", want, got)
		}
	})
}

func BenchmarkSafeCounter(b *testing.B) {
	counter := &SafeCounter{}
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			counter.Increment()
		}
	})
}

func BenchmarkAtomicCounter(b *testing.B) {
	counter := &AtomicCounter{}
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			counter.Increment()
		}
	})
}
