package main

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

// SafeCounter は安全なカウンターです
type SafeCounter struct {
	mu    sync.Mutex
	value int64
}

// Increment はカウンターを安全にインクリメントします
func (sc *SafeCounter) Increment() {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.value++
}

// Value は現在の値を取得します
func (sc *SafeCounter) Value() int64 {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.value
}

// AtomicCounter は原子操作を使用するカウンターです
type AtomicCounter struct {
	value int64
}

// Increment はカウンターを原子的にインクリメントします
func (ac *AtomicCounter) Increment() {
	atomic.AddInt64(&ac.value, 1)
}

// Value は現在の値を取得します
func (ac *AtomicCounter) Value() int64 {
	return atomic.LoadInt64(&ac.value)
}

// UnsafeCounter は安全でないカウンターです（比較用）
type UnsafeCounter struct {
	value int64
}

// Increment はカウンターを安全でないインクリメントします
func (uc *UnsafeCounter) Increment() {
	uc.value++ // レースコンディションが発生する可能性
}

// Value は現在の値を取得します
func (uc *UnsafeCounter) Value() int64 {
	return uc.value
}

// TestSafeCounter_WithSynctest はMutexを使った安全なカウンターをテストします
func TestSafeCounter_WithSynctest(t *testing.T) {
	synctest.Run(func() {
		counter := &SafeCounter{}
		const numGoroutines = 100
		const incrementsPerGoroutine = 10

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var wg sync.WaitGroup

		// 複数のGoroutineでカウンターを並行更新
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(goroutineID int) {
				defer wg.Done()

				for j := 0; j < incrementsPerGoroutine; j++ {
					select {
					case <-ctx.Done():
						return
					default:
						counter.Increment()

						// 他のGoroutineに処理を譲る機会を与える
						time.Sleep(1 * time.Millisecond)
					}
				}
			}(i)
		}

		// すべてのGoroutineの完了を待機
		wg.Wait()

		// 期待値と実際の値を比較
		expected := int64(numGoroutines * incrementsPerGoroutine)
		actual := counter.Value()

		if actual != expected {
			t.Errorf("Safe counter test failed: expected %d, got %d", expected, actual)
		}

		t.Logf("Safe counter test passed: %d increments completed correctly", actual)
	})
}

// TestAtomicCounter_WithSynctest は原子操作を使ったカウンターをテストします
func TestAtomicCounter_WithSynctest(t *testing.T) {
	synctest.Run(func() {
		counter := &AtomicCounter{}
		const numGoroutines = 100
		const incrementsPerGoroutine = 10

		var wg sync.WaitGroup

		// 複数のGoroutineでカウンターを並行更新
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(goroutineID int) {
				defer wg.Done()

				for j := 0; j < incrementsPerGoroutine; j++ {
					counter.Increment()

					// 他のGoroutineに処理を譲る機会を与える
					time.Sleep(1 * time.Millisecond)
				}
			}(i)
		}

		// すべてのGoroutineの完了を待機
		wg.Wait()

		// 期待値と実際の値を比較
		expected := int64(numGoroutines * incrementsPerGoroutine)
		actual := counter.Value()

		if actual != expected {
			t.Errorf("Atomic counter test failed: expected %d, got %d", expected, actual)
		}

		t.Logf("Atomic counter test passed: %d increments completed correctly", actual)
	})
}

// TestUnsafeCounter_DetectRaceCondition は安全でないカウンターでレースコンディションを検出します
func TestUnsafeCounter_DetectRaceCondition(t *testing.T) {
	// このテストは通常の環境では間欠的に失敗する可能性があります
	// synctestでは決定的な結果が得られます

	synctest.Run(func() {
		counter := &UnsafeCounter{}
		const numGoroutines = 50
		const incrementsPerGoroutine = 20

		var wg sync.WaitGroup

		// 複数のGoroutineでカウンターを並行更新
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(goroutineID int) {
				defer wg.Done()

				for j := 0; j < incrementsPerGoroutine; j++ {
					counter.Increment() // レースコンディションが発生する可能性

					// 他のGoroutineに処理を譲る機会を与える
					time.Sleep(1 * time.Millisecond)
				}
			}(i)
		}

		// すべてのGoroutineの完了を待機
		wg.Wait()

		expected := int64(numGoroutines * incrementsPerGoroutine)
		actual := counter.Value()

		// レースコンディションにより期待値と異なる可能性があることを記録
		if actual != expected {
			t.Logf("Race condition detected: expected %d, got %d (difference: %d)",
				expected, actual, expected-actual)
		} else {
			t.Logf("Unsafe counter got lucky: %d increments completed (but this is not guaranteed)", actual)
		}

		// 安全でないカウンターのテストでは失敗を期待しない
		// （レースコンディションは必ず発生するわけではないため）
	})
}

// TestConcurrentCounterOperations は複数の操作を同時に実行するテストです
func TestConcurrentCounterOperations(t *testing.T) {
	synctest.Run(func() {
		safeCounter := &SafeCounter{}
		atomicCounter := &AtomicCounter{}

		const numReaders = 10
		const numWriters = 10
		const operationsPerGoroutine = 5

		var wg sync.WaitGroup
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// 書き込みGoroutineを開始
		for i := 0; i < numWriters; i++ {
			wg.Add(1)
			go func(writerID int) {
				defer wg.Done()

				for j := 0; j < operationsPerGoroutine; j++ {
					select {
					case <-ctx.Done():
						return
					default:
						safeCounter.Increment()
						atomicCounter.Increment()
						time.Sleep(1 * time.Millisecond)
					}
				}
			}(i)
		}

		// 読み込みGoroutineを開始
		for i := 0; i < numReaders; i++ {
			wg.Add(1)
			go func(readerID int) {
				defer wg.Done()

				for j := 0; j < operationsPerGoroutine; j++ {
					select {
					case <-ctx.Done():
						return
					default:
						safeValue := safeCounter.Value()
						atomicValue := atomicCounter.Value()

						// 値の一貫性をチェック
						if safeValue < 0 || atomicValue < 0 {
							t.Errorf("Reader %d: negative value detected: safe=%d, atomic=%d",
								readerID, safeValue, atomicValue)
						}

						time.Sleep(1 * time.Millisecond)
					}
				}
			}(i)
		}

		// すべてのGoroutineの完了を待機
		wg.Wait()

		// 最終的な値をチェック
		expectedWrites := int64(numWriters * operationsPerGoroutine)
		safeValue := safeCounter.Value()
		atomicValue := atomicCounter.Value()

		if safeValue != expectedWrites {
			t.Errorf("Safe counter final value incorrect: expected %d, got %d", expectedWrites, safeValue)
		}

		if atomicValue != expectedWrites {
			t.Errorf("Atomic counter final value incorrect: expected %d, got %d", expectedWrites, atomicValue)
		}

		t.Logf("Concurrent operations test passed: %d writes, %d reads completed",
			expectedWrites, numReaders*operationsPerGoroutine)
	})
}

// ベンチマーク用の実装
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
