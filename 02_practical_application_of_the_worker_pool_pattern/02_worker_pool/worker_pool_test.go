package main

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestWorkerPool_Start(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pool := NewWorkerPool(2, 5, 10)

		err := pool.Start()
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// ワーカー数を確認
		pool.mu.RLock()
		workerCount := len(pool.workers)
		pool.mu.RUnlock()

		if workerCount != 2 {
			t.Errorf("Expected 2 workers, got %d", workerCount)
		}

		// クリーンアップ
		if err := pool.Shutdown(5 * time.Second); err != nil {
			t.Logf("Failed to shutdown pool: %v", err)
		}
	})
}

func TestWorkerPool_SubmitTask_Success(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pool := NewWorkerPool(1, 2, 5)
		if err := pool.Start(); err != nil {
			t.Fatalf("Failed to start pool: %v", err)
		}
		defer func() {
			if err := pool.Shutdown(5 * time.Second); err != nil {
				t.Logf("Failed to shutdown pool: %v", err)
			}
		}()

		var executed bool
		var mu sync.Mutex

		task := Task{
			ID:   1,
			Data: "test data",
			Execute: func(data any) error {
				mu.Lock()
				executed = true
				mu.Unlock()
				return nil
			},
		}

		err := pool.SubmitTask(task)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		synctest.Wait()

		mu.Lock()
		if !executed {
			t.Error("Task was not executed")
		}
		mu.Unlock()
	})
}

func TestWorkerPool_SubmitTask_QueueFull(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pool := NewWorkerPool(1, 1, 1) // より小さなキューサイズ
		if err := pool.Start(); err != nil {
			t.Fatalf("Failed to start pool: %v", err)
		}
		// release 閉じるまでタスクを実行中のままにする
		release := make(chan struct{})
		defer func() {
			close(release)
			if err := pool.Shutdown(5 * time.Second); err != nil {
				t.Logf("Failed to shutdown pool: %v", err)
			}
			synctest.Wait()
		}()

		blockingTask := Task{
			ID:   1,
			Data: "blocking",
			Execute: func(data any) error {
				<-release // ワーカーをブロック
				return nil
			},
		}

		// ワーカーをブロック
		if err := pool.SubmitTask(blockingTask); err != nil {
			t.Fatalf("Failed to submit blocking task: %v", err)
		}

		// ワーカーがタスクを取り出して実行中になるまで待つ
		synctest.Wait()

		// キューを満杯にする
		fillerTask := Task{
			ID:   2,
			Data: "filler",
			Execute: func(data any) error {
				<-release
				return nil
			},
		}
		if err := pool.SubmitTask(fillerTask); err != nil {
			t.Fatalf("Failed to submit filler task: %v", err)
		}

		// キューが満杯の状態でタスクを送信
		overflowTask := Task{
			ID:   3,
			Data: "overflow",
			Execute: func(data any) error {
				return nil
			},
		}

		err := pool.SubmitTask(overflowTask)
		if err == nil {
			t.Error("Expected error for full queue, got nil")
		}
	})
}

func TestWorkerPool_TaskExecution_WithError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pool := NewWorkerPool(1, 1, 5)
		if err := pool.Start(); err != nil {
			t.Fatalf("Failed to start pool: %v", err)
		}
		defer func() {
			if err := pool.Shutdown(5 * time.Second); err != nil {
				t.Logf("Failed to shutdown pool: %v", err)
			}
		}()

		var executedCount atomic.Int64
		var errorCount atomic.Int64

		// 成功タスク
		successTask := Task{
			ID:   1,
			Data: "success",
			Execute: func(data any) error {
				executedCount.Add(1)
				return nil
			},
		}

		// エラータスク
		errorTask := Task{
			ID:   2,
			Data: "error",
			Execute: func(data any) error {
				executedCount.Add(1)
				errorCount.Add(1)
				return fmt.Errorf("simulated error")
			},
		}

		if err := pool.SubmitTask(successTask); err != nil {
			t.Fatalf("Failed to submit success task: %v", err)
		}
		if err := pool.SubmitTask(errorTask); err != nil {
			t.Fatalf("Failed to submit error task: %v", err)
		}

		synctest.Wait()

		if executedCount.Load() != 2 {
			t.Errorf("Expected 2 executed tasks, got %d", executedCount.Load())
		}

		metrics := pool.GetMetrics()
		if metrics.failedTasks != 1 {
			t.Errorf("Expected 1 failed task, got %d", metrics.failedTasks)
		}
	})
}

func TestWorkerPool_DynamicScaling(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pool := NewWorkerPool(1, 3, 10)

		// スケールアップ閾値を低く設定
		pool.config.ScaleUpThreshold = 0.7

		if err := pool.Start(); err != nil {
			t.Fatalf("Failed to start pool: %v", err)
		}
		defer func() {
			if err := pool.Shutdown(5 * time.Second); err != nil {
				t.Logf("Failed to shutdown pool: %v", err)
			}
			// 停止後も実行中のタスク関数（2秒）は最後まで走るので、終わるまで待つ
			time.Sleep(2 * time.Second)
		}()

		// 初期ワーカー数を確認
		pool.mu.RLock()
		initialWorkers := len(pool.workers)
		pool.mu.RUnlock()

		if initialWorkers != 1 {
			t.Errorf("Expected 1 initial worker, got %d", initialWorkers)
		}

		// キューを満杯にしてスケールアップをトリガー
		for i := range 9 {
			task := Task{
				ID:   i + 1,
				Data: "load",
				Execute: func(data any) error {
					time.Sleep(2 * time.Second) // 長い実行時間でキューを詰まらせる
					return nil
				},
			}
			if err := pool.SubmitTask(task); err != nil {
				t.Logf("Failed to submit task %d: %v", i, err)
			}
		}

		synctest.Wait()

		// メトリクスを更新してからスケーリングを実行
		pool.updateMetrics()
		pool.autoScale()

		synctest.Wait()

		// ワーカー数が増加していることを確認
		pool.mu.RLock()
		finalWorkers := len(pool.workers)
		pool.mu.RUnlock()

		if finalWorkers <= initialWorkers {
			t.Errorf("Expected worker count to increase, got %d (initial: %d)", finalWorkers, initialWorkers)
		}
	})
}

func TestWorkerPool_Shutdown_Graceful(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pool := NewWorkerPool(1, 1, 2)
		if err := pool.Start(); err != nil {
			t.Fatalf("Failed to start pool: %v", err)
		}

		var completedTasks atomic.Int64

		// 短時間実行されるタスクを追加
		task := Task{
			ID:   1,
			Data: "quick task",
			Execute: func(data any) error {
				time.Sleep(100 * time.Millisecond)
				completedTasks.Add(1)
				return nil
			},
		}
		if err := pool.SubmitTask(task); err != nil {
			t.Fatalf("Failed to submit task: %v", err)
		}

		// タスクの処理時間（100ms）が過ぎて完了するまで待つ
		synctest.Sleep(150 * time.Millisecond)

		start := time.Now()
		err := pool.Shutdown(2 * time.Second)
		duration := time.Since(start)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// タスクが完了していることを確認
		completed := completedTasks.Load()
		if completed == 0 {
			t.Error("No tasks were completed during shutdown")
		}

		// シャットダウンが適切な時間で完了していることを確認
		if duration > 3*time.Second {
			t.Errorf("Shutdown took too long: %v", duration)
		}
	})
}

func TestWorkerPool_Shutdown_Timeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pool := NewWorkerPool(1, 1, 5)

		if err := pool.Start(); err != nil {
			t.Fatalf("Failed to start pool: %v", err)
		}

		// WaitGroupをパッチして確実にタイムアウトを発生させる
		pool.wg.Add(1) // 手動で追加することで永続的にブロック

		// 短いタイムアウトでシャットダウン
		err := pool.Shutdown(50 * time.Millisecond)

		if err == nil {
			t.Error("Expected timeout error, got nil")
		}

		// クリーンアップ
		pool.wg.Done()
	})
}

func TestWorkerPool_ConcurrentSubmission(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pool := NewWorkerPool(3, 5, 60) // キューサイズを大きくする
		if err := pool.Start(); err != nil {
			t.Fatalf("Failed to start pool: %v", err)
		}
		defer func() {
			if err := pool.Shutdown(5 * time.Second); err != nil {
				t.Logf("Failed to shutdown pool: %v", err)
			}
		}()

		const numGoroutines = 10
		const tasksPerGoroutine = 5

		var wg sync.WaitGroup
		var successCount atomic.Int64
		var errorCount atomic.Int64

		// 複数のGoroutineから同時にタスクを送信
		for i := range numGoroutines {
			wg.Go(func() {
				goroutineID := i

				for j := range tasksPerGoroutine {
					task := Task{
						ID:   goroutineID*tasksPerGoroutine + j,
						Data: fmt.Sprintf("task-%d-%d", goroutineID, j),
						Execute: func(data any) error {
							time.Sleep(10 * time.Millisecond)
							return nil
						},
					}

					if err := pool.SubmitTask(task); err != nil {
						errorCount.Add(1)
					} else {
						successCount.Add(1)
					}
				}
			})
		}

		wg.Wait()

		// 50件×10msのタスクが全部終わるまで仮想時計を進める
		synctest.Sleep(time.Second)

		expectedTasks := int64(numGoroutines * tasksPerGoroutine)
		actualSuccess := successCount.Load()

		if actualSuccess != expectedTasks {
			t.Errorf("Expected %d successful submissions, got %d (errors: %d)", expectedTasks, actualSuccess, errorCount.Load())
		}

		metrics := pool.GetMetrics()
		if metrics.totalTasks != expectedTasks {
			t.Errorf("Expected %d total tasks, got %d", expectedTasks, metrics.totalTasks)
		}
	})
}
