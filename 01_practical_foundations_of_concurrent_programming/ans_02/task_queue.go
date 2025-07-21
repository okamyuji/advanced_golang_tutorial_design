package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// Task はタスクを表現します
type Task struct {
	ID        int
	CreatedAt time.Time
	Execute   func() error
}

// TaskQueueSystem はタスクキューシステムです
type TaskQueueSystem struct {
	tasks          chan Task
	maxQueueSize   int
	maxWorkers     int
	currentWorkers int64
	processedCount int64
	droppedCount   int64
	mu             sync.RWMutex
	workerWG       sync.WaitGroup
	shutdownChan   chan struct{}
	isShuttingDown bool
}

// NewTaskQueueSystem は新しいタスクキューシステムを作成します
func NewTaskQueueSystem(maxQueueSize, maxWorkers int) *TaskQueueSystem {
	return &TaskQueueSystem{
		tasks:        make(chan Task, maxQueueSize),
		maxQueueSize: maxQueueSize,
		maxWorkers:   maxWorkers,
		shutdownChan: make(chan struct{}),
	}
}

// Start はシステムを開始します
func (tqs *TaskQueueSystem) Start(ctx context.Context) {
	// ワーカーを開始
	for i := 0; i < tqs.maxWorkers; i++ {
		tqs.workerWG.Add(1)
		go tqs.worker(ctx, i)
	}

	// 統計情報を定期的に出力
	go tqs.statsReporter(ctx)
}

// worker はタスクを処理するワーカーです
func (tqs *TaskQueueSystem) worker(ctx context.Context, workerID int) {
	defer tqs.workerWG.Done()
	atomic.AddInt64(&tqs.currentWorkers, 1)
	defer atomic.AddInt64(&tqs.currentWorkers, -1)

	fmt.Printf("Worker %d started\n", workerID)

	for {
		select {
		case <-ctx.Done():
			fmt.Printf("Worker %d stopping due to context cancellation\n", workerID)
			return
		case <-tqs.shutdownChan:
			fmt.Printf("Worker %d stopping due to shutdown\n", workerID)
			return
		case task, ok := <-tqs.tasks:
			if !ok {
				fmt.Printf("Worker %d stopping due to closed channel\n", workerID)
				return
			}

			// タスクを実行
			if err := task.Execute(); err != nil {
				log.Printf("Worker %d: Task %d failed: %v", workerID, task.ID, err)
			} else {
				fmt.Printf("Worker %d: Task %d completed\n", workerID, task.ID)
			}

			atomic.AddInt64(&tqs.processedCount, 1)
		}
	}
}

// SubmitTask はタスクをキューに追加します
func (tqs *TaskQueueSystem) SubmitTask(task Task) error {
	tqs.mu.RLock()
	isShuttingDown := tqs.isShuttingDown
	tqs.mu.RUnlock()

	if isShuttingDown {
		return fmt.Errorf("system is shutting down")
	}

	select {
	case tqs.tasks <- task:
		return nil
	default:
		// キューが満杯の場合、古いタスクを破棄
		select {
		case <-tqs.tasks:
			atomic.AddInt64(&tqs.droppedCount, 1)
			fmt.Printf("Dropped old task to make room for task %d\n", task.ID)
		default:
		}

		// 新しいタスクを追加
		select {
		case tqs.tasks <- task:
			return nil
		default:
			atomic.AddInt64(&tqs.droppedCount, 1)
			return fmt.Errorf("failed to submit task %d: queue is full", task.ID)
		}
	}
}

// Shutdown はシステムを適切に停止します
func (tqs *TaskQueueSystem) Shutdown(timeout time.Duration) error {
	tqs.mu.Lock()
	if tqs.isShuttingDown {
		tqs.mu.Unlock()
		return fmt.Errorf("already shutting down")
	}
	tqs.isShuttingDown = true
	tqs.mu.Unlock()

	fmt.Println("Starting graceful shutdown...")

	// 新しいタスクの受付を停止
	close(tqs.shutdownChan)

	// タスクチャンネルを閉じる
	close(tqs.tasks)

	// ワーカーの完了を待機（タイムアウト付き）
	done := make(chan struct{})
	go func() {
		tqs.workerWG.Wait()
		close(done)
	}()

	select {
	case <-done:
		fmt.Println("All workers stopped gracefully")
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("shutdown timeout: some workers may still be running")
	}
}

// GetStats は統計情報を取得します
func (tqs *TaskQueueSystem) GetStats() (int64, int64, int64, int) {
	processed := atomic.LoadInt64(&tqs.processedCount)
	dropped := atomic.LoadInt64(&tqs.droppedCount)
	workers := atomic.LoadInt64(&tqs.currentWorkers)
	queueLength := len(tqs.tasks)

	return processed, dropped, workers, queueLength
}

// statsReporter は統計情報を定期的に出力します
func (tqs *TaskQueueSystem) statsReporter(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-tqs.shutdownChan:
			return
		case <-ticker.C:
			processed, dropped, workers, queueLength := tqs.GetStats()
			fmt.Printf("Stats: Processed=%d, Dropped=%d, Workers=%d, Queue=%d\n",
				processed, dropped, workers, queueLength)
		}
	}
}

func main() {
	// タスクキューシステムを作成（キュー容量5、最大ワーカー数3）
	tqs := NewTaskQueueSystem(5, 3)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// システムを開始
	tqs.Start(ctx)

	// タスクを生成して送信
	go func() {
		for i := 1; i <= 20; i++ {
			taskID := i
			task := Task{
				ID:        taskID,
				CreatedAt: time.Now(),
				Execute: func() error {
					// 処理時間をシミュレート
					time.Sleep(time.Duration(500) * time.Millisecond)

					// 10%の確率でエラーを発生
					if taskID%10 == 0 {
						return fmt.Errorf("simulated error for task %d", taskID)
					}
					return nil
				},
			}

			if err := tqs.SubmitTask(task); err != nil {
				log.Printf("Failed to submit task %d: %v", taskID, err)
			}

			time.Sleep(200 * time.Millisecond)
		}
	}()

	// しばらく動作させる
	time.Sleep(10 * time.Second)

	// システムを停止
	if err := tqs.Shutdown(5 * time.Second); err != nil {
		log.Printf("Shutdown error: %v", err)
	}

	// 最終統計を出力
	processed, dropped, workers, queueLength := tqs.GetStats()
	fmt.Printf("Final Stats: Processed=%d, Dropped=%d, Workers=%d, Queue=%d\n",
		processed, dropped, workers, queueLength)
}
