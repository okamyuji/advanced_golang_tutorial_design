package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// Task タスクを表現します
type Task struct {
	ID        int
	CreatedAt time.Time
	Execute   func() error
}

// TaskQueueSystem タスクキューシステムです
type TaskQueueSystem struct {
	tasks          chan Task
	maxQueueSize   int
	maxWorkers     int
	currentWorkers atomic.Int64
	processedCount atomic.Int64
	droppedCount   atomic.Int64
	mu             sync.RWMutex
	workerWG       sync.WaitGroup
	shutdownChan   chan struct{}
	isShuttingDown bool
}

// NewTaskQueueSystem 新しいタスクキューシステムを作成します
func NewTaskQueueSystem(maxQueueSize, maxWorkers int) *TaskQueueSystem {
	return &TaskQueueSystem{
		tasks:        make(chan Task, maxQueueSize),
		maxQueueSize: maxQueueSize,
		maxWorkers:   maxWorkers,
		shutdownChan: make(chan struct{}),
	}
}

// Start システムを開始します
func (tqs *TaskQueueSystem) Start(ctx context.Context) {
	// ワーカーを開始
	for i := range tqs.maxWorkers {
		tqs.workerWG.Go(func() { tqs.worker(ctx, i) })
	}

	// 統計情報を定期的に出力
	go tqs.statsReporter(ctx)
}

// worker タスクを処理するワーカーです
func (tqs *TaskQueueSystem) worker(ctx context.Context, workerID int) {
	tqs.currentWorkers.Add(1)
	defer tqs.currentWorkers.Add(-1)

	fmt.Printf("Worker %d started\n", workerID)

	for {
		select {
		case <-ctx.Done():
			fmt.Printf("Worker %d stopping due to context cancellation\n", workerID)
			return
		case task, ok := <-tqs.tasks:
			// Shutdown tasksを閉じるだけなので、キューに残ったタスクを処理し終えてから抜ける
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

			tqs.processedCount.Add(1)
		}
	}
}

// SubmitTask タスクをキューに追加します
func (tqs *TaskQueueSystem) SubmitTask(task Task) error {
	// 送信が終わるまで読み取りロックを持ち、Shutdownのclose(tasks)と重ならないようにする
	tqs.mu.RLock()
	defer tqs.mu.RUnlock()

	if tqs.isShuttingDown {
		return fmt.Errorf("system is shutting down")
	}

	select {
	case tqs.tasks <- task:
		return nil
	default:
		// キューが満杯の場合、古いタスクを破棄
		select {
		case <-tqs.tasks:
			tqs.droppedCount.Add(1)
			fmt.Printf("Dropped old task to make room for task %d\n", task.ID)
		default:
		}

		// 新しいタスクを追加
		select {
		case tqs.tasks <- task:
			return nil
		default:
			tqs.droppedCount.Add(1)
			return fmt.Errorf("failed to submit task %d: queue is full", task.ID)
		}
	}
}

// Shutdown システムを適切に停止します
func (tqs *TaskQueueSystem) Shutdown(timeout time.Duration) error {
	tqs.mu.Lock()
	if tqs.isShuttingDown {
		tqs.mu.Unlock()
		return fmt.Errorf("already shutting down")
	}
	tqs.isShuttingDown = true
	// 新しいタスクの受付を停止する。書き込みロック中なのでSubmitTaskの送信とは重ならない
	close(tqs.tasks)
	tqs.mu.Unlock()

	fmt.Println("Starting graceful shutdown...")
	close(tqs.shutdownChan)

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

// GetStats 統計情報を取得します
func (tqs *TaskQueueSystem) GetStats() (int64, int64, int64, int) {
	processed := tqs.processedCount.Load()
	dropped := tqs.droppedCount.Load()
	workers := tqs.currentWorkers.Load()
	queueLength := len(tqs.tasks)

	return processed, dropped, workers, queueLength
}

// statsReporter 統計情報を定期的に出力します
func (tqs *TaskQueueSystem) statsReporter(ctx context.Context) {
	tick := time.Tick(2 * time.Second)

	for {
		select {
		case <-ctx.Done():
			return
		case <-tqs.shutdownChan:
			return
		case <-tick:
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
		for i := range 20 {
			taskID := i + 1
			task := Task{
				ID:        taskID,
				CreatedAt: time.Now(),
				Execute: func() error {
					// 処理時間をシミュレート
					time.Sleep(500 * time.Millisecond)

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
