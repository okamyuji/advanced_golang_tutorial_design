package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// Taskはワーカーが処理するタスクを表現します
type Task struct {
	ID       int
	Data     interface{}
	Execute  func(interface{}) error
	Priority int // 優先度（高いほど優先）
}

// WorkerPoolは動的なワーカープールを管理します
type WorkerPool struct {
	// 基本設定
	minWorkers    int
	maxWorkers    int
	taskQueueSize int

	// チャンネル
	taskQueue   chan Task
	resultQueue chan TaskResult

	// ワーカー管理
	workers  map[int]*Worker
	workerID int64
	mu       sync.RWMutex

	// 制御
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// メトリクス
	metrics *PoolMetrics

	// 設定
	config *PoolConfig
}

// Workerは個別のワーカーを表現します
type Worker struct {
	ID           int
	pool         *WorkerPool
	lastActivity time.Time
	taskCount    int64
	errorCount   int64
}

// TaskResultはタスクの実行結果を表現します
type TaskResult struct {
	TaskID   int
	Success  bool
	Error    error
	Duration time.Duration
	WorkerID int
}

// PoolMetricsはプールの統計情報を管理します
type PoolMetrics struct {
	mu               sync.RWMutex
	activeWorkers    int64
	totalTasks       int64
	completedTasks   int64
	failedTasks      int64
	averageTaskTime  time.Duration
	queueUtilization float64
	lastUpdateTime   time.Time
}

// PoolConfigはプールの設定を管理します
type PoolConfig struct {
	WorkerIdleTimeout  time.Duration
	MetricsInterval    time.Duration
	ScaleUpThreshold   float64 // キュー使用率の閾値
	ScaleDownThreshold float64 // アイドルワーカー比率の閾値
	MaxTaskRetries     int
	TaskTimeout        time.Duration
}

// NewWorkerPoolは新しいワーカープールを作成します
func NewWorkerPool(minWorkers, maxWorkers, queueSize int) *WorkerPool {
	ctx, cancel := context.WithCancel(context.Background())

	return &WorkerPool{
		minWorkers:    minWorkers,
		maxWorkers:    maxWorkers,
		taskQueueSize: queueSize,
		taskQueue:     make(chan Task, queueSize),
		resultQueue:   make(chan TaskResult, queueSize),
		workers:       make(map[int]*Worker),
		ctx:           ctx,
		cancel:        cancel,
		metrics:       &PoolMetrics{},
		config: &PoolConfig{
			WorkerIdleTimeout:  30 * time.Second,
			MetricsInterval:    5 * time.Second,
			ScaleUpThreshold:   0.8, // キュー使用率80%で拡張
			ScaleDownThreshold: 0.3, // アイドル30%で縮小
			MaxTaskRetries:     3,
			TaskTimeout:        60 * time.Second,
		},
	}
}

// Startはワーカープールを開始します
func (wp *WorkerPool) Start() error {
	// 最小数のワーカーを開始
	for i := 0; i < wp.minWorkers; i++ {
		if err := wp.addWorker(); err != nil {
			return fmt.Errorf("failed to start initial worker: %v", err)
		}
	}

	// 結果処理Goroutineを開始
	wp.wg.Add(1)
	go wp.resultHandler()

	// メトリクス収集Goroutineを開始
	wp.wg.Add(1)
	go wp.metricsCollector()

	// 動的スケーリングGoroutineを開始
	wp.wg.Add(1)
	go wp.dynamicScaler()

	log.Printf("Worker pool started with %d workers", wp.minWorkers)
	return nil
}

// addWorkerは新しいワーカーを追加します
func (wp *WorkerPool) addWorker() error {
	wp.mu.Lock()
	defer wp.mu.Unlock()

	if len(wp.workers) >= wp.maxWorkers {
		return fmt.Errorf("maximum workers limit reached")
	}

	workerID := int(atomic.AddInt64(&wp.workerID, 1))
	worker := &Worker{
		ID:           workerID,
		pool:         wp,
		lastActivity: time.Now(),
	}

	wp.workers[workerID] = worker

	wp.wg.Add(1)
	go worker.run()

	atomic.AddInt64(&wp.metrics.activeWorkers, 1)
	return nil
}

// removeWorkerはワーカーを削除します
func (wp *WorkerPool) removeWorker(workerID int) {
	wp.mu.Lock()
	defer wp.mu.Unlock()

	if _, exists := wp.workers[workerID]; exists {
		delete(wp.workers, workerID)
		atomic.AddInt64(&wp.metrics.activeWorkers, -1)
	}
}

// SubmitTaskはタスクをキューに追加します
func (wp *WorkerPool) SubmitTask(task Task) error {
	select {
	case wp.taskQueue <- task:
		atomic.AddInt64(&wp.metrics.totalTasks, 1)
		return nil
	case <-wp.ctx.Done():
		return fmt.Errorf("worker pool is shutting down")
	default:
		return fmt.Errorf("task queue is full")
	}
}

// runはワーカーのメインループです
func (w *Worker) run() {
	defer w.pool.wg.Done()
	defer w.pool.removeWorker(w.ID)

	idleTimer := time.NewTimer(w.pool.config.WorkerIdleTimeout)
	defer idleTimer.Stop()

	for {
		select {
		case <-w.pool.ctx.Done():
			return
		case <-idleTimer.C:
			// アイドルタイムアウトでワーカーを終了（最小数は維持）
			w.pool.mu.RLock()
			currentWorkers := len(w.pool.workers)
			w.pool.mu.RUnlock()

			if currentWorkers > w.pool.minWorkers {
				log.Printf("Worker %d terminating due to idle timeout", w.ID)
				return
			}
			idleTimer.Reset(w.pool.config.WorkerIdleTimeout)

		case task := <-w.pool.taskQueue:
			w.lastActivity = time.Now()
			result := w.executeTask(task)

			// 結果を送信
			select {
			case w.pool.resultQueue <- result:
			case <-w.pool.ctx.Done():
				return
			}

			// アイドルタイマーをリセット
			if !idleTimer.Stop() {
				<-idleTimer.C
			}
			idleTimer.Reset(w.pool.config.WorkerIdleTimeout)
		}
	}
}

// executeTaskはタスクを実行します
func (w *Worker) executeTask(task Task) TaskResult {
	start := time.Now()

	// タスクカウントを更新
	atomic.AddInt64(&w.taskCount, 1)

	// タイムアウト付きでタスクを実行
	ctx, cancel := context.WithTimeout(w.pool.ctx, w.pool.config.TaskTimeout)
	defer cancel()

	result := TaskResult{
		TaskID:   task.ID,
		WorkerID: w.ID,
		Duration: 0,
	}

	// パニック回復
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Worker %d: Task %d panicked: %v", w.ID, task.ID, r)
			result.Success = false
			result.Error = fmt.Errorf("task panicked: %v", r)
			atomic.AddInt64(&w.errorCount, 1)
		}
		result.Duration = time.Since(start)
	}()

	// タスクを実行
	if task.Execute == nil {
		result.Success = false
		result.Error = fmt.Errorf("task execute function is nil")
		atomic.AddInt64(&w.errorCount, 1)
		return result
	}

	done := make(chan error, 1)
	go func() {
		done <- task.Execute(task.Data)
	}()

	select {
	case err := <-done:
		if err != nil {
			result.Success = false
			result.Error = err
			atomic.AddInt64(&w.errorCount, 1)
		} else {
			result.Success = true
		}
	case <-ctx.Done():
		result.Success = false
		result.Error = fmt.Errorf("task timeout")
		atomic.AddInt64(&w.errorCount, 1)
	}

	return result
}

// resultHandlerは結果を処理します
func (wp *WorkerPool) resultHandler() {
	defer wp.wg.Done()

	for {
		select {
		case <-wp.ctx.Done():
			return
		case result := <-wp.resultQueue:
			if result.Success {
				atomic.AddInt64(&wp.metrics.completedTasks, 1)
			} else {
				atomic.AddInt64(&wp.metrics.failedTasks, 1)
				log.Printf("Task %d failed on worker %d: %v",
					result.TaskID, result.WorkerID, result.Error)
			}

			// 平均タスク実行時間を更新
			wp.updateAverageTaskTime(result.Duration)
		}
	}
}

// updateAverageTaskTimeは平均タスク実行時間を更新します
func (wp *WorkerPool) updateAverageTaskTime(duration time.Duration) {
	wp.metrics.mu.Lock()
	defer wp.metrics.mu.Unlock()

	totalCompleted := atomic.LoadInt64(&wp.metrics.completedTasks) + atomic.LoadInt64(&wp.metrics.failedTasks)
	if totalCompleted > 0 {
		// 累積平均を計算
		wp.metrics.averageTaskTime = time.Duration(
			(int64(wp.metrics.averageTaskTime)*totalCompleted + int64(duration)) / (totalCompleted + 1))
	}
}

// metricsCollectorはメトリクスを定期的に収集します
func (wp *WorkerPool) metricsCollector() {
	defer wp.wg.Done()

	ticker := time.NewTicker(wp.config.MetricsInterval)
	defer ticker.Stop()

	for {
		select {
		case <-wp.ctx.Done():
			return
		case <-ticker.C:
			wp.updateMetrics()
		}
	}
}

// updateMetricsはメトリクスを更新します
func (wp *WorkerPool) updateMetrics() {
	wp.metrics.mu.Lock()
	defer wp.metrics.mu.Unlock()

	// キュー使用率を計算
	queueLength := len(wp.taskQueue)
	wp.metrics.queueUtilization = float64(queueLength) / float64(wp.taskQueueSize)
	wp.metrics.lastUpdateTime = time.Now()

	// 統計情報を出力
	wp.mu.RLock()
	workerCount := len(wp.workers)
	wp.mu.RUnlock()

	total := atomic.LoadInt64(&wp.metrics.totalTasks)
	completed := atomic.LoadInt64(&wp.metrics.completedTasks)
	failed := atomic.LoadInt64(&wp.metrics.failedTasks)

	log.Printf("Pool Stats: Workers=%d, Queue=%d/%d (%.1f%%), Total=%d, Completed=%d, Failed=%d",
		workerCount, queueLength, wp.taskQueueSize, wp.metrics.queueUtilization*100,
		total, completed, failed)
}

// dynamicScalerは動的スケーリングを実行します
func (wp *WorkerPool) dynamicScaler() {
	defer wp.wg.Done()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-wp.ctx.Done():
			return
		case <-ticker.C:
			wp.autoScale()
		}
	}
}

// autoScaleは自動スケーリングを実行します
func (wp *WorkerPool) autoScale() {
	wp.metrics.mu.RLock()
	queueUtil := wp.metrics.queueUtilization
	wp.metrics.mu.RUnlock()

	wp.mu.RLock()
	currentWorkers := len(wp.workers)
	wp.mu.RUnlock()

	// スケールアップ判定
	if queueUtil > wp.config.ScaleUpThreshold && currentWorkers < wp.maxWorkers {
		if err := wp.addWorker(); err == nil {
			log.Printf("Scaled up: %d -> %d workers (queue utilization: %.1f%%)",
				currentWorkers, currentWorkers+1, queueUtil*100)
		}
	}

	// スケールダウンは自然に発生（アイドルタイムアウト）
}

// Shutdownはワーカープールを適切に停止します
func (wp *WorkerPool) Shutdown(timeout time.Duration) error {
	log.Println("Starting worker pool shutdown...")

	// 新しいタスクの受付を停止
	close(wp.taskQueue)

	// ワーカーに停止シグナルを送信
	wp.cancel()

	// 完了を待機（タイムアウト付き）
	done := make(chan struct{})
	go func() {
		wp.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("Worker pool shutdown completed")
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("shutdown timeout exceeded")
	}
}

// GetMetricsは現在のメトリクスを取得します
func (wp *WorkerPool) GetMetrics() *PoolMetrics {
	wp.metrics.mu.RLock()
	defer wp.metrics.mu.RUnlock()

	// メトリクスのコピーを返す
	return &PoolMetrics{
		activeWorkers:    atomic.LoadInt64(&wp.metrics.activeWorkers),
		totalTasks:       atomic.LoadInt64(&wp.metrics.totalTasks),
		completedTasks:   atomic.LoadInt64(&wp.metrics.completedTasks),
		failedTasks:      atomic.LoadInt64(&wp.metrics.failedTasks),
		queueUtilization: wp.metrics.queueUtilization,
		lastUpdateTime:   wp.metrics.lastUpdateTime,
	}
}

func main() {
	// ワーカープールを作成（最小2、最大10、キューサイズ50）
	pool := NewWorkerPool(2, 10, 50)

	// プールを開始
	if err := pool.Start(); err != nil {
		log.Fatalf("Failed to start worker pool: %v", err)
	}

	// サンプルタスクを送信
	go func() {
		for i := 1; i <= 100; i++ {
			taskID := i
			task := Task{
				ID:   taskID,
				Data: fmt.Sprintf("Task data %d", taskID),
				Execute: func(data interface{}) error {
					// シミュレーション処理
					processingTime := time.Duration(100) * time.Millisecond
					time.Sleep(processingTime)

					// 10%の確率でエラー
					if taskID%10 == 0 {
						return fmt.Errorf("simulated error for task %d", taskID)
					}

					return nil
				},
				Priority: taskID % 3, // 0-2の優先度
			}

			if err := pool.SubmitTask(task); err != nil {
				log.Printf("Failed to submit task %d: %v", taskID, err)
			}

			time.Sleep(50 * time.Millisecond)
		}
	}()

	// 30秒間動作させる
	time.Sleep(30 * time.Second)

	// シャットダウン
	if err := pool.Shutdown(10 * time.Second); err != nil {
		log.Printf("Shutdown error: %v", err)
	}

	// 最終メトリクスを表示
	metrics := pool.GetMetrics()
	fmt.Printf("Final metrics: Workers=%d, Total=%d, Completed=%d, Failed=%d\n",
		metrics.activeWorkers, metrics.totalTasks, metrics.completedTasks, metrics.failedTasks)
}
