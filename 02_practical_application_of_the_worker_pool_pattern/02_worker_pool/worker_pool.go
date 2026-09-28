package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// Task ワーカーが処理するタスクを表現します
type Task struct {
	ID       int
	Data     any
	Execute  func(any) error
	Priority int // 優先度（高いほど優先）
}

// WorkerPool 動的なワーカープールを管理します
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
	workerID atomic.Int64
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

// Worker 個別のワーカーを表現します
type Worker struct {
	ID           int
	pool         *WorkerPool
	lastActivity time.Time
	taskCount    atomic.Int64
	errorCount   atomic.Int64
}

// TaskResult タスクの実行結果を表現します
type TaskResult struct {
	TaskID   int
	Success  bool
	Error    error
	Duration time.Duration
	WorkerID int
}

// PoolMetrics プールの統計情報を管理します
type PoolMetrics struct {
	mu               sync.RWMutex
	activeWorkers    atomic.Int64
	totalTasks       atomic.Int64
	completedTasks   atomic.Int64
	failedTasks      atomic.Int64
	averageTaskTime  time.Duration
	queueUtilization float64
	lastUpdateTime   time.Time
}

// PoolMetricsSnapshot GetMetrics が返すプールの統計スナップショットです
type PoolMetricsSnapshot struct {
	activeWorkers    int64
	totalTasks       int64
	completedTasks   int64
	failedTasks      int64
	queueUtilization float64
	lastUpdateTime   time.Time
}

// PoolConfig プールの設定を管理します
type PoolConfig struct {
	WorkerIdleTimeout  time.Duration
	MetricsInterval    time.Duration
	ScaleUpThreshold   float64 // キュー使用率の閾値
	ScaleDownThreshold float64 // アイドルワーカー比率の閾値
	MaxTaskRetries     int
	TaskTimeout        time.Duration
}

// NewWorkerPool 新しいワーカープールを作成します
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

// Start ワーカープールを開始します
func (wp *WorkerPool) Start() error {
	// 最小数のワーカーを開始
	for range wp.minWorkers {
		if err := wp.addWorker(); err != nil {
			return fmt.Errorf("failed to start initial worker: %w", err)
		}
	}

	// 結果処理Goroutineを開始
	wp.wg.Go(wp.resultHandler)

	// メトリクス収集Goroutineを開始
	wp.wg.Go(wp.metricsCollector)

	// 動的スケーリングGoroutineを開始
	wp.wg.Go(wp.dynamicScaler)

	log.Printf("Worker pool started with %d workers", wp.minWorkers)
	return nil
}

// addWorker 新しいワーカーを追加します
func (wp *WorkerPool) addWorker() error {
	wp.mu.Lock()
	defer wp.mu.Unlock()

	if len(wp.workers) >= wp.maxWorkers {
		return fmt.Errorf("maximum workers limit reached")
	}

	workerID := int(wp.workerID.Add(1))
	worker := &Worker{
		ID:           workerID,
		pool:         wp,
		lastActivity: time.Now(),
	}

	wp.workers[workerID] = worker

	wp.wg.Go(worker.run)

	wp.metrics.activeWorkers.Add(1)
	return nil
}

// removeWorker ワーカーを削除します
func (wp *WorkerPool) removeWorker(workerID int) {
	wp.mu.Lock()
	defer wp.mu.Unlock()

	if _, exists := wp.workers[workerID]; exists {
		delete(wp.workers, workerID)
		wp.metrics.activeWorkers.Add(-1)
	}
}

// SubmitTask タスクをキューに追加します
func (wp *WorkerPool) SubmitTask(task Task) error {
	// 停止後の投入を拒否する。キューはcloseせず、読む側はctxのキャンセルで止める
	if wp.ctx.Err() != nil {
		return fmt.Errorf("worker pool is shutting down")
	}
	select {
	case wp.taskQueue <- task:
		wp.metrics.totalTasks.Add(1)
		return nil
	case <-wp.ctx.Done():
		return fmt.Errorf("worker pool is shutting down")
	default:
		return fmt.Errorf("task queue is full")
	}
}

// run ワーカーのメインループです
func (w *Worker) run() {
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

			// Go 1.23以降はStopと排出なしのResetで古い発火値が届かない
			idleTimer.Reset(w.pool.config.WorkerIdleTimeout)
		}
	}
}

// executeTask タスクを実行します
func (w *Worker) executeTask(task Task) TaskResult {
	start := time.Now()

	// タスクカウントを更新
	w.taskCount.Add(1)

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
			w.errorCount.Add(1)
		}
		result.Duration = time.Since(start)
	}()

	// タスクを実行
	if task.Execute == nil {
		result.Success = false
		result.Error = fmt.Errorf("task execute function is nil")
		w.errorCount.Add(1)
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
			w.errorCount.Add(1)
		} else {
			result.Success = true
		}
	case <-ctx.Done():
		result.Success = false
		result.Error = fmt.Errorf("task timeout")
		w.errorCount.Add(1)
	}

	return result
}

// resultHandler 結果を処理します
func (wp *WorkerPool) resultHandler() {
	for {
		select {
		case <-wp.ctx.Done():
			return
		case result := <-wp.resultQueue:
			if result.Success {
				wp.metrics.completedTasks.Add(1)
			} else {
				wp.metrics.failedTasks.Add(1)
				log.Printf("Task %d failed on worker %d: %v",
					result.TaskID, result.WorkerID, result.Error)
			}

			// 平均タスク実行時間を更新
			wp.updateAverageTaskTime(result.Duration)
		}
	}
}

// updateAverageTaskTime 平均タスク実行時間を更新します
func (wp *WorkerPool) updateAverageTaskTime(duration time.Duration) {
	wp.metrics.mu.Lock()
	defer wp.metrics.mu.Unlock()

	totalCompleted := wp.metrics.completedTasks.Load() + wp.metrics.failedTasks.Load()
	if totalCompleted > 0 {
		// 累積平均を計算
		wp.metrics.averageTaskTime = time.Duration(
			(int64(wp.metrics.averageTaskTime)*totalCompleted + int64(duration)) / (totalCompleted + 1))
	}
}

// metricsCollector メトリクスを定期的に収集します
func (wp *WorkerPool) metricsCollector() {
	tick := time.Tick(wp.config.MetricsInterval)

	for {
		select {
		case <-wp.ctx.Done():
			return
		case <-tick:
			wp.updateMetrics()
		}
	}
}

// updateMetrics メトリクスを更新します
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

	total := wp.metrics.totalTasks.Load()
	completed := wp.metrics.completedTasks.Load()
	failed := wp.metrics.failedTasks.Load()

	log.Printf("Pool Stats: Workers=%d, Queue=%d/%d (%.1f%%), Total=%d, Completed=%d, Failed=%d",
		workerCount, queueLength, wp.taskQueueSize, wp.metrics.queueUtilization*100,
		total, completed, failed)
}

// dynamicScaler 動的スケーリングを実行します
func (wp *WorkerPool) dynamicScaler() {
	tick := time.Tick(10 * time.Second)

	for {
		select {
		case <-wp.ctx.Done():
			return
		case <-tick:
			wp.autoScale()
		}
	}
}

// autoScale 自動スケーリングを実行します
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

// Shutdown ワーカープールを適切に停止します
func (wp *WorkerPool) Shutdown(timeout time.Duration) error {
	log.Println("Starting worker pool shutdown...")

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

// GetMetrics 現在のメトリクスを取得します
func (wp *WorkerPool) GetMetrics() *PoolMetricsSnapshot {
	wp.metrics.mu.RLock()
	defer wp.metrics.mu.RUnlock()

	// メトリクスのスナップショットを返す
	return &PoolMetricsSnapshot{
		activeWorkers:    wp.metrics.activeWorkers.Load(),
		totalTasks:       wp.metrics.totalTasks.Load(),
		completedTasks:   wp.metrics.completedTasks.Load(),
		failedTasks:      wp.metrics.failedTasks.Load(),
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
		for i := range 100 {
			taskID := i + 1
			task := Task{
				ID:   taskID,
				Data: fmt.Sprintf("Task data %d", taskID),
				Execute: func(data any) error {
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
