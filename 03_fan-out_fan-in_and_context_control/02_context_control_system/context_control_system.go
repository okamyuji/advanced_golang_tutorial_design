package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// ContextTypeはコンテキストタイプです
type ContextType string

const (
	ContextTypeTimeout  ContextType = "timeout"
	ContextTypeCancel   ContextType = "cancel"
	ContextTypeDeadline ContextType = "deadline"
	ContextTypeValue    ContextType = "value"
)

// Taskはタスクデータです
type Task struct {
	ID         int64
	Name       string
	Data       []byte
	Priority   int
	Timeout    time.Duration
	Retries    int
	MaxRetries int
	CreatedAt  time.Time
	Metadata   map[string]interface{}
}

// TaskResultはタスク実行結果です
type TaskResult struct {
	TaskID      int64
	Success     bool
	Result      interface{}
	Error       error
	Duration    time.Duration
	WorkerID    int
	ContextType ContextType
	RetryCount  int
	CompletedAt time.Time
}

// ContextControllerはコンテキスト制御管理者です
type ContextControlController struct {
	// 基本設定
	workerCount  int
	maxQueueSize int

	// コンテキスト制御
	rootCtx context.Context
	cancel  context.CancelFunc

	// タスク管理
	taskQueue   chan Task
	resultQueue chan TaskResult
	workers     []*ContextWorker

	// 同期制御
	wg sync.WaitGroup

	// 統計・監視
	stats     *ControllerStats
	isRunning int32
	startTime time.Time

	// タイムアウト制御
	defaultTimeout time.Duration
	timeoutManager *TimeoutManager
}

// ContextWorkerはコンテキスト制御ワーカーです
type ContextWorker struct {
	ID          int
	controller  *ContextControlController
	currentTask *Task
	mu          sync.RWMutex
	stats       *WorkerStats
	processor   *TaskProcessor
}

// TaskProcessorはタスク処理エンジンです
type TaskProcessor struct {
	ID             int
	processedCount int64
	errorCount     int64
	timeoutCount   int64
	cancelledCount int64
}

// ControllerStatsはコントローラー統計です
type ControllerStats struct {
	mu                 sync.RWMutex
	totalSubmitted     int64
	totalCompleted     int64
	totalSuccess       int64
	totalErrors        int64
	totalTimeouts      int64
	totalCancellations int64
	averageProcessTime time.Duration
	startTime          time.Time
	workerStats        map[int]*WorkerStats
}

// WorkerStatsはワーカー統計です
type WorkerStats struct {
	WorkerID           int
	TasksProcessed     int64
	TasksSuccess       int64
	TasksError         int64
	TasksTimeout       int64
	TasksCancelled     int64
	TotalProcessTime   time.Duration
	AverageProcessTime time.Duration
}

// TimeoutManagerはタイムアウト管理です
type TimeoutManager struct {
	mu             sync.RWMutex
	activeTimeouts map[int64]context.CancelFunc
	defaultTimeout time.Duration
	maxTimeout     time.Duration
}

// TaskConfig タスク設定です
type TaskConfig struct {
	DefaultTimeout  time.Duration
	MaxTimeout      time.Duration
	WorkerCount     int
	QueueSize       int
	EnableMetrics   bool
	MetricsInterval time.Duration
	RetryEnabled    bool
	MaxRetries      int
}

// NewTaskConfigはデフォルトタスク設定を作成します
func NewTaskConfig() *TaskConfig {
	return &TaskConfig{
		DefaultTimeout:  30 * time.Second,
		MaxTimeout:      5 * time.Minute,
		WorkerCount:     4,
		QueueSize:       1000,
		EnableMetrics:   true,
		MetricsInterval: 10 * time.Second,
		RetryEnabled:    true,
		MaxRetries:      3,
	}
}

// NewTimeoutManagerは新しいタイムアウトマネージャーを作成します
func NewTimeoutManager(defaultTimeout, maxTimeout time.Duration) *TimeoutManager {
	return &TimeoutManager{
		activeTimeouts: make(map[int64]context.CancelFunc),
		defaultTimeout: defaultTimeout,
		maxTimeout:     maxTimeout,
	}
}

// NewContextControlControllerは新しいコンテキスト制御コントローラーを作成します
func NewContextControlController(config *TaskConfig) *ContextControlController {
	rootCtx, cancel := context.WithCancel(context.Background())

	controller := &ContextControlController{
		workerCount:    config.WorkerCount,
		maxQueueSize:   config.QueueSize,
		rootCtx:        rootCtx,
		cancel:         cancel,
		taskQueue:      make(chan Task, config.QueueSize),
		resultQueue:    make(chan TaskResult, config.QueueSize),
		workers:        make([]*ContextWorker, config.WorkerCount),
		defaultTimeout: config.DefaultTimeout,
		timeoutManager: NewTimeoutManager(config.DefaultTimeout, config.MaxTimeout),
		stats: &ControllerStats{
			workerStats: make(map[int]*WorkerStats),
			startTime:   time.Now(),
		},
	}

	// ワーカーを初期化
	for i := 0; i < config.WorkerCount; i++ {
		worker := &ContextWorker{
			ID:         i,
			controller: controller,
			stats: &WorkerStats{
				WorkerID: i,
			},
			processor: &TaskProcessor{
				ID: i,
			},
		}
		controller.workers[i] = worker
		controller.stats.workerStats[i] = worker.stats
	}

	return controller
}

// Startはコントローラーを開始します
func (c *ContextControlController) Start() error {
	if !atomic.CompareAndSwapInt32(&c.isRunning, 0, 1) {
		return fmt.Errorf("controller is already running")
	}

	c.startTime = time.Now()
	c.stats.startTime = c.startTime

	log.Printf("Starting context control controller with %d workers", c.workerCount)

	// ワーカーを開始
	for _, worker := range c.workers {
		c.wg.Add(1)
		go worker.run()
	}

	// 結果ハンドラーを開始
	c.wg.Add(1)
	go c.handleResults()

	// メトリクス監視を開始
	c.wg.Add(1)
	go c.monitorMetrics()

	// タイムアウト清理を開始
	c.wg.Add(1)
	go c.timeoutManager.cleanup()

	log.Printf("Context control controller started successfully")
	return nil
}

// runはワーカーのメインループです
func (w *ContextWorker) run() {
	defer w.controller.wg.Done()

	log.Printf("Context worker %d started", w.ID)

	for {
		select {
		case <-w.controller.rootCtx.Done():
			log.Printf("Worker %d stopping due to root context cancellation", w.ID)
			return
		case task, ok := <-w.controller.taskQueue:
			if !ok {
				log.Printf("Worker %d stopping due to task queue closure", w.ID)
				return
			}

			// タスクを処理
			result := w.processTaskWithContext(task)

			// 結果を送信
			select {
			case w.controller.resultQueue <- result:
				// 正常に送信完了
			case <-w.controller.rootCtx.Done():
				log.Printf("Worker %d: context cancelled while sending result", w.ID)
				return
			}
		}
	}
}

// processTaskWithContextはコンテキスト付きでタスクを処理します
func (w *ContextWorker) processTaskWithContext(task Task) TaskResult {
	start := time.Now()

	// 現在のタスクを設定
	w.mu.Lock()
	w.currentTask = &task
	w.mu.Unlock()

	defer func() {
		w.mu.Lock()
		w.currentTask = nil
		w.mu.Unlock()
	}()

	result := TaskResult{
		TaskID:      task.ID,
		WorkerID:    w.ID,
		RetryCount:  task.Retries,
		CompletedAt: time.Now(),
	}

	// タスク固有のタイムアウトを決定
	timeout := task.Timeout
	if timeout == 0 {
		timeout = w.controller.defaultTimeout
	}

	// タイムアウト付きコンテキストを作成
	taskCtx, cancel := context.WithTimeout(w.controller.rootCtx, timeout)
	defer cancel()

	// タイムアウト管理に登録
	w.controller.timeoutManager.registerTask(task.ID, cancel)
	defer w.controller.timeoutManager.unregisterTask(task.ID)

	// コンテキストタイプを設定
	result.ContextType = ContextTypeTimeout

	// 実際のタスク処理を実行
	processingResult := w.executeTaskWithContext(taskCtx, task)

	result.Duration = time.Since(start)
	result.Success = processingResult.Success
	result.Result = processingResult.Result
	result.Error = processingResult.Error

	// エラーの種類に基づいてコンテキストタイプを更新
	if result.Error != nil {
		switch result.Error {
		case context.DeadlineExceeded:
			result.ContextType = ContextTypeTimeout
			atomic.AddInt64(&w.stats.TasksTimeout, 1)
			atomic.AddInt64(&w.processor.timeoutCount, 1)
		case context.Canceled:
			result.ContextType = ContextTypeCancel
			atomic.AddInt64(&w.stats.TasksCancelled, 1)
			atomic.AddInt64(&w.processor.cancelledCount, 1)
		default:
			atomic.AddInt64(&w.stats.TasksError, 1)
			atomic.AddInt64(&w.processor.errorCount, 1)
		}
	} else {
		atomic.AddInt64(&w.stats.TasksSuccess, 1)
	}

	// 統計更新
	atomic.AddInt64(&w.stats.TasksProcessed, 1)
	atomic.AddInt64(&w.processor.processedCount, 1)
	w.stats.TotalProcessTime += result.Duration

	if w.stats.TasksProcessed > 0 {
		w.stats.AverageProcessTime = w.stats.TotalProcessTime / time.Duration(w.stats.TasksProcessed)
	}

	return result
}

// TaskProcessingResultはタスク処理結果です
type TaskProcessingResult struct {
	Success bool
	Result  interface{}
	Error   error
}

// executeTaskWithContextはコンテキスト付きでタスクを実行します
func (w *ContextWorker) executeTaskWithContext(ctx context.Context, task Task) TaskProcessingResult {
	// 実際の処理をシミュレート
	processingTime := time.Duration(rand.Intn(1000)+100) * time.Millisecond

	// 長時間処理をシミュレートするために、短い間隔でコンテキストをチェック
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	startTime := time.Now()

	for {
		select {
		case <-ctx.Done():
			// コンテキストがキャンセルまたはタイムアウト
			return TaskProcessingResult{
				Success: false,
				Error:   ctx.Err(),
			}
		case <-ticker.C:
			if time.Since(startTime) >= processingTime {
				// 処理完了
				// ランダムにエラーを発生（10%の確率）
				if rand.Float64() < 0.1 {
					return TaskProcessingResult{
						Success: false,
						Error:   fmt.Errorf("random processing error for task %d", task.ID),
					}
				}

				return TaskProcessingResult{
					Success: true,
					Result:  fmt.Sprintf("processed_task_%d_by_worker_%d", task.ID, w.ID),
				}
			}
		}
	}
}

// SubmitTaskはタスクをキューに追加します
func (c *ContextControlController) SubmitTask(task Task) error {
	if atomic.LoadInt32(&c.isRunning) == 0 {
		return fmt.Errorf("controller is not running")
	}

	// デフォルト値を設定
	if task.CreatedAt.IsZero() {
		task.CreatedAt = time.Now()
	}
	if task.Timeout == 0 {
		task.Timeout = c.defaultTimeout
	}

	select {
	case c.taskQueue <- task:
		atomic.AddInt64(&c.stats.totalSubmitted, 1)
		return nil
	case <-c.rootCtx.Done():
		return fmt.Errorf("controller is shutting down")
	default:
		return fmt.Errorf("task queue is full")
	}
}

// SubmitTaskWithTimeoutはタイムアウト付きでタスクを追加します
func (c *ContextControlController) SubmitTaskWithTimeout(task Task, submitTimeout time.Duration) error {
	ctx, cancel := context.WithTimeout(c.rootCtx, submitTimeout)
	defer cancel()

	select {
	case c.taskQueue <- task:
		atomic.AddInt64(&c.stats.totalSubmitted, 1)
		return nil
	case <-ctx.Done():
		return fmt.Errorf("submit timeout: %v", ctx.Err())
	}
}

// handleResultsは結果を処理します
func (c *ContextControlController) handleResults() {
	defer c.wg.Done()

	log.Println("Result handler started")

	for {
		select {
		case <-c.rootCtx.Done():
			log.Println("Result handler stopping due to context cancellation")
			return
		case result, ok := <-c.resultQueue:
			if !ok {
				log.Println("Result handler stopping due to result queue closure")
				return
			}

			// 統計更新
			atomic.AddInt64(&c.stats.totalCompleted, 1)

			if result.Success {
				atomic.AddInt64(&c.stats.totalSuccess, 1)
			} else {
				switch result.ContextType {
				case ContextTypeTimeout:
					atomic.AddInt64(&c.stats.totalTimeouts, 1)
				case ContextTypeCancel:
					atomic.AddInt64(&c.stats.totalCancellations, 1)
				default:
					atomic.AddInt64(&c.stats.totalErrors, 1)
				}
			}

			// 平均処理時間を更新
			completed := atomic.LoadInt64(&c.stats.totalCompleted)
			if completed > 0 {
				c.stats.mu.Lock()
				c.stats.averageProcessTime = time.Duration(
					(int64(c.stats.averageProcessTime)*completed + int64(result.Duration)) / (completed + 1))
				c.stats.mu.Unlock()
			}

			// ログ出力（詳細）
			if result.Error != nil {
				log.Printf("Task %d completed with error (Worker %d): %v [%s] - Duration: %v",
					result.TaskID, result.WorkerID, result.Error, result.ContextType, result.Duration)
			} else {
				log.Printf("Task %d completed successfully (Worker %d): %v - Duration: %v",
					result.TaskID, result.WorkerID, result.Result, result.Duration)
			}
		}
	}
}

// registerTaskはタスクのタイムアウトを登録します
func (tm *TimeoutManager) registerTask(taskID int64, cancel context.CancelFunc) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.activeTimeouts[taskID] = cancel
}

// unregisterTaskはタスクのタイムアウトを登録解除します
func (tm *TimeoutManager) unregisterTask(taskID int64) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	delete(tm.activeTimeouts, taskID)
}

// cancelTaskはタスクをキャンセルします
func (tm *TimeoutManager) cancelTask(taskID int64) bool {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if cancel, exists := tm.activeTimeouts[taskID]; exists {
		cancel()
		delete(tm.activeTimeouts, taskID)
		return true
	}
	return false
}

// cleanupは期限切れタイムアウトを清理します
func (tm *TimeoutManager) cleanup() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		tm.mu.Lock()
		activeCount := len(tm.activeTimeouts)
		tm.mu.Unlock()

		if activeCount > 0 {
			log.Printf("TimeoutManager: %d active timeouts", activeCount)
		}
	}
}

// monitorMetricsはメトリクスを監視します
func (c *ContextControlController) monitorMetrics() {
	defer c.wg.Done()

	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	log.Println("Metrics monitor started")

	for {
		select {
		case <-c.rootCtx.Done():
			log.Println("Metrics monitor stopping")
			return
		case <-ticker.C:
			c.reportMetrics()
		}
	}
}

// reportMetricsはメトリクスを報告します
func (c *ContextControlController) reportMetrics() {
	c.stats.mu.RLock()
	defer c.stats.mu.RUnlock()

	submitted := atomic.LoadInt64(&c.stats.totalSubmitted)
	completed := atomic.LoadInt64(&c.stats.totalCompleted)
	success := atomic.LoadInt64(&c.stats.totalSuccess)
	errors := atomic.LoadInt64(&c.stats.totalErrors)
	timeouts := atomic.LoadInt64(&c.stats.totalTimeouts)
	cancellations := atomic.LoadInt64(&c.stats.totalCancellations)

	var successRate float64
	if completed > 0 {
		successRate = float64(success) / float64(completed) * 100
	}

	uptime := time.Since(c.stats.startTime)
	var throughput float64
	if uptime.Seconds() > 0 {
		throughput = float64(completed) / uptime.Seconds()
	}

	log.Printf("Controller Metrics: Submitted=%d, Completed=%d, Success=%.1f%%, Errors=%d, Timeouts=%d, Cancellations=%d",
		submitted, completed, successRate, errors, timeouts, cancellations)
	log.Printf("Performance: AvgTime=%v, Throughput=%.2f tasks/sec, Uptime=%v",
		c.stats.averageProcessTime, throughput, uptime)

	// ワーカー別統計
	for _, worker := range c.workers {
		processed := atomic.LoadInt64(&worker.stats.TasksProcessed)
		success := atomic.LoadInt64(&worker.stats.TasksSuccess)
		errors := atomic.LoadInt64(&worker.stats.TasksError)
		timeouts := atomic.LoadInt64(&worker.stats.TasksTimeout)
		cancelled := atomic.LoadInt64(&worker.stats.TasksCancelled)

		if processed > 0 {
			successRate := float64(success) / float64(processed) * 100
			log.Printf("Worker %d: Processed=%d, Success=%.1f%%, Errors=%d, Timeouts=%d, Cancelled=%d, AvgTime=%v",
				worker.ID, processed, successRate, errors, timeouts, cancelled, worker.stats.AverageProcessTime)
		}
	}
}

// CancelTaskはタスクをキャンセルします
func (c *ContextControlController) CancelTask(taskID int64) bool {
	return c.timeoutManager.cancelTask(taskID)
}

// GetCurrentTasksは現在実行中のタスクを取得します
func (c *ContextControlController) GetCurrentTasks() map[int]*Task {
	result := make(map[int]*Task)

	for _, worker := range c.workers {
		worker.mu.RLock()
		if worker.currentTask != nil {
			taskCopy := *worker.currentTask
			result[worker.ID] = &taskCopy
		}
		worker.mu.RUnlock()
	}

	return result
}

// Shutdownはコントローラーを停止します
func (c *ContextControlController) Shutdown(timeout time.Duration) error {
	if !atomic.CompareAndSwapInt32(&c.isRunning, 1, 0) {
		return fmt.Errorf("controller is not running")
	}

	log.Println("Shutting down context control controller...")

	// 1. 新しいタスクの受付を停止
	close(c.taskQueue)

	// 2. 現在実行中のタスクをキャンセル
	currentTasks := c.GetCurrentTasks()
	for workerID, task := range currentTasks {
		c.timeoutManager.cancelTask(task.ID)
		log.Printf("Cancelled task %d on worker %d", task.ID, workerID)
	}

	// 3. ワーカーの終了を待機
	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("All workers stopped gracefully")
	case <-time.After(timeout):
		log.Println("Timeout reached, forcing shutdown...")
		c.cancel()

		// 追加の待機時間
		select {
		case <-done:
			log.Println("Workers stopped after cancellation")
		case <-time.After(2 * time.Second):
			log.Println("Some workers may not have stopped properly")
		}
	}

	// 4. チャネルを閉じる
	close(c.resultQueue)

	log.Println("Context control controller shutdown completed")
	return nil
}

// GetStatsは統計情報を取得します
func (c *ContextControlController) GetStats() map[string]interface{} {
	c.stats.mu.RLock()
	defer c.stats.mu.RUnlock()

	submitted := atomic.LoadInt64(&c.stats.totalSubmitted)
	completed := atomic.LoadInt64(&c.stats.totalCompleted)
	success := atomic.LoadInt64(&c.stats.totalSuccess)
	errors := atomic.LoadInt64(&c.stats.totalErrors)
	timeouts := atomic.LoadInt64(&c.stats.totalTimeouts)
	cancellations := atomic.LoadInt64(&c.stats.totalCancellations)

	uptime := time.Since(c.stats.startTime)
	var throughput float64
	if uptime.Seconds() > 0 {
		throughput = float64(completed) / uptime.Seconds()
	}

	workerStats := make(map[string]interface{})
	for id, stats := range c.stats.workerStats {
		workerStats[fmt.Sprintf("worker_%d", id)] = map[string]interface{}{
			"tasks_processed":      atomic.LoadInt64(&stats.TasksProcessed),
			"tasks_success":        atomic.LoadInt64(&stats.TasksSuccess),
			"tasks_error":          atomic.LoadInt64(&stats.TasksError),
			"tasks_timeout":        atomic.LoadInt64(&stats.TasksTimeout),
			"tasks_cancelled":      atomic.LoadInt64(&stats.TasksCancelled),
			"average_process_time": stats.AverageProcessTime,
		}
	}

	return map[string]interface{}{
		"total_submitted":      submitted,
		"total_completed":      completed,
		"total_success":        success,
		"total_errors":         errors,
		"total_timeouts":       timeouts,
		"total_cancellations":  cancellations,
		"average_process_time": c.stats.averageProcessTime,
		"throughput_tasks_sec": throughput,
		"uptime":               uptime,
		"worker_count":         len(c.workers),
		"worker_stats":         workerStats,
	}
}

func main() {
	// コンテキスト制御設定
	config := NewTaskConfig()
	config.WorkerCount = 4
	config.QueueSize = 500
	config.DefaultTimeout = 5 * time.Second
	config.MaxTimeout = 30 * time.Second

	// コントローラー作成・開始
	controller := NewContextControlController(config)
	if err := controller.Start(); err != nil {
		log.Fatalf("Failed to start controller: %v", err)
	}

	// テストタスクを並行で送信
	go func() {
		for i := 0; i < 500; i++ {
			// タスクタイプを変更してテスト
			var timeout time.Duration
			switch i % 4 {
			case 0:
				timeout = 1 * time.Second // 短いタイムアウト
			case 1:
				timeout = 3 * time.Second // 中程度のタイムアウト
			case 2:
				timeout = 10 * time.Second // 長いタイムアウト
			default:
				timeout = 0 // デフォルトタイムアウト使用
			}

			task := Task{
				ID:       int64(i),
				Name:     fmt.Sprintf("task_%d", i),
				Data:     []byte(fmt.Sprintf("data_for_task_%d", i)),
				Priority: rand.Intn(5),
				Timeout:  timeout,
				Metadata: map[string]interface{}{
					"batch_id": i / 50,
					"type":     "test",
				},
			}

			if err := controller.SubmitTask(task); err != nil {
				log.Printf("Failed to submit task %d: %v", i, err)
				break
			}

			// いくつかのタスクは途中でキャンセル
			if i%20 == 0 && i > 0 {
				go func(taskID int64) {
					time.Sleep(2 * time.Second)
					if controller.CancelTask(taskID) {
						log.Printf("Cancelled task %d", taskID)
					}
				}(int64(i - 10))
			}

			time.Sleep(20 * time.Millisecond)
		}

		log.Println("All test tasks submitted")
	}()

	// 45秒間実行
	time.Sleep(45 * time.Second)

	// 現在実行中のタスクを表示
	currentTasks := controller.GetCurrentTasks()
	log.Printf("Currently running tasks: %d", len(currentTasks))
	for workerID, task := range currentTasks {
		log.Printf("Worker %d: Task %d (%s)", workerID, task.ID, task.Name)
	}

	// 最終統計表示
	stats := controller.GetStats()
	log.Printf("Final Stats: %+v", stats)

	// グレースフルシャットダウン
	if err := controller.Shutdown(10 * time.Second); err != nil {
		log.Printf("Shutdown error: %v", err)
	}
}
