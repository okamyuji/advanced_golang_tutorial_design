package main

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"
)

// ContextType コンテキストタイプです
type ContextType string

const (
	ContextTypeTimeout  ContextType = "timeout"
	ContextTypeCancel   ContextType = "cancel"
	ContextTypeDeadline ContextType = "deadline"
	ContextTypeValue    ContextType = "value"
)

// Task タスクデータです
type Task struct {
	ID         int64
	Name       string
	Data       []byte
	Priority   int
	Timeout    time.Duration
	Retries    int
	MaxRetries int
	CreatedAt  time.Time
	Metadata   map[string]any
}

// TaskResult タスク実行結果です
type TaskResult struct {
	TaskID      int64
	Success     bool
	Result      any
	Error       error
	Duration    time.Duration
	WorkerID    int
	ContextType ContextType
	RetryCount  int
	CompletedAt time.Time
}

// ContextControlController コンテキスト制御管理者です
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
	isRunning atomic.Bool
	startTime time.Time

	// タイムアウト制御
	defaultTimeout time.Duration
	timeoutManager *TimeoutManager
}

// ContextWorker コンテキスト制御ワーカーです
type ContextWorker struct {
	ID          int
	controller  *ContextControlController
	currentTask *Task
	mu          sync.RWMutex
	stats       *WorkerStats
	processor   *TaskProcessor
}

// TaskProcessor タスク処理エンジンです
type TaskProcessor struct {
	ID             int
	processedCount atomic.Int64
	errorCount     atomic.Int64
	timeoutCount   atomic.Int64
	cancelledCount atomic.Int64
}

// ControllerStats コントローラー統計です
type ControllerStats struct {
	mu                 sync.RWMutex
	totalSubmitted     atomic.Int64
	totalCompleted     atomic.Int64
	totalSuccess       atomic.Int64
	totalErrors        atomic.Int64
	totalTimeouts      atomic.Int64
	totalCancellations atomic.Int64
	averageProcessTime time.Duration
	startTime          time.Time
	workerStats        map[int]*WorkerStats
}

// WorkerStats ワーカー統計です。ワーカーgoroutine（書き込み）と監視・API側
// （読み取り）の両方から参照されるため、型付きatomicで保持します。
type WorkerStats struct {
	WorkerID         int
	TasksProcessed   atomic.Int64
	TasksSuccess     atomic.Int64
	TasksError       atomic.Int64
	TasksTimeout     atomic.Int64
	TasksCancelled   atomic.Int64
	TotalProcessTime atomic.Int64 // 累積処理時間（ナノ秒）
}

// AverageProcessTime 現時点の平均処理時間を計算します
func (s *WorkerStats) AverageProcessTime() time.Duration {
	processed := s.TasksProcessed.Load()
	if processed == 0 {
		return 0
	}
	return time.Duration(s.TotalProcessTime.Load()) / time.Duration(processed)
}

// TimeoutManager タイムアウト管理です
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

// NewTaskConfig デフォルトタスク設定を作成します
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

// NewTimeoutManager 新しいタイムアウトマネージャーを作成します
func NewTimeoutManager(defaultTimeout, maxTimeout time.Duration) *TimeoutManager {
	return &TimeoutManager{
		activeTimeouts: make(map[int64]context.CancelFunc),
		defaultTimeout: defaultTimeout,
		maxTimeout:     maxTimeout,
	}
}

// NewContextControlController 新しいコンテキスト制御コントローラーを作成します
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
	for i := range config.WorkerCount {
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

// Start コントローラーを開始します
func (c *ContextControlController) Start() error {
	if !c.isRunning.CompareAndSwap(false, true) {
		return fmt.Errorf("controller is already running")
	}

	c.startTime = time.Now()
	c.stats.startTime = c.startTime

	log.Printf("Starting context control controller with %d workers", c.workerCount)

	// ワーカーを開始
	for _, worker := range c.workers {
		c.wg.Go(worker.run)
	}

	// 結果ハンドラーを開始
	c.wg.Go(c.handleResults)

	// メトリクス監視を開始
	c.wg.Go(c.monitorMetrics)

	// タイムアウト後片付けを開始（rootCtxのキャンセルで終了させる）
	c.wg.Go(func() {
		c.timeoutManager.cleanup(c.rootCtx)
	})

	log.Printf("Context control controller started successfully")
	return nil
}

// run ワーカーのメインループです
func (w *ContextWorker) run() {
	log.Printf("Context worker %d started", w.ID)

	for {
		select {
		case <-w.controller.rootCtx.Done():
			log.Printf("Worker %d stopping due to root context cancellation", w.ID)
			return
		case task := <-w.controller.taskQueue:
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

// processTaskWithContext コンテキスト付きでタスクを処理します
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

	// タスク固有のタイムアウトを決定（未指定なら既定値）
	timeout := cmp.Or(task.Timeout, w.controller.defaultTimeout)

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
	switch {
	case result.Error == nil:
		w.stats.TasksSuccess.Add(1)
	case errors.Is(result.Error, context.DeadlineExceeded):
		result.ContextType = ContextTypeTimeout
		w.stats.TasksTimeout.Add(1)
		w.processor.timeoutCount.Add(1)
	case errors.Is(result.Error, context.Canceled):
		result.ContextType = ContextTypeCancel
		w.stats.TasksCancelled.Add(1)
		w.processor.cancelledCount.Add(1)
	default:
		w.stats.TasksError.Add(1)
		w.processor.errorCount.Add(1)
	}

	// 統計更新
	w.stats.TasksProcessed.Add(1)
	w.processor.processedCount.Add(1)
	w.stats.TotalProcessTime.Add(int64(result.Duration))

	return result
}

// TaskProcessingResult タスク処理結果です
type TaskProcessingResult struct {
	Success bool
	Result  any
	Error   error
}

// executeTaskWithContext コンテキスト付きでタスクを実行します
func (w *ContextWorker) executeTaskWithContext(ctx context.Context, task Task) TaskProcessingResult {
	// 実際の処理をシミュレート
	processingTime := 100*time.Millisecond + rand.N(1000*time.Millisecond)

	// 長時間処理をシミュレートするために、短い間隔でコンテキストをチェック
	tick := time.Tick(10 * time.Millisecond)

	startTime := time.Now()

	for {
		select {
		case <-ctx.Done():
			// コンテキストがキャンセルまたはタイムアウト
			return TaskProcessingResult{
				Success: false,
				Error:   ctx.Err(),
			}
		case <-tick:
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

// SubmitTask タスクをキューに追加します
func (c *ContextControlController) SubmitTask(task Task) error {
	if !c.isRunning.Load() {
		return fmt.Errorf("controller is not running")
	}

	// デフォルト値を設定
	if task.CreatedAt.IsZero() {
		task.CreatedAt = time.Now()
	}
	task.Timeout = cmp.Or(task.Timeout, c.defaultTimeout)

	// 停止後の投入を拒否する。taskQueueはcloseせず、読む側はrootCtxのキャンセルで止める
	// （closeするとこの送信と競合してpanic: send on closed channelになる）。
	if c.rootCtx.Err() != nil {
		return fmt.Errorf("controller is shutting down")
	}

	select {
	case c.taskQueue <- task:
		c.stats.totalSubmitted.Add(1)
		return nil
	case <-c.rootCtx.Done():
		return fmt.Errorf("controller is shutting down")
	default:
		return fmt.Errorf("task queue is full")
	}
}

// SubmitTaskWithTimeout タイムアウト付きでタスクを追加します
func (c *ContextControlController) SubmitTaskWithTimeout(task Task, submitTimeout time.Duration) error {
	if c.rootCtx.Err() != nil {
		return fmt.Errorf("controller is shutting down")
	}

	ctx, cancel := context.WithTimeout(c.rootCtx, submitTimeout)
	defer cancel()

	select {
	case c.taskQueue <- task:
		c.stats.totalSubmitted.Add(1)
		return nil
	case <-ctx.Done():
		return fmt.Errorf("submit timeout: %w", ctx.Err())
	}
}

// handleResults 結果を処理します
func (c *ContextControlController) handleResults() {
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
			c.stats.totalCompleted.Add(1)

			if result.Success {
				c.stats.totalSuccess.Add(1)
			} else {
				switch result.ContextType {
				case ContextTypeTimeout:
					c.stats.totalTimeouts.Add(1)
				case ContextTypeCancel:
					c.stats.totalCancellations.Add(1)
				default:
					c.stats.totalErrors.Add(1)
				}
			}

			// 平均処理時間を更新
			completed := c.stats.totalCompleted.Load()
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

// registerTask タスクのタイムアウトを登録します
func (tm *TimeoutManager) registerTask(taskID int64, cancel context.CancelFunc) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.activeTimeouts[taskID] = cancel
}

// unregisterTask タスクのタイムアウトを登録解除します
func (tm *TimeoutManager) unregisterTask(taskID int64) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	delete(tm.activeTimeouts, taskID)
}

// cancelTask タスクをキャンセルします
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

// cleanup 期限切れタイムアウトを後片付けします。ctxがキャンセルされたら終了します。
func (tm *TimeoutManager) cleanup(ctx context.Context) {
	tick := time.Tick(30 * time.Second)

	for {
		select {
		case <-ctx.Done():
			return
		case <-tick:
			tm.mu.Lock()
			activeCount := len(tm.activeTimeouts)
			tm.mu.Unlock()

			if activeCount > 0 {
				log.Printf("TimeoutManager: %d active timeouts", activeCount)
			}
		}
	}
}

// monitorMetrics メトリクスを監視します
func (c *ContextControlController) monitorMetrics() {
	tick := time.Tick(15 * time.Second)

	log.Println("Metrics monitor started")

	for {
		select {
		case <-c.rootCtx.Done():
			log.Println("Metrics monitor stopping")
			return
		case <-tick:
			c.reportMetrics()
		}
	}
}

// reportMetrics メトリクスを報告します
func (c *ContextControlController) reportMetrics() {
	c.stats.mu.RLock()
	defer c.stats.mu.RUnlock()

	submitted := c.stats.totalSubmitted.Load()
	completed := c.stats.totalCompleted.Load()
	success := c.stats.totalSuccess.Load()
	errorCount := c.stats.totalErrors.Load()
	timeouts := c.stats.totalTimeouts.Load()
	cancellations := c.stats.totalCancellations.Load()

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
		submitted, completed, successRate, errorCount, timeouts, cancellations)
	log.Printf("Performance: AvgTime=%v, Throughput=%.2f tasks/sec, Uptime=%v",
		c.stats.averageProcessTime, throughput, uptime)

	// ワーカー別統計
	for _, worker := range c.workers {
		processed := worker.stats.TasksProcessed.Load()
		success := worker.stats.TasksSuccess.Load()
		errs := worker.stats.TasksError.Load()
		timeouts := worker.stats.TasksTimeout.Load()
		cancelled := worker.stats.TasksCancelled.Load()

		if processed > 0 {
			successRate := float64(success) / float64(processed) * 100
			log.Printf("Worker %d: Processed=%d, Success=%.1f%%, Errors=%d, Timeouts=%d, Cancelled=%d, AvgTime=%v",
				worker.ID, processed, successRate, errs, timeouts, cancelled, worker.stats.AverageProcessTime())
		}
	}
}

// CancelTask タスクをキャンセルします
func (c *ContextControlController) CancelTask(taskID int64) bool {
	return c.timeoutManager.cancelTask(taskID)
}

// GetCurrentTasks 現在実行中のタスクを取得します
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

// Shutdown コントローラーを停止します
func (c *ContextControlController) Shutdown(timeout time.Duration) error {
	if !c.isRunning.CompareAndSwap(true, false) {
		return fmt.Errorf("controller is not running")
	}

	log.Println("Shutting down context control controller...")

	// ワーカーに停止を指示する。taskQueueはcloseしない
	// （SubmitTask/SubmitTaskWithTimeoutとの競合でpanic: send on closed channelになるため）。
	// rootCtxのキャンセルは実行中タスクの派生コンテキストにも伝播するので、
	// 個別タスクのキャンセルは不要。
	c.cancel()

	// ワーカーの終了を待機
	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("All workers stopped gracefully")
	case <-time.After(timeout):
		// 送信側のgoroutineが残っている可能性があるので、チャネルは閉じずにエラーを返す
		return fmt.Errorf("shutdown timeout: some workers may not have stopped")
	}

	// resultQueueへの送信元（ワーカー）はwg.Waitの完了で全て止まっているのでcloseしてよい
	close(c.resultQueue)

	log.Println("Context control controller shutdown completed")
	return nil
}

// GetStats 統計情報を取得します
func (c *ContextControlController) GetStats() map[string]any {
	c.stats.mu.RLock()
	defer c.stats.mu.RUnlock()

	submitted := c.stats.totalSubmitted.Load()
	completed := c.stats.totalCompleted.Load()
	success := c.stats.totalSuccess.Load()
	errorCount := c.stats.totalErrors.Load()
	timeouts := c.stats.totalTimeouts.Load()
	cancellations := c.stats.totalCancellations.Load()

	uptime := time.Since(c.stats.startTime)
	var throughput float64
	if uptime.Seconds() > 0 {
		throughput = float64(completed) / uptime.Seconds()
	}

	workerStats := make(map[string]any)
	for id, stats := range c.stats.workerStats {
		workerStats[fmt.Sprintf("worker_%d", id)] = map[string]any{
			"tasks_processed":      stats.TasksProcessed.Load(),
			"tasks_success":        stats.TasksSuccess.Load(),
			"tasks_error":          stats.TasksError.Load(),
			"tasks_timeout":        stats.TasksTimeout.Load(),
			"tasks_cancelled":      stats.TasksCancelled.Load(),
			"average_process_time": stats.AverageProcessTime(),
		}
	}

	return map[string]any{
		"total_submitted":      submitted,
		"total_completed":      completed,
		"total_success":        success,
		"total_errors":         errorCount,
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
		for i := range 500 {
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
				Priority: rand.IntN(5),
				Timeout:  timeout,
				Metadata: map[string]any{
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
