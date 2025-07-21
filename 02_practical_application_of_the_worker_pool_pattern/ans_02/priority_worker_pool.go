package main

import (
	"container/heap"
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// PriorityTaskは優先度付きタスクです
type PriorityTask struct {
	ID        int
	Priority  int // 0が最高優先度
	Data      interface{}
	Execute   func(interface{}) error
	CreatedAt time.Time
	Urgent    bool // 緊急タスク（他を中断）
}

// PriorityTaskQueueは優先度付きタスクキューです
type PriorityTaskQueue struct {
	tasks  []*PriorityTask
	mu     sync.RWMutex
	closed bool
}

// PriorityWorkerPoolは優先度付きワーカープールです
type PriorityWorkerPool struct {
	// 基本設定
	workerCount int

	// キュー管理
	taskQueue   *PriorityTaskQueue
	urgentQueue chan PriorityTask
	resultQueue chan PriorityResult

	// ワーカー管理
	workers []*PriorityWorker

	// 制御
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// 統計
	stats *PriorityStats
}

// PriorityWorkerは優先度対応ワーカーです
type PriorityWorker struct {
	ID            int
	pool          *PriorityWorkerPool
	currentTask   *PriorityTask
	currentTaskMu sync.RWMutex
	interruptChan chan struct{}
}

// PriorityResultは実行結果です
type PriorityResult struct {
	TaskID      int
	Priority    int
	Success     bool
	Error       error
	Duration    time.Duration
	WorkerID    int
	Interrupted bool
}

// PriorityStatsは統計情報です
type PriorityStats struct {
	mu               sync.RWMutex
	totalTasks       int64
	completedTasks   int64
	interruptedTasks int64
	tasksByPriority  map[int]int64
	averageWaitTime  map[int]time.Duration
}

// NewPriorityTaskQueueは新しい優先度付きキューを作成します
func NewPriorityTaskQueue() *PriorityTaskQueue {
	q := &PriorityTaskQueue{
		tasks: make([]*PriorityTask, 0),
	}
	return q
}

// Lenはheap.Interfaceの実装です
func (pq *PriorityTaskQueue) Len() int {
	return len(pq.tasks)
}

// Lessはheap.Interfaceの実装です（優先度が低いほど、作成時間が古いほど優先）
func (pq *PriorityTaskQueue) Less(i, j int) bool {
	if pq.tasks[i].Priority != pq.tasks[j].Priority {
		return pq.tasks[i].Priority < pq.tasks[j].Priority
	}
	return pq.tasks[i].CreatedAt.Before(pq.tasks[j].CreatedAt)
}

// Swapはheap.Interfaceの実装です
func (pq *PriorityTaskQueue) Swap(i, j int) {
	pq.tasks[i], pq.tasks[j] = pq.tasks[j], pq.tasks[i]
}

// Pushはheap.Interfaceの実装です
func (pq *PriorityTaskQueue) Push(x interface{}) {
	pq.tasks = append(pq.tasks, x.(*PriorityTask))
}

// Popはheap.Interfaceの実装です
func (pq *PriorityTaskQueue) Pop() interface{} {
	old := pq.tasks
	n := len(old)
	task := old[n-1]
	pq.tasks = old[0 : n-1]
	return task
}

// Enqueue タスクをキューに追加します
func (pq *PriorityTaskQueue) Enqueue(task *PriorityTask) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	if pq.closed {
		return
	}

	heap.Push(pq, task)
}

// Dequeueはタスクをキューから取得します
func (pq *PriorityTaskQueue) Dequeue(ctx context.Context) (*PriorityTask, bool) {
	for {
		pq.mu.Lock()

		// キューにタスクがある場合は取得
		if pq.Len() > 0 {
			task := heap.Pop(pq).(*PriorityTask)
			pq.mu.Unlock()
			return task, true
		}

		// キューが閉じられている場合は終了
		if pq.closed {
			pq.mu.Unlock()
			return nil, false
		}

		pq.mu.Unlock()

		// 短時間待機してからリトライ
		select {
		case <-ctx.Done():
			return nil, false
		case <-time.After(10 * time.Millisecond):
			// 継続
		}
	}
}

// Closeはキューを閉じます
func (pq *PriorityTaskQueue) Close() {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	pq.closed = true
}

// Sizeは現在のキューサイズを返します
func (pq *PriorityTaskQueue) Size() int {
	pq.mu.RLock()
	defer pq.mu.RUnlock()
	return pq.Len()
}

// NewPriorityWorkerPoolは新しい優先度付きワーカープールを作成します
func NewPriorityWorkerPool(workerCount int) *PriorityWorkerPool {
	ctx, cancel := context.WithCancel(context.Background())

	return &PriorityWorkerPool{
		workerCount: workerCount,
		taskQueue:   NewPriorityTaskQueue(),
		urgentQueue: make(chan PriorityTask, 10), // 緊急タスク用
		resultQueue: make(chan PriorityResult, workerCount*2),
		workers:     make([]*PriorityWorker, workerCount),
		ctx:         ctx,
		cancel:      cancel,
		stats: &PriorityStats{
			tasksByPriority: make(map[int]int64),
			averageWaitTime: make(map[int]time.Duration),
		},
	}
}

// Startはワーカープールを開始します
func (pwp *PriorityWorkerPool) Start() error {
	// ワーカーを開始
	for i := 0; i < pwp.workerCount; i++ {
		worker := &PriorityWorker{
			ID:            i,
			pool:          pwp,
			interruptChan: make(chan struct{}, 1),
		}
		pwp.workers[i] = worker

		pwp.wg.Add(1)
		go worker.run()
	}

	// 結果処理を開始
	pwp.wg.Add(1)
	go pwp.resultHandler()

	// 統計レポートを開始
	pwp.wg.Add(1)
	go pwp.statsReporter()

	log.Printf("Priority worker pool started with %d workers", pwp.workerCount)
	return nil
}

// runはワーカーのメインループです
func (pw *PriorityWorker) run() {
	defer pw.pool.wg.Done()

	for {
		select {
		case <-pw.pool.ctx.Done():
			return

		// 緊急タスクを最優先で処理
		case urgentTask := <-pw.pool.urgentQueue:
			pw.handleUrgentTask(urgentTask)

		default:
			// 通常の優先度付きタスクを処理
			task, ok := pw.pool.taskQueue.Dequeue(pw.pool.ctx)
			if !ok {
				return
			}

			result := pw.executeTask(*task, false)

			select {
			case pw.pool.resultQueue <- result:
			case <-pw.pool.ctx.Done():
				return
			}
		}
	}
}

// handleUrgentTaskは緊急タスクを処理します
func (pw *PriorityWorker) handleUrgentTask(urgentTask PriorityTask) {
	// 現在のタスクを中断
	pw.currentTaskMu.Lock()
	if pw.currentTask != nil {
		log.Printf("Worker %d: Interrupting task %d for urgent task %d",
			pw.ID, pw.currentTask.ID, urgentTask.ID)

		// 中断シグナルを送信
		select {
		case pw.interruptChan <- struct{}{}:
		default:
		}
	}
	pw.currentTaskMu.Unlock()

	// 緊急タスクを実行
	result := pw.executeTask(urgentTask, true)

	select {
	case pw.pool.resultQueue <- result:
	case <-pw.pool.ctx.Done():
	}
}

// executeTaskはタスクを実行します
func (pw *PriorityWorker) executeTask(task PriorityTask, isUrgent bool) PriorityResult {
	start := time.Now()

	// 待機時間を計算
	waitTime := start.Sub(task.CreatedAt)

	result := PriorityResult{
		TaskID:   task.ID,
		Priority: task.Priority,
		WorkerID: pw.ID,
	}

	// 現在のタスクを設定
	pw.currentTaskMu.Lock()
	if !isUrgent {
		pw.currentTask = &task
	}
	pw.currentTaskMu.Unlock()

	// パニック回復
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Worker %d: Task %d panicked: %v", pw.ID, task.ID, r)
			result.Success = false
			result.Error = fmt.Errorf("task panicked: %v", r)
		}
		result.Duration = time.Since(start)

		// 現在のタスクをクリア
		if !isUrgent {
			pw.currentTaskMu.Lock()
			pw.currentTask = nil
			pw.currentTaskMu.Unlock()
		}

		// 統計を更新
		pw.pool.updateStats(result, waitTime)
	}()

	// タスクを実行（中断可能）
	done := make(chan error, 1)
	go func() {
		done <- task.Execute(task.Data)
	}()

	select {
	case err := <-done:
		if err != nil {
			result.Success = false
			result.Error = err
		} else {
			result.Success = true
		}
	case <-pw.interruptChan:
		result.Success = false
		result.Error = fmt.Errorf("task interrupted")
		result.Interrupted = true

		// 中断されたタスクを再キューイング（緊急タスクでない場合）
		if !isUrgent {
			go func() {
				select {
				case <-time.After(1 * time.Second): // 少し待ってから再キューイング
					pw.pool.taskQueue.Enqueue(&task)
				case <-pw.pool.ctx.Done():
				}
			}()
		}
	case <-pw.pool.ctx.Done():
		result.Success = false
		result.Error = fmt.Errorf("worker pool shutting down")
	}

	return result
}

// SubmitTaskはタスクを追加します
func (pwp *PriorityWorkerPool) SubmitTask(task PriorityTask) error {
	task.CreatedAt = time.Now()

	if task.Urgent {
		// 緊急タスクは専用キューに送信
		select {
		case pwp.urgentQueue <- task:
			atomic.AddInt64(&pwp.stats.totalTasks, 1)
			return nil
		case <-pwp.ctx.Done():
			return fmt.Errorf("pool is shutting down")
		default:
			return fmt.Errorf("urgent queue is full")
		}
	} else {
		// 通常タスクは優先度付きキューに送信
		pwp.taskQueue.Enqueue(&task)
		atomic.AddInt64(&pwp.stats.totalTasks, 1)
		return nil
	}
}

// updateStatsは統計を更新します
func (pwp *PriorityWorkerPool) updateStats(result PriorityResult, waitTime time.Duration) {
	pwp.stats.mu.Lock()
	defer pwp.stats.mu.Unlock()

	if result.Success {
		atomic.AddInt64(&pwp.stats.completedTasks, 1)
	}

	if result.Interrupted {
		atomic.AddInt64(&pwp.stats.interruptedTasks, 1)
	}

	// 優先度別統計を更新
	pwp.stats.tasksByPriority[result.Priority]++

	// 平均待機時間を更新
	if existing, ok := pwp.stats.averageWaitTime[result.Priority]; ok {
		count := pwp.stats.tasksByPriority[result.Priority]
		pwp.stats.averageWaitTime[result.Priority] = time.Duration(
			(int64(existing)*count + int64(waitTime)) / (count + 1))
	} else {
		pwp.stats.averageWaitTime[result.Priority] = waitTime
	}
}

// resultHandlerは結果を処理します
func (pwp *PriorityWorkerPool) resultHandler() {
	defer pwp.wg.Done()

	for {
		select {
		case <-pwp.ctx.Done():
			return
		case result := <-pwp.resultQueue:
			if !result.Success && !result.Interrupted {
				log.Printf("Task %d (priority %d) failed on worker %d: %v",
					result.TaskID, result.Priority, result.WorkerID, result.Error)
			} else if result.Interrupted {
				log.Printf("Task %d (priority %d) interrupted on worker %d",
					result.TaskID, result.Priority, result.WorkerID)
			}
		}
	}
}

// statsReporterは統計を定期的に報告します
func (pwp *PriorityWorkerPool) statsReporter() {
	defer pwp.wg.Done()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-pwp.ctx.Done():
			return
		case <-ticker.C:
			pwp.printStats()
		}
	}
}

// printStatsは統計を出力します
func (pwp *PriorityWorkerPool) printStats() {
	pwp.stats.mu.RLock()
	defer pwp.stats.mu.RUnlock()

	total := atomic.LoadInt64(&pwp.stats.totalTasks)
	completed := atomic.LoadInt64(&pwp.stats.completedTasks)
	interrupted := atomic.LoadInt64(&pwp.stats.interruptedTasks)
	queueSize := pwp.taskQueue.Size()
	urgentQueueSize := len(pwp.urgentQueue)

	log.Printf("Priority Pool Stats: Total=%d, Completed=%d, Interrupted=%d, Queue=%d, Urgent=%d",
		total, completed, interrupted, queueSize, urgentQueueSize)

	// 優先度別統計
	for priority, count := range pwp.stats.tasksByPriority {
		waitTime := pwp.stats.averageWaitTime[priority]
		log.Printf("  Priority %d: Count=%d, AvgWait=%v", priority, count, waitTime)
	}
}

// Shutdownはプールを停止します
func (pwp *PriorityWorkerPool) Shutdown(timeout time.Duration) error {
	log.Println("Starting priority worker pool shutdown...")

	pwp.taskQueue.Close()
	close(pwp.urgentQueue)
	pwp.cancel()

	done := make(chan struct{})
	go func() {
		pwp.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("Priority worker pool shutdown completed")
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("shutdown timeout exceeded")
	}
}

// GetStatsは統計を取得します
func (pwp *PriorityWorkerPool) GetStats() (int64, int64, int64, int) {
	total := atomic.LoadInt64(&pwp.stats.totalTasks)
	completed := atomic.LoadInt64(&pwp.stats.completedTasks)
	interrupted := atomic.LoadInt64(&pwp.stats.interruptedTasks)
	queueSize := pwp.taskQueue.Size()

	return total, completed, interrupted, queueSize
}

func main() {
	// 優先度付きワーカープールを作成
	pool := NewPriorityWorkerPool(3)

	if err := pool.Start(); err != nil {
		log.Fatalf("Failed to start pool: %v", err)
	}

	// 異なる優先度のタスクを送信
	go func() {
		taskID := 1

		// 低優先度タスクを大量に送信
		for i := 0; i < 20; i++ {
			task := PriorityTask{
				ID:       taskID,
				Priority: 3, // 低優先度
				Data:     fmt.Sprintf("low-priority-task-%d", taskID),
				Execute: func(data interface{}) error {
					time.Sleep(2 * time.Second) // 長時間実行
					log.Printf("Completed low priority task: %s", data.(string))
					return nil
				},
			}
			if err := pool.SubmitTask(task); err != nil {
				log.Printf("Failed to submit task: %v", err)
			}
			taskID++
			time.Sleep(100 * time.Millisecond)
		}
	}()

	// 5秒後に高優先度タスクを送信
	go func() {
		time.Sleep(5 * time.Second)

		for i := 0; i < 10; i++ {
			taskID := 100 + i
			task := PriorityTask{
				ID:       taskID,
				Priority: 1, // 高優先度
				Data:     fmt.Sprintf("high-priority-task-%d", taskID),
				Execute: func(data interface{}) error {
					time.Sleep(500 * time.Millisecond)
					log.Printf("Completed high priority task: %s", data.(string))
					return nil
				},
			}
			if err := pool.SubmitTask(task); err != nil {
				log.Printf("Failed to submit task: %v", err)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}()

	// 10秒後に緊急タスクを送信
	go func() {
		time.Sleep(10 * time.Second)

		for i := 0; i < 3; i++ {
			taskID := 200 + i
			task := PriorityTask{
				ID:       taskID,
				Priority: 0, // 最高優先度
				Data:     fmt.Sprintf("urgent-task-%d", taskID),
				Urgent:   true, // 緊急フラグ
				Execute: func(data interface{}) error {
					time.Sleep(300 * time.Millisecond)
					log.Printf("Completed URGENT task: %s", data.(string))
					return nil
				},
			}
			if err := pool.SubmitTask(task); err != nil {
				log.Printf("Failed to submit task: %v", err)
			}
			time.Sleep(500 * time.Millisecond)
		}
	}()

	// 60秒間動作させる
	time.Sleep(60 * time.Second)

	// 最終統計を表示
	total, completed, interrupted, queueSize := pool.GetStats()
	log.Printf("Final Results: Total=%d, Completed=%d, Interrupted=%d, Remaining=%d",
		total, completed, interrupted, queueSize)

	if err := pool.Shutdown(10 * time.Second); err != nil {
		log.Printf("Shutdown error: %v", err)
	}
}
