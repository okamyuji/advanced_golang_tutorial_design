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

// PriorityTask 優先度付きタスクです
type PriorityTask struct {
	ID        int
	Priority  int // 0が最高優先度
	Data      any
	Execute   func(any) error
	CreatedAt time.Time
	Urgent    bool // 緊急タスク（他を中断）
}

// PriorityTaskQueue 優先度付きタスクキューです
type PriorityTaskQueue struct {
	tasks  []*PriorityTask
	mu     sync.RWMutex
	cond   *sync.Cond // タスクの追加とCloseを待ち手に知らせる
	closed bool
}

// PriorityWorkerPool 優先度付きワーカープールです
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

// PriorityWorker 優先度対応ワーカーです
type PriorityWorker struct {
	ID            int
	pool          *PriorityWorkerPool
	currentTask   *PriorityTask
	currentTaskMu sync.RWMutex
	interruptChan chan struct{}
}

// PriorityResult 実行結果です
type PriorityResult struct {
	TaskID      int
	Priority    int
	Success     bool
	Error       error
	Duration    time.Duration
	WorkerID    int
	Interrupted bool
}

// PriorityStats 統計情報です
type PriorityStats struct {
	mu               sync.RWMutex
	totalTasks       atomic.Int64
	completedTasks   atomic.Int64
	interruptedTasks atomic.Int64
	tasksByPriority  map[int]int64
	averageWaitTime  map[int]time.Duration
}

// NewPriorityTaskQueue 新しい優先度付きキューを作成します
func NewPriorityTaskQueue() *PriorityTaskQueue {
	q := &PriorityTaskQueue{
		tasks: make([]*PriorityTask, 0),
	}
	q.cond = sync.NewCond(&q.mu)
	return q
}

// Len heap.Interfaceの実装です
func (pq *PriorityTaskQueue) Len() int {
	return len(pq.tasks)
}

// Less heap.Interfaceの実装です（優先度が低いほど、作成時間が古いほど優先）
func (pq *PriorityTaskQueue) Less(i, j int) bool {
	if pq.tasks[i].Priority != pq.tasks[j].Priority {
		return pq.tasks[i].Priority < pq.tasks[j].Priority
	}
	return pq.tasks[i].CreatedAt.Before(pq.tasks[j].CreatedAt)
}

// Swap heap.Interfaceの実装です
func (pq *PriorityTaskQueue) Swap(i, j int) {
	pq.tasks[i], pq.tasks[j] = pq.tasks[j], pq.tasks[i]
}

// Push heap.Interfaceの実装です
func (pq *PriorityTaskQueue) Push(x any) {
	pq.tasks = append(pq.tasks, x.(*PriorityTask))
}

// Pop heap.Interfaceの実装です
func (pq *PriorityTaskQueue) Pop() any {
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
	pq.cond.Signal()
}

// Dequeue タスクをキューから取得します
// タスクが届くか、キューが閉じられるか、ctxがキャンセルされるまで待ちます
func (pq *PriorityTaskQueue) Dequeue(ctx context.Context) (*PriorityTask, bool) {
	// sync.Cond.Wait ctxを直接待てないので、キャンセル時にBroadcastして起こす
	stop := context.AfterFunc(ctx, func() {
		pq.mu.Lock()
		defer pq.mu.Unlock()
		pq.cond.Broadcast()
	})
	defer stop()

	pq.mu.Lock()
	defer pq.mu.Unlock()
	for pq.Len() == 0 && !pq.closed && ctx.Err() == nil {
		pq.cond.Wait()
	}
	if pq.Len() > 0 {
		return heap.Pop(pq).(*PriorityTask), true
	}
	return nil, false
}

// Close キューを閉じます
func (pq *PriorityTaskQueue) Close() {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	pq.closed = true
	pq.cond.Broadcast()
}

// Size 現在のキューサイズを返します
func (pq *PriorityTaskQueue) Size() int {
	pq.mu.RLock()
	defer pq.mu.RUnlock()
	return pq.Len()
}

// NewPriorityWorkerPool 新しい優先度付きワーカープールを作成します
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

// Start ワーカープールを開始します
func (pwp *PriorityWorkerPool) Start() error {
	// ワーカーを開始
	for i := range pwp.workerCount {
		worker := &PriorityWorker{
			ID:            i,
			pool:          pwp,
			interruptChan: make(chan struct{}, 1),
		}
		pwp.workers[i] = worker

		pwp.wg.Go(worker.run)
	}

	// 結果処理を開始
	pwp.wg.Go(pwp.resultHandler)

	// 統計レポートを開始
	pwp.wg.Go(pwp.statsReporter)

	log.Printf("Priority worker pool started with %d workers", pwp.workerCount)
	return nil
}

// run ワーカーのメインループです
func (pw *PriorityWorker) run() {
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

// handleUrgentTask 緊急タスクを処理します
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

// executeTask タスクを実行します
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

// SubmitTask タスクを追加します
func (pwp *PriorityWorkerPool) SubmitTask(task PriorityTask) error {
	// 停止後の投入を拒否する。キューはcloseせず、読む側はctxのキャンセルで止める
	if pwp.ctx.Err() != nil {
		return fmt.Errorf("pool is shutting down")
	}
	task.CreatedAt = time.Now()

	if task.Urgent {
		// 緊急タスクは専用キューに送信
		select {
		case pwp.urgentQueue <- task:
			pwp.stats.totalTasks.Add(1)
			return nil
		case <-pwp.ctx.Done():
			return fmt.Errorf("pool is shutting down")
		default:
			return fmt.Errorf("urgent queue is full")
		}
	} else {
		// 通常タスクは優先度付きキューに送信
		pwp.taskQueue.Enqueue(&task)
		pwp.stats.totalTasks.Add(1)
		return nil
	}
}

// updateStats 統計を更新します
func (pwp *PriorityWorkerPool) updateStats(result PriorityResult, waitTime time.Duration) {
	pwp.stats.mu.Lock()
	defer pwp.stats.mu.Unlock()

	if result.Success {
		pwp.stats.completedTasks.Add(1)
	}

	if result.Interrupted {
		pwp.stats.interruptedTasks.Add(1)
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

// resultHandler 結果を処理します
func (pwp *PriorityWorkerPool) resultHandler() {
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

// statsReporter 統計を定期的に報告します
func (pwp *PriorityWorkerPool) statsReporter() {
	tick := time.Tick(10 * time.Second)

	for {
		select {
		case <-pwp.ctx.Done():
			return
		case <-tick:
			pwp.printStats()
		}
	}
}

// printStats 統計を出力します
func (pwp *PriorityWorkerPool) printStats() {
	pwp.stats.mu.RLock()
	defer pwp.stats.mu.RUnlock()

	total := pwp.stats.totalTasks.Load()
	completed := pwp.stats.completedTasks.Load()
	interrupted := pwp.stats.interruptedTasks.Load()
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

// Shutdown プールを停止します
func (pwp *PriorityWorkerPool) Shutdown(timeout time.Duration) error {
	log.Println("Starting priority worker pool shutdown...")

	pwp.taskQueue.Close()
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

// GetStats 統計を取得します
func (pwp *PriorityWorkerPool) GetStats() (int64, int64, int64, int) {
	total := pwp.stats.totalTasks.Load()
	completed := pwp.stats.completedTasks.Load()
	interrupted := pwp.stats.interruptedTasks.Load()
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
		for range 20 {
			task := PriorityTask{
				ID:       taskID,
				Priority: 3, // 低優先度
				Data:     fmt.Sprintf("low-priority-task-%d", taskID),
				Execute: func(data any) error {
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

		for i := range 10 {
			taskID := 100 + i
			task := PriorityTask{
				ID:       taskID,
				Priority: 1, // 高優先度
				Data:     fmt.Sprintf("high-priority-task-%d", taskID),
				Execute: func(data any) error {
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

		for i := range 3 {
			taskID := 200 + i
			task := PriorityTask{
				ID:       taskID,
				Priority: 0, // 最高優先度
				Data:     fmt.Sprintf("urgent-task-%d", taskID),
				Urgent:   true, // 緊急フラグ
				Execute: func(data any) error {
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
