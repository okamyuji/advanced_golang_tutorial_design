package main

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// DynamicWorkerPool 動的負荷制御機能付きワーカープールです
type DynamicWorkerPool struct {
	// 基本設定
	minWorkers    int
	maxWorkers    int
	taskQueueSize int

	// チャンネル
	taskQueue   chan DynamicTask
	resultQueue chan DynamicResult

	// ワーカー管理
	workers  map[int]*DynamicWorker
	workerID atomic.Int64
	mu       sync.RWMutex

	// 制御
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// 負荷監視
	loadMonitor *LoadMonitor

	// closed trueの後はtaskQueueへ送らない。muの書き込みロック中に切り替える
	closed bool

	// 設定
	config *DynamicConfig
}

// DynamicTask 動的プール用タスクです
type DynamicTask struct {
	ID           int
	Data         any
	Execute      func(any) error
	CPUIntensive bool // CPU集約的かどうか
}

// DynamicResult 実行結果です
type DynamicResult struct {
	TaskID   int
	Success  bool
	Error    error
	Duration time.Duration
	WorkerID int
}

// DynamicWorker 動的ワーカーです
type DynamicWorker struct {
	ID           int
	pool         *DynamicWorkerPool
	lastActivity atomic.Int64  // 最後にタスクを受け取った時刻（UnixNano）
	quit         chan struct{} // scaleDownがこのワーカーだけを止めるために閉じる
	taskCount    atomic.Int64
}

// LoadMonitor 負荷監視を行います
type LoadMonitor struct {
	mu             sync.RWMutex
	cpuUsage       float64
	lastCPUCheck   time.Time
	cpuHistory     []float64
	maxHistorySize int
}

// DynamicConfig 動的制御の設定です
type DynamicConfig struct {
	CPUHighThreshold   float64
	CPULowThreshold    float64
	QueueHighThreshold float64
	ScaleUpCooldown    time.Duration
	ScaleDownCooldown  time.Duration
	LoadCheckInterval  time.Duration
	WorkerIdleTimeout  time.Duration
}

// NewDynamicWorkerPool 新しい動的ワーカープールを作成します
func NewDynamicWorkerPool(minWorkers, maxWorkers, queueSize int) *DynamicWorkerPool {
	ctx, cancel := context.WithCancel(context.Background())

	return &DynamicWorkerPool{
		minWorkers:    minWorkers,
		maxWorkers:    maxWorkers,
		taskQueueSize: queueSize,
		taskQueue:     make(chan DynamicTask, queueSize),
		resultQueue:   make(chan DynamicResult, queueSize),
		workers:       make(map[int]*DynamicWorker),
		ctx:           ctx,
		cancel:        cancel,
		loadMonitor: &LoadMonitor{
			maxHistorySize: 10,
			cpuHistory:     make([]float64, 0, 10),
		},
		config: &DynamicConfig{
			CPUHighThreshold:   80.0,
			CPULowThreshold:    50.0,
			QueueHighThreshold: 80.0,
			ScaleUpCooldown:    30 * time.Second,
			ScaleDownCooldown:  60 * time.Second,
			LoadCheckInterval:  5 * time.Second,
			WorkerIdleTimeout:  120 * time.Second,
		},
	}
}

// Start 動的ワーカープールを開始します
func (dwp *DynamicWorkerPool) Start() error {
	// 最小数のワーカーを開始
	for range dwp.minWorkers {
		if err := dwp.addWorker(); err != nil {
			return fmt.Errorf("failed to start initial worker: %w", err)
		}
	}

	// 結果処理を開始
	dwp.wg.Go(dwp.resultHandler)

	// 負荷監視を開始
	dwp.wg.Go(func() { dwp.loadMonitor.monitor(dwp.ctx, dwp.config.LoadCheckInterval) })

	// 動的スケーリングを開始
	dwp.wg.Go(dwp.dynamicScaler)

	log.Printf("Dynamic worker pool started with %d workers", dwp.minWorkers)
	return nil
}

// addWorker 新しいワーカーを追加します
func (dwp *DynamicWorkerPool) addWorker() error {
	dwp.mu.Lock()
	defer dwp.mu.Unlock()

	if len(dwp.workers) >= dwp.maxWorkers {
		return fmt.Errorf("maximum workers limit reached")
	}

	workerID := int(dwp.workerID.Add(1))
	worker := &DynamicWorker{
		ID:   workerID,
		pool: dwp,
		quit: make(chan struct{}),
	}
	worker.lastActivity.Store(time.Now().UnixNano())

	dwp.workers[workerID] = worker

	dwp.wg.Go(worker.run)

	return nil
}

// removeWorker ワーカーを削除します
func (dwp *DynamicWorkerPool) removeWorker(workerID int) {
	dwp.mu.Lock()
	defer dwp.mu.Unlock()

	delete(dwp.workers, workerID)
}

// run ワーカーのメインループです
func (dw *DynamicWorker) run() {
	defer dw.pool.removeWorker(dw.ID)

	idleTimer := time.NewTimer(dw.pool.config.WorkerIdleTimeout)
	defer idleTimer.Stop()

	for {
		select {
		case <-dw.pool.ctx.Done():
			return
		case <-dw.quit:
			log.Printf("Worker %d stopped by scale down", dw.ID)
			return
		case <-idleTimer.C:
			// アイドルタイムアウト（最小数は維持）
			dw.pool.mu.RLock()
			currentWorkers := len(dw.pool.workers)
			dw.pool.mu.RUnlock()

			if currentWorkers > dw.pool.minWorkers {
				log.Printf("Worker %d terminating due to idle timeout", dw.ID)
				return
			}
			idleTimer.Reset(dw.pool.config.WorkerIdleTimeout)

		case task, ok := <-dw.pool.taskQueue:
			if !ok {
				return
			}
			dw.lastActivity.Store(time.Now().UnixNano())
			result := dw.executeTask(task)

			select {
			case dw.pool.resultQueue <- result:
			case <-dw.pool.ctx.Done():
				return
			}

			// Go 1.23以降はResetの前にStopとチャネルの排出をしなくても古い値は届かない
			idleTimer.Reset(dw.pool.config.WorkerIdleTimeout)
		}
	}
}

// executeTask タスクを実行します
func (dw *DynamicWorker) executeTask(task DynamicTask) DynamicResult {
	start := time.Now()
	dw.taskCount.Add(1)

	result := DynamicResult{
		TaskID:   task.ID,
		WorkerID: dw.ID,
	}

	// パニック回復
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Worker %d: Task %d panicked: %v", dw.ID, task.ID, r)
			result.Success = false
			result.Error = fmt.Errorf("task panicked: %v", r)
		}
		result.Duration = time.Since(start)
	}()

	// タスクを実行
	if err := task.Execute(task.Data); err != nil {
		result.Success = false
		result.Error = err
	} else {
		result.Success = true
	}

	return result
}

// monitor 負荷を監視します
func (lm *LoadMonitor) monitor(ctx context.Context, interval time.Duration) {
	tick := time.Tick(interval)

	for {
		select {
		case <-ctx.Done():
			return
		case <-tick:
			lm.updateCPUUsage()
		}
	}
}

// updateCPUUsage CPU使用率を更新します
func (lm *LoadMonitor) updateCPUUsage() {
	// 簡易CPU使用率計算（実際の実装では/proc/statやruntime/metricsを使用）
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	// Goroutine数とメモリ使用量からCPU使用率を推定
	goroutines := float64(runtime.NumGoroutine())
	cpuCores := float64(runtime.NumCPU())
	estimatedCPU := min((goroutines/cpuCores)*10.0, 100.0) // 簡易推定

	lm.mu.Lock()
	defer lm.mu.Unlock()

	lm.cpuUsage = estimatedCPU
	lm.lastCPUCheck = time.Now()

	// 履歴を更新
	lm.cpuHistory = append(lm.cpuHistory, estimatedCPU)
	if len(lm.cpuHistory) > lm.maxHistorySize {
		lm.cpuHistory = lm.cpuHistory[1:]
	}
}

// getCPUUsage 現在のCPU使用率を取得します
func (lm *LoadMonitor) getCPUUsage() float64 {
	lm.mu.RLock()
	defer lm.mu.RUnlock()
	return lm.cpuUsage
}

// getAverageCPUUsage 平均CPU使用率を取得します
func (lm *LoadMonitor) getAverageCPUUsage() float64 {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	if len(lm.cpuHistory) == 0 {
		return 0.0
	}

	sum := 0.0
	for _, cpu := range lm.cpuHistory {
		sum += cpu
	}
	return sum / float64(len(lm.cpuHistory))
}

// dynamicScaler 動的スケーリングを実行します
func (dwp *DynamicWorkerPool) dynamicScaler() {
	tick := time.Tick(dwp.config.LoadCheckInterval)

	lastScaleUp := time.Time{}
	lastScaleDown := time.Time{}

	for {
		select {
		case <-dwp.ctx.Done():
			return
		case <-tick:
			dwp.evaluateScaling(&lastScaleUp, &lastScaleDown)
		}
	}
}

// evaluateScaling スケーリングの必要性を評価します
func (dwp *DynamicWorkerPool) evaluateScaling(lastScaleUp, lastScaleDown *time.Time) {
	// 現在の状態を取得
	queueLength := len(dwp.taskQueue)
	queueUtil := float64(queueLength) / float64(dwp.taskQueueSize) * 100.0
	cpuUsage := dwp.loadMonitor.getCPUUsage()
	avgCPUUsage := dwp.loadMonitor.getAverageCPUUsage()

	dwp.mu.RLock()
	currentWorkers := len(dwp.workers)
	dwp.mu.RUnlock()

	now := time.Now()

	// スケールダウン判定
	if avgCPUUsage > dwp.config.CPUHighThreshold &&
		currentWorkers > dwp.minWorkers &&
		now.Sub(*lastScaleDown) > dwp.config.ScaleDownCooldown {

		dwp.scaleDown()
		*lastScaleDown = now
		log.Printf("Scaled down: %d -> %d workers (CPU: %.1f%%, Queue: %.1f%%)",
			currentWorkers, currentWorkers-1, avgCPUUsage, queueUtil)
	}

	// スケールアップ判定
	if queueUtil > dwp.config.QueueHighThreshold &&
		avgCPUUsage < dwp.config.CPULowThreshold &&
		currentWorkers < dwp.maxWorkers &&
		now.Sub(*lastScaleUp) > dwp.config.ScaleUpCooldown {

		if err := dwp.addWorker(); err == nil {
			*lastScaleUp = now
			log.Printf("Scaled up: %d -> %d workers (CPU: %.1f%%, Queue: %.1f%%)",
				currentWorkers, currentWorkers+1, avgCPUUsage, queueUtil)
		}
	}

	// 現在の状態をログ出力
	log.Printf("Load Stats: Workers=%d, CPU=%.1f%% (avg: %.1f%%), Queue=%d/%d (%.1f%%)",
		currentWorkers, cpuUsage, avgCPUUsage, queueLength, dwp.taskQueueSize, queueUtil)
}

// scaleDown ワーカーを削減します
// 最も長くタスクを受け取っていないワーカーを1つ止めます
func (dwp *DynamicWorkerPool) scaleDown() {
	dwp.mu.Lock()
	defer dwp.mu.Unlock()

	if len(dwp.workers) <= dwp.minWorkers {
		return
	}
	var oldest *DynamicWorker
	for _, w := range dwp.workers {
		if oldest == nil || w.lastActivity.Load() < oldest.lastActivity.Load() {
			oldest = w
		}
	}
	// 先にマップから外すので、同じワーカーのquitを二重に閉じることはない
	delete(dwp.workers, oldest.ID)
	close(oldest.quit)
}

// SubmitTask タスクを追加します
func (dwp *DynamicWorkerPool) SubmitTask(task DynamicTask) error {
	// 送信が終わるまで読み取りロックを持ち、Shutdownのclose(taskQueue)と重ならないようにする
	dwp.mu.RLock()
	defer dwp.mu.RUnlock()
	if dwp.closed {
		return fmt.Errorf("pool is shutting down")
	}

	select {
	case dwp.taskQueue <- task:
		return nil
	case <-dwp.ctx.Done():
		return fmt.Errorf("pool is shutting down")
	default:
		return fmt.Errorf("task queue is full")
	}
}

// resultHandler 結果を処理します
func (dwp *DynamicWorkerPool) resultHandler() {
	for {
		select {
		case <-dwp.ctx.Done():
			return
		case result := <-dwp.resultQueue:
			if !result.Success {
				log.Printf("Task %d failed on worker %d: %v",
					result.TaskID, result.WorkerID, result.Error)
			}
		}
	}
}

// Shutdown プールを停止します
func (dwp *DynamicWorkerPool) Shutdown(timeout time.Duration) error {
	log.Println("Starting dynamic worker pool shutdown...")

	dwp.mu.Lock()
	if !dwp.closed {
		dwp.closed = true
		close(dwp.taskQueue)
	}
	dwp.mu.Unlock()
	dwp.cancel()

	done := make(chan struct{})
	go func() {
		dwp.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("Dynamic worker pool shutdown completed")
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("shutdown timeout exceeded")
	}
}

// GetStats 統計を取得します
func (dwp *DynamicWorkerPool) GetStats() (int, float64, float64) {
	dwp.mu.RLock()
	workerCount := len(dwp.workers)
	dwp.mu.RUnlock()

	queueLength := len(dwp.taskQueue)
	queueUtil := float64(queueLength) / float64(dwp.taskQueueSize) * 100.0
	cpuUsage := dwp.loadMonitor.getCPUUsage()

	return workerCount, queueUtil, cpuUsage
}

func main() {
	// 動的ワーカープールを作成
	pool := NewDynamicWorkerPool(2, 8, 20)

	if err := pool.Start(); err != nil {
		log.Fatalf("Failed to start pool: %v", err)
	}

	// 負荷パターンを変化させるタスクを送信
	go func() {
		// 軽い負荷から開始
		for n := range 10 {
			i := n + 1
			task := DynamicTask{
				ID:   i,
				Data: fmt.Sprintf("light-task-%d", i),
				Execute: func(data any) error {
					time.Sleep(100 * time.Millisecond)
					return nil
				},
				CPUIntensive: false,
			}
			if err := pool.SubmitTask(task); err != nil {
				log.Printf("Failed to submit task: %v", err)
			}
			time.Sleep(200 * time.Millisecond)
		}

		// 重い負荷に切り替え
		for n := range 20 {
			i := 11 + n
			task := DynamicTask{
				ID:   i,
				Data: fmt.Sprintf("heavy-task-%d", i),
				Execute: func(data any) error {
					// CPU集約的処理をシミュレート
					end := time.Now().Add(500 * time.Millisecond)
					for time.Now().Before(end) {
						runtime.Gosched()
					}
					return nil
				},
				CPUIntensive: true,
			}
			if err := pool.SubmitTask(task); err != nil {
				log.Printf("Failed to submit task: %v", err)
			}
			time.Sleep(50 * time.Millisecond)
		}

		// 軽い負荷に戻す
		for n := range 10 {
			i := 31 + n
			task := DynamicTask{
				ID:   i,
				Data: fmt.Sprintf("cool-down-task-%d", i),
				Execute: func(data any) error {
					time.Sleep(50 * time.Millisecond)
					return nil
				},
				CPUIntensive: false,
			}
			if err := pool.SubmitTask(task); err != nil {
				log.Printf("Failed to submit task: %v", err)
			}
			time.Sleep(300 * time.Millisecond)
		}
	}()

	// 統計を定期的に出力
	go func() {
		for range time.Tick(10 * time.Second) {
			workers, queueUtil, cpuUsage := pool.GetStats()
			log.Printf("Current Stats: Workers=%d, Queue=%.1f%%, CPU=%.1f%%",
				workers, queueUtil, cpuUsage)
		}
	}()

	// 120秒間動作させる
	time.Sleep(120 * time.Second)

	if err := pool.Shutdown(10 * time.Second); err != nil {
		log.Printf("Shutdown error: %v", err)
	}
}
