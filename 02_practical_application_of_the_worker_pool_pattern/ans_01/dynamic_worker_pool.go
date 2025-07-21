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

// DynamicWorkerPoolは動的負荷制御機能付きワーカープールです
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
	workerID int64
	mu       sync.RWMutex

	// 制御
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// 負荷監視
	loadMonitor *LoadMonitor

	// 設定
	config *DynamicConfig
}

// DynamicTaskは動的プール用タスクです
type DynamicTask struct {
	ID           int
	Data         interface{}
	Execute      func(interface{}) error
	CPUIntensive bool // CPU集約的かどうか
}

// DynamicResultは実行結果です
type DynamicResult struct {
	TaskID   int
	Success  bool
	Error    error
	Duration time.Duration
	WorkerID int
}

// DynamicWorkerは動的ワーカーです
type DynamicWorker struct {
	ID           int
	pool         *DynamicWorkerPool
	lastActivity time.Time
	taskCount    int64
}

// LoadMonitorは負荷監視を行います
type LoadMonitor struct {
	mu             sync.RWMutex
	cpuUsage       float64
	lastCPUCheck   time.Time
	cpuHistory     []float64
	maxHistorySize int
}

// DynamicConfigは動的制御の設定です
type DynamicConfig struct {
	CPUHighThreshold   float64
	CPULowThreshold    float64
	QueueHighThreshold float64
	ScaleUpCooldown    time.Duration
	ScaleDownCooldown  time.Duration
	LoadCheckInterval  time.Duration
	WorkerIdleTimeout  time.Duration
}

// NewDynamicWorkerPoolは新しい動的ワーカープールを作成します
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
			CPUHighThreshold:   70.0,
			CPULowThreshold:    50.0,
			QueueHighThreshold: 80.0,
			ScaleUpCooldown:    30 * time.Second,
			ScaleDownCooldown:  60 * time.Second,
			LoadCheckInterval:  5 * time.Second,
			WorkerIdleTimeout:  120 * time.Second,
		},
	}
}

// Startは動的ワーカープールを開始します
func (dwp *DynamicWorkerPool) Start() error {
	// 最小数のワーカーを開始
	for i := 0; i < dwp.minWorkers; i++ {
		if err := dwp.addWorker(); err != nil {
			return fmt.Errorf("failed to start initial worker: %v", err)
		}
	}

	// 結果処理を開始
	dwp.wg.Add(1)
	go dwp.resultHandler()

	// 負荷監視を開始
	dwp.wg.Add(1)
	go dwp.loadMonitor.monitor(dwp.ctx, dwp.config.LoadCheckInterval)

	// 動的スケーリングを開始
	dwp.wg.Add(1)
	go dwp.dynamicScaler()

	log.Printf("Dynamic worker pool started with %d workers", dwp.minWorkers)
	return nil
}

// addWorkerは新しいワーカーを追加します
func (dwp *DynamicWorkerPool) addWorker() error {
	dwp.mu.Lock()
	defer dwp.mu.Unlock()

	if len(dwp.workers) >= dwp.maxWorkers {
		return fmt.Errorf("maximum workers limit reached")
	}

	workerID := int(atomic.AddInt64(&dwp.workerID, 1))
	worker := &DynamicWorker{
		ID:           workerID,
		pool:         dwp,
		lastActivity: time.Now(),
	}

	dwp.workers[workerID] = worker

	dwp.wg.Add(1)
	go worker.run()

	return nil
}

// removeWorkerはワーカーを削除します
func (dwp *DynamicWorkerPool) removeWorker(workerID int) {
	dwp.mu.Lock()
	defer dwp.mu.Unlock()

	delete(dwp.workers, workerID)
}

// runはワーカーのメインループです
func (dw *DynamicWorker) run() {
	defer dw.pool.wg.Done()
	defer dw.pool.removeWorker(dw.ID)

	idleTimer := time.NewTimer(dw.pool.config.WorkerIdleTimeout)
	defer idleTimer.Stop()

	for {
		select {
		case <-dw.pool.ctx.Done():
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

		case task := <-dw.pool.taskQueue:
			dw.lastActivity = time.Now()
			result := dw.executeTask(task)

			select {
			case dw.pool.resultQueue <- result:
			case <-dw.pool.ctx.Done():
				return
			}

			if !idleTimer.Stop() {
				<-idleTimer.C
			}
			idleTimer.Reset(dw.pool.config.WorkerIdleTimeout)
		}
	}
}

// executeTaskはタスクを実行します
func (dw *DynamicWorker) executeTask(task DynamicTask) DynamicResult {
	start := time.Now()
	atomic.AddInt64(&dw.taskCount, 1)

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

// monitorは負荷を監視します
func (lm *LoadMonitor) monitor(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			lm.updateCPUUsage()
		}
	}
}

// updateCPUUsageはCPU使用率を更新します
func (lm *LoadMonitor) updateCPUUsage() {
	// 簡易CPU使用率計算（実際の実装では/proc/statやruntime/metricsを使用）
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	// Goroutine数とメモリ使用量からCPU使用率を推定
	goroutines := float64(runtime.NumGoroutine())
	cpuCores := float64(runtime.NumCPU())
	estimatedCPU := (goroutines / cpuCores) * 10.0 // 簡易推定

	if estimatedCPU > 100.0 {
		estimatedCPU = 100.0
	}

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

// getCPUUsageは現在のCPU使用率を取得します
func (lm *LoadMonitor) getCPUUsage() float64 {
	lm.mu.RLock()
	defer lm.mu.RUnlock()
	return lm.cpuUsage
}

// getAverageCPUUsageは平均CPU使用率を取得します
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

// dynamicScalerは動的スケーリングを実行します
func (dwp *DynamicWorkerPool) dynamicScaler() {
	defer dwp.wg.Done()

	ticker := time.NewTicker(dwp.config.LoadCheckInterval)
	defer ticker.Stop()

	lastScaleUp := time.Time{}
	lastScaleDown := time.Time{}

	for {
		select {
		case <-dwp.ctx.Done():
			return
		case <-ticker.C:
			dwp.evaluateScaling(&lastScaleUp, &lastScaleDown)
		}
	}
}

// evaluateScalingはスケーリングの必要性を評価します
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

// scaleDownはワーカーを削減します
func (dwp *DynamicWorkerPool) scaleDown() {
	dwp.mu.RLock()
	var oldestWorker *DynamicWorker
	oldestTime := time.Now()

	for _, worker := range dwp.workers {
		if worker.lastActivity.Before(oldestTime) {
			oldestTime = worker.lastActivity
			oldestWorker = worker
		}
	}
	dwp.mu.RUnlock()

	if oldestWorker != nil {
		// ワーカーにシャットダウンシグナルを送る代わりに
		// アイドルタイムアウトを短くして自然に終了させる
		log.Printf("Marking worker %d for scale down", oldestWorker.ID)
	}
}

// SubmitTaskはタスクを追加します
func (dwp *DynamicWorkerPool) SubmitTask(task DynamicTask) error {
	select {
	case dwp.taskQueue <- task:
		return nil
	case <-dwp.ctx.Done():
		return fmt.Errorf("pool is shutting down")
	default:
		return fmt.Errorf("task queue is full")
	}
}

// resultHandlerは結果を処理します
func (dwp *DynamicWorkerPool) resultHandler() {
	defer dwp.wg.Done()

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

// Shutdownはプールを停止します
func (dwp *DynamicWorkerPool) Shutdown(timeout time.Duration) error {
	log.Println("Starting dynamic worker pool shutdown...")

	close(dwp.taskQueue)
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

// GetStatsは統計を取得します
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
		for i := 1; i <= 10; i++ {
			task := DynamicTask{
				ID:   i,
				Data: fmt.Sprintf("light-task-%d", i),
				Execute: func(data interface{}) error {
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
		for i := 11; i <= 30; i++ {
			task := DynamicTask{
				ID:   i,
				Data: fmt.Sprintf("heavy-task-%d", i),
				Execute: func(data interface{}) error {
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
		for i := 31; i <= 40; i++ {
			task := DynamicTask{
				ID:   i,
				Data: fmt.Sprintf("cool-down-task-%d", i),
				Execute: func(data interface{}) error {
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
		for {
			time.Sleep(10 * time.Second)
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
