package main

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"sync"
	"time"
)

// GoroutineManager はGoroutineのライフサイクルを管理します
type GoroutineManager struct {
	mu      sync.RWMutex
	workers map[string]*Worker
	metrics *RuntimeMetrics
}

// Worker は個別のワーカーを表現します
type Worker struct {
	ID      string
	Started time.Time
	ctx     context.Context
	cancel  context.CancelFunc
	done    chan struct{}
}

// RuntimeMetrics はランタイムメトリクスを収集します
type RuntimeMetrics struct {
	mu              sync.RWMutex
	goroutineCount  int
	memoryUsageMB   float64
	lastCollectedAt time.Time
}

// NewGoroutineManager は新しいマネージャを作成します
func NewGoroutineManager() *GoroutineManager {
	return &GoroutineManager{
		workers: make(map[string]*Worker),
		metrics: &RuntimeMetrics{},
	}
}

// StartWorker は新しいワーカーを開始します
func (gm *GoroutineManager) StartWorker(id string, taskFunc func(context.Context)) error {
	gm.mu.Lock()
	defer gm.mu.Unlock()

	// 既存ワーカーの重複チェック
	if _, exists := gm.workers[id]; exists {
		return fmt.Errorf("worker %s already exists", id)
	}

	// コンテキストとキャンセル関数を作成
	ctx, cancel := context.WithCancel(context.Background())

	worker := &Worker{
		ID:      id,
		Started: time.Now(),
		ctx:     ctx,
		cancel:  cancel,
		done:    make(chan struct{}),
	}

	// ワーカーをGoroutineで開始
	go func() {
		defer close(worker.done)
		taskFunc(ctx)
	}()

	gm.workers[id] = worker
	return nil
}

// StopWorker は指定されたワーカーを停止します
func (gm *GoroutineManager) StopWorker(id string) error {
	gm.mu.Lock()
	worker, exists := gm.workers[id]
	if !exists {
		gm.mu.Unlock()
		return fmt.Errorf("worker %s not found", id)
	}
	delete(gm.workers, id)
	gm.mu.Unlock()

	// ワーカーのキャンセルと完了待ち
	worker.cancel()

	// タイムアウト付きで完了を待機
	select {
	case <-worker.done:
		return nil
	case <-time.After(5 * time.Second):
		return fmt.Errorf("worker %s did not stop within timeout", id)
	}
}

// StopAll はすべてのワーカーを停止します
func (gm *GoroutineManager) StopAll() {
	gm.mu.Lock()
	workers := make([]*Worker, 0, len(gm.workers))
	for _, worker := range gm.workers {
		workers = append(workers, worker)
	}
	gm.workers = make(map[string]*Worker) // マップをクリア
	gm.mu.Unlock()

	// すべてのワーカーをキャンセル
	for _, worker := range workers {
		worker.cancel()
	}

	// すべてのワーカーの完了を待機
	for _, worker := range workers {
		select {
		case <-worker.done:
		case <-time.After(5 * time.Second):
			log.Printf("worker %s did not stop within timeout", worker.ID)
		}
	}
}

// CollectMetrics はランタイムメトリクスを収集します
func (gm *GoroutineManager) CollectMetrics() {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	gm.metrics.mu.Lock()
	defer gm.metrics.mu.Unlock()

	gm.metrics.goroutineCount = runtime.NumGoroutine()
	gm.metrics.memoryUsageMB = float64(memStats.Alloc) / 1024 / 1024
	gm.metrics.lastCollectedAt = time.Now()
}

// GetMetrics は現在のメトリクスを取得します
func (gm *GoroutineManager) GetMetrics() (int, float64, time.Time) {
	gm.metrics.mu.RLock()
	defer gm.metrics.mu.RUnlock()

	return gm.metrics.goroutineCount,
		gm.metrics.memoryUsageMB,
		gm.metrics.lastCollectedAt
}

// GetWorkerCount は現在のワーカー数を取得します
func (gm *GoroutineManager) GetWorkerCount() int {
	gm.mu.RLock()
	defer gm.mu.RUnlock()
	return len(gm.workers)
}

// サンプルタスク関数
func sampleTask(ctx context.Context) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			fmt.Println("Task cancelled")
			return
		case <-ticker.C:
			// 何らかの処理を実行
			runtime.Gosched() // 他のGoroutineに処理を譲る
		}
	}
}

func main() {
	gm := NewGoroutineManager()

	// ワーカーを開始
	for i := 0; i < 3; i++ {
		workerID := fmt.Sprintf("worker-%d", i)
		if err := gm.StartWorker(workerID, sampleTask); err != nil {
			log.Printf("Failed to start worker %s: %v", workerID, err)
		}
	}

	// メトリクス収集を開始
	go func() {
		for {
			gm.CollectMetrics()
			goroutines, memory, timestamp := gm.GetMetrics()
			fmt.Printf("Metrics [%s]: Goroutines=%d, Memory=%.2fMB, Workers=%d\n",
				timestamp.Format("15:04:05"), goroutines, memory, gm.GetWorkerCount())
			time.Sleep(1 * time.Second)
		}
	}()

	// 5秒間動作させる
	time.Sleep(5 * time.Second)

	// すべてのワーカーを停止
	gm.StopAll()
	fmt.Println("All workers stopped")
}
