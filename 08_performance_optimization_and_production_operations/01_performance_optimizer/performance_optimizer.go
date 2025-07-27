package main

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// PerformanceOptimizer Go 1.24の最適化機能を活用した性能改善システム
type PerformanceOptimizer struct {
	// 新map実装の効果測定用
	oldStyleMap map[string]interface{}
	newStyleMap map[string]interface{}
	mapMutex    sync.RWMutex

	// メモリアロケータ最適化の効果測定用
	objectPool sync.Pool
	allocCount int64

	// 並行処理性能測定用
	concurrentTasks chan func()
	workerCount     int
	running         bool
	wg              sync.WaitGroup
}

// BenchmarkResult ベンチマーク結果
type BenchmarkResult struct {
	OperationType    string        `json:"operation_type"`
	TotalOperations  int64         `json:"total_operations"`
	ExecutionTime    time.Duration `json:"execution_time"`
	OperationsPerSec float64       `json:"operations_per_sec"`
	MemoryUsage      uint64        `json:"memory_usage"`
	AllocCount       uint64        `json:"alloc_count"`
}

// NewPerformanceOptimizer 新しいパフォーマンス最適化システムを作成
func NewPerformanceOptimizer(workerCount int) *PerformanceOptimizer {
	po := &PerformanceOptimizer{
		oldStyleMap:     make(map[string]interface{}),
		newStyleMap:     make(map[string]interface{}),
		workerCount:     workerCount,
		concurrentTasks: make(chan func(), workerCount*2),
	}

	// オブジェクトプールの初期化
	po.objectPool = sync.Pool{
		New: func() interface{} {
			// 小オブジェクトを作成（メモリアロケータ最適化対象）
			return make([]byte, 64)
		},
	}

	return po
}

// Start システム開始
func (po *PerformanceOptimizer) Start(ctx context.Context) error {
	po.running = true

	// ワーカープール開始
	for i := 0; i < po.workerCount; i++ {
		po.wg.Add(1)
		go po.worker(ctx)
	}

	return nil
}

// Stop システム停止
func (po *PerformanceOptimizer) Stop() error {
	po.running = false
	close(po.concurrentTasks)
	po.wg.Wait()
	return nil
}

// worker 並行タスク処理ワーカー
func (po *PerformanceOptimizer) worker(ctx context.Context) {
	defer po.wg.Done()

	for {
		select {
		case task, ok := <-po.concurrentTasks:
			if !ok {
				return
			}
			task()
		case <-ctx.Done():
			return
		}
	}
}

// BenchmarkMapOperations map操作のベンチマーク測定
func (po *PerformanceOptimizer) BenchmarkMapOperations(operations int64) (*BenchmarkResult, error) {
	var memStart, memEnd runtime.MemStats
	runtime.GC() // ガベージコレクション実行
	runtime.ReadMemStats(&memStart)

	startTime := time.Now()

	// 新map実装での操作（Go 1.24の最適化対象）
	for i := int64(0); i < operations; i++ {
		key := fmt.Sprintf("key_%d", i)
		value := fmt.Sprintf("value_%d", i)

		po.mapMutex.Lock()
		po.newStyleMap[key] = value
		po.mapMutex.Unlock()

		po.mapMutex.RLock()
		_ = po.newStyleMap[key]
		po.mapMutex.RUnlock()

		if i%1000 == 0 {
			// 定期的にマップをクリア（メモリ使用量制御）
			po.mapMutex.Lock()
			if len(po.newStyleMap) > 5000 {
				po.newStyleMap = make(map[string]interface{})
			}
			po.mapMutex.Unlock()
		}
	}

	executionTime := time.Since(startTime)
	runtime.ReadMemStats(&memEnd)

	return &BenchmarkResult{
		OperationType:    "map_operations",
		TotalOperations:  operations,
		ExecutionTime:    executionTime,
		OperationsPerSec: float64(operations) / executionTime.Seconds(),
		MemoryUsage:      memEnd.Alloc - memStart.Alloc,
		AllocCount:       memEnd.Mallocs - memStart.Mallocs,
	}, nil
}

// BenchmarkMemoryAllocation メモリアロケーション最適化のベンチマーク
func (po *PerformanceOptimizer) BenchmarkMemoryAllocation(operations int64) (*BenchmarkResult, error) {
	var memStart, memEnd runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&memStart)

	startTime := time.Now()

	for i := int64(0); i < operations; i++ {
		// オブジェクトプールを使用（Go 1.24のメモリアロケータ最適化活用）
		objInterface := po.objectPool.Get()
		obj := objInterface.([]byte)

		// 何らかの処理をシミュレート
		for j := 0; j < len(obj); j++ {
			obj[j] = byte(i % 256)
		}

		// プールに戻す
		po.objectPool.Put(objInterface)

		atomic.AddInt64(&po.allocCount, 1)
	}

	executionTime := time.Since(startTime)
	runtime.ReadMemStats(&memEnd)

	return &BenchmarkResult{
		OperationType:    "memory_allocation",
		TotalOperations:  operations,
		ExecutionTime:    executionTime,
		OperationsPerSec: float64(operations) / executionTime.Seconds(),
		MemoryUsage:      memEnd.Alloc - memStart.Alloc,
		AllocCount:       memEnd.Mallocs - memStart.Mallocs,
	}, nil
}

// BenchmarkConcurrentProcessing 並行処理のベンチマーク
func (po *PerformanceOptimizer) BenchmarkConcurrentProcessing(operations int64) (*BenchmarkResult, error) {
	if !po.running {
		return nil, fmt.Errorf("performance optimizer not running")
	}

	var memStart, memEnd runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&memStart)

	startTime := time.Now()
	completedOps := int64(0)
	var opsMutex sync.Mutex

	// 並行タスクを投入
	for i := int64(0); i < operations; i++ {
		select {
		case po.concurrentTasks <- func() {
			// CPU集約的な処理をシミュレート
			result := 0
			for j := 0; j < 1000; j++ {
				result += j
			}

			opsMutex.Lock()
			completedOps++
			opsMutex.Unlock()
		}:
		default:
			// チャンネルが満杯の場合は待機
			time.Sleep(time.Microsecond)
			i-- // 再試行
		}
	}

	// すべてのタスクの完了を待機
	for {
		opsMutex.Lock()
		completed := completedOps
		opsMutex.Unlock()

		if completed >= operations {
			break
		}
		time.Sleep(time.Millisecond)
	}

	executionTime := time.Since(startTime)
	runtime.ReadMemStats(&memEnd)

	return &BenchmarkResult{
		OperationType:    "concurrent_processing",
		TotalOperations:  operations,
		ExecutionTime:    executionTime,
		OperationsPerSec: float64(operations) / executionTime.Seconds(),
		MemoryUsage:      memEnd.Alloc - memStart.Alloc,
		AllocCount:       memEnd.Mallocs - memStart.Mallocs,
	}, nil
}

// GetOptimizationReport 最適化レポートを取得
func (po *PerformanceOptimizer) GetOptimizationReport() map[string]interface{} {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	return map[string]interface{}{
		"goroutine_count":    runtime.NumGoroutine(),
		"memory_alloc":       m.Alloc,
		"memory_total_alloc": m.TotalAlloc,
		"memory_sys":         m.Sys,
		"gc_cycles":          m.NumGC,
		"alloc_count":        atomic.LoadInt64(&po.allocCount),
		"worker_count":       po.workerCount,
		"system_running":     po.running,
	}
}

func main() {
	ctx := context.Background()

	// パフォーマンス最適化システムを作成
	optimizer := NewPerformanceOptimizer(4)

	// システム開始
	if err := optimizer.Start(ctx); err != nil {
		panic(err)
	}
	defer func() {
		if err := optimizer.Stop(); err != nil {
			fmt.Printf("Failed to stop optimizer: %v\n", err)
		}
	}()

	fmt.Println("Go 1.24 パフォーマンス最適化システム開始")

	// Map操作ベンチマーク
	mapResult, err := optimizer.BenchmarkMapOperations(10000)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Map操作: %.2f ops/sec, メモリ使用量: %d bytes\n",
		mapResult.OperationsPerSec, mapResult.MemoryUsage)

	// メモリアロケーションベンチマーク
	memResult, err := optimizer.BenchmarkMemoryAllocation(50000)
	if err != nil {
		panic(err)
	}
	fmt.Printf("メモリアロケーション: %.2f ops/sec, アロケーション数: %d\n",
		memResult.OperationsPerSec, memResult.AllocCount)

	// 並行処理ベンチマーク
	concResult, err := optimizer.BenchmarkConcurrentProcessing(1000)
	if err != nil {
		panic(err)
	}
	fmt.Printf("並行処理: %.2f ops/sec, 実行時間: %v\n",
		concResult.OperationsPerSec, concResult.ExecutionTime)

	// 最適化レポート出力
	report := optimizer.GetOptimizationReport()
	fmt.Printf("システムレポート: %+v\n", report)
}
