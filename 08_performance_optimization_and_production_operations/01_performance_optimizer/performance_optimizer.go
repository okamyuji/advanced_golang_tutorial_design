package main

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// PerformanceOptimizer map操作、オブジェクトプール、並行処理の性能を測るシステム
type PerformanceOptimizer struct {
	// dataMap map操作の計測用。組み込みmapはGo 1.24からSwiss Tablesの実装で、旧実装には切り替えられない
	dataMap  map[string]any
	mapMutex sync.RWMutex

	// objectPool 割り当ての回数を減らす効果の計測用
	objectPool sync.Pool
	allocCount atomic.Int64

	// 並行処理性能測定用
	concurrentTasks chan func()
	workerCount     int
	wg              sync.WaitGroup

	// stateMu 投入中（RLock）と concurrentTasks の close（Lock）を排他する
	stateMu sync.RWMutex
	running bool
	stopped bool
	ctx     context.Context
}

var (
	errNotRunning     = errors.New("performance optimizer not running")
	errAlreadyStarted = errors.New("performance optimizer already started")
)

// BenchmarkResult ベンチマーク結果
type BenchmarkResult struct {
	OperationType    string        `json:"operation_type"`
	TotalOperations  int64         `json:"total_operations"`
	ExecutionTime    time.Duration `json:"execution_time"`
	OperationsPerSec float64       `json:"operations_per_sec"`
	MemoryUsage      uint64        `json:"memory_usage"` // 計測中に割り当てた総バイト数（Alloc は GC で減るので差分に使えない）
	AllocCount       uint64        `json:"alloc_count"`
}

// NewPerformanceOptimizer 新しいパフォーマンス最適化システムを作成
func NewPerformanceOptimizer(workerCount int) *PerformanceOptimizer {
	po := &PerformanceOptimizer{
		dataMap:         make(map[string]any),
		workerCount:     workerCount,
		concurrentTasks: make(chan func(), workerCount*2),
	}

	// オブジェクトプールの初期化
	po.objectPool = sync.Pool{
		New: func() any {
			// 小オブジェクトを作成（メモリアロケータ最適化対象）
			return make([]byte, 64)
		},
	}

	return po
}

// Start システム開始。ctx が終わると新しいタスクの投入を受け付けなくなる。ワーカーは Stop で止まる
func (po *PerformanceOptimizer) Start(ctx context.Context) error {
	po.stateMu.Lock()
	defer po.stateMu.Unlock()
	// close 済みのチャネルは再利用できないので、停止後の再開も受け付けない
	if po.running || po.stopped {
		return errAlreadyStarted
	}
	po.running = true
	po.ctx = ctx

	// ワーカープール開始
	for range po.workerCount {
		po.wg.Go(po.worker)
	}

	return nil
}

// Stop システム停止。二度目以降の呼び出しは何もしない
func (po *PerformanceOptimizer) Stop() error {
	po.stateMu.Lock()
	if !po.running {
		po.stateMu.Unlock()
		return nil
	}
	po.running = false
	po.stopped = true
	close(po.concurrentTasks)
	po.stateMu.Unlock()

	po.wg.Wait()
	return nil
}

// isRunning 実行中かどうか
func (po *PerformanceOptimizer) isRunning() bool {
	po.stateMu.RLock()
	defer po.stateMu.RUnlock()
	return po.running
}

// submit タスクを1件投入する。停止後や ctx 終了後はエラーを返す
func (po *PerformanceOptimizer) submit(task func()) error {
	po.stateMu.RLock()
	defer po.stateMu.RUnlock()
	if !po.running {
		return errNotRunning
	}
	select {
	case po.concurrentTasks <- task:
		return nil
	case <-po.ctx.Done():
		return po.ctx.Err()
	}
}

// worker 並行タスク処理ワーカー。Stop がチャネルを close するまで、
// キューに入ったタスクを最後まで実行する（投入済みのタスクを待つ側が止まらないようにするため）
func (po *PerformanceOptimizer) worker() {
	for task := range po.concurrentTasks {
		task()
	}
}

// BenchmarkMapOperations map操作のベンチマーク測定
func (po *PerformanceOptimizer) BenchmarkMapOperations(operations int64) (*BenchmarkResult, error) {
	var memStart, memEnd runtime.MemStats
	runtime.GC() // ガベージコレクション実行
	runtime.ReadMemStats(&memStart)

	startTime := time.Now()

	// 新map実装での操作（Go 1.24の最適化対象）
	for i := range operations {
		key := fmt.Sprintf("key_%d", i)
		value := fmt.Sprintf("value_%d", i)

		po.mapMutex.Lock()
		po.dataMap[key] = value
		po.mapMutex.Unlock()

		po.mapMutex.RLock()
		_ = po.dataMap[key]
		po.mapMutex.RUnlock()

		if i%1000 == 0 {
			// 定期的にマップをクリア（メモリ使用量制御）
			po.mapMutex.Lock()
			if len(po.dataMap) > 5000 {
				po.dataMap = make(map[string]any)
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
		MemoryUsage:      memEnd.TotalAlloc - memStart.TotalAlloc,
		AllocCount:       memEnd.Mallocs - memStart.Mallocs,
	}, nil
}

// BenchmarkMemoryAllocation メモリアロケーション最適化のベンチマーク
func (po *PerformanceOptimizer) BenchmarkMemoryAllocation(operations int64) (*BenchmarkResult, error) {
	var memStart, memEnd runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&memStart)

	startTime := time.Now()

	for i := range operations {
		// オブジェクトプールを使用（使い終えたバッファを再利用して割り当ての回数を減らす）
		objInterface := po.objectPool.Get()
		obj := objInterface.([]byte)

		// 何らかの処理をシミュレート
		for j := range obj {
			obj[j] = byte(i % 256)
		}

		// プールに戻す
		po.objectPool.Put(objInterface)

		po.allocCount.Add(1)
	}

	executionTime := time.Since(startTime)
	runtime.ReadMemStats(&memEnd)

	return &BenchmarkResult{
		OperationType:    "memory_allocation",
		TotalOperations:  operations,
		ExecutionTime:    executionTime,
		OperationsPerSec: float64(operations) / executionTime.Seconds(),
		MemoryUsage:      memEnd.TotalAlloc - memStart.TotalAlloc,
		AllocCount:       memEnd.Mallocs - memStart.Mallocs,
	}, nil
}

// BenchmarkConcurrentProcessing 並行処理のベンチマーク
func (po *PerformanceOptimizer) BenchmarkConcurrentProcessing(operations int64) (*BenchmarkResult, error) {
	if !po.isRunning() {
		return nil, errNotRunning
	}

	var memStart, memEnd runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&memStart)

	startTime := time.Now()
	var tasks sync.WaitGroup
	task := func() {
		defer tasks.Done()
		// CPU集約的な処理をシミュレート
		result := 0
		for j := range 1000 {
			result += j
		}
		_ = result
	}

	// 並行タスクを投入（チャネルが満杯なら空くまで待つ）
	var submitErr error
	for range operations {
		tasks.Add(1)
		if err := po.submit(task); err != nil {
			tasks.Done()
			submitErr = err
			break
		}
	}

	// 投入済みのタスクはワーカーが必ず実行するので、完了を待ってから返す
	tasks.Wait()
	if submitErr != nil {
		return nil, submitErr
	}

	executionTime := time.Since(startTime)
	runtime.ReadMemStats(&memEnd)

	return &BenchmarkResult{
		OperationType:    "concurrent_processing",
		TotalOperations:  operations,
		ExecutionTime:    executionTime,
		OperationsPerSec: float64(operations) / executionTime.Seconds(),
		MemoryUsage:      memEnd.TotalAlloc - memStart.TotalAlloc,
		AllocCount:       memEnd.Mallocs - memStart.Mallocs,
	}, nil
}

// GetOptimizationReport 最適化レポートを取得
func (po *PerformanceOptimizer) GetOptimizationReport() map[string]any {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	return map[string]any{
		"goroutine_count":    runtime.NumGoroutine(),
		"memory_alloc":       m.Alloc,
		"memory_total_alloc": m.TotalAlloc,
		"memory_sys":         m.Sys,
		"gc_cycles":          m.NumGC,
		"alloc_count":        po.allocCount.Load(),
		"worker_count":       po.workerCount,
		"system_running":     po.isRunning(),
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
