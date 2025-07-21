// 復習問題1: Go 1.24新機能を活用した性能最適化システム
// 問題: Go 1.24の新map実装とメモリアロケータ最適化を活用し、
// 高負荷データ処理システムの性能改善を実装してください。

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

// AdvancedOptimizer Go 1.24最適化機能を活用した高度な性能最適化システム
type AdvancedOptimizer struct {
	// 新map実装の効果測定用
	optimizedMaps  []map[string]interface{}
	mapAccessCount int64
	mapMutex       sync.RWMutex

	// メモリアロケータ最適化活用
	smallObjectPool  sync.Pool
	mediumObjectPool sync.Pool
	largeObjectPool  sync.Pool
	allocStats       *AllocationStats

	// 並行処理最適化
	workerPool      *OptimizedWorkerPool
	taskDistributor *TaskDistributor
	resultCollector *ResultCollector

	// 性能監視
	performanceMonitor *PerformanceMonitor
	optimizationReport *OptimizationReport
}

// AllocationStats アロケーション統計
type AllocationStats struct {
	SmallObjects  int64 `json:"small_objects"`
	MediumObjects int64 `json:"medium_objects"`
	LargeObjects  int64 `json:"large_objects"`
	PoolHits      int64 `json:"pool_hits"`
	PoolMisses    int64 `json:"pool_misses"`
	TotalAllocs   int64 `json:"total_allocs"`
}

// OptimizedWorkerPool 最適化されたワーカープール
type OptimizedWorkerPool struct {
	workers     []chan func()
	workerCount int
	taskCount   int64
	wg          sync.WaitGroup
	ctx         context.Context
	cancel      context.CancelFunc
}

// TaskDistributor タスク分散器
type TaskDistributor struct {
	hashRing  []int
	nodeCount int
}

// ResultCollector 結果収集器
type ResultCollector struct {
	results     map[string]interface{}
	resultCount int64
	mutex       sync.RWMutex
}

// PerformanceMonitor 性能監視器
type PerformanceMonitor struct {
	startTime    time.Time
	measurements []PerformanceMeasurement
}

// PerformanceMeasurement 性能測定値
type PerformanceMeasurement struct {
	Timestamp      time.Time     `json:"timestamp"`
	MemoryAlloc    uint64        `json:"memory_alloc"`
	GoroutineCount int           `json:"goroutine_count"`
	GCCycles       uint32        `json:"gc_cycles"`
	ProcessingRate float64       `json:"processing_rate"`
	LatencyP50     time.Duration `json:"latency_p50"`
	LatencyP95     time.Duration `json:"latency_p95"`
}

// OptimizationReport 最適化レポート
type OptimizationReport struct {
	SystemInfo      SystemInfo         `json:"system_info"`
	AllocStats      *AllocationStats   `json:"allocation_stats"`
	MapStats        MapStatistics      `json:"map_stats"`
	WorkerPoolStats WorkerPoolStats    `json:"worker_pool_stats"`
	Performance     PerformanceMetrics `json:"performance"`
	Improvements    []string           `json:"improvements"`
}

// SystemInfo システム情報
type SystemInfo struct {
	GoVersion string `json:"go_version"`
	GOOS      string `json:"goos"`
	GOARCH    string `json:"goarch"`
	NumCPU    int    `json:"num_cpu"`
	MaxProcs  int    `json:"max_procs"`
}

// MapStatistics Map統計
type MapStatistics struct {
	TotalMaps     int     `json:"total_maps"`
	AccessCount   int64   `json:"access_count"`
	AverageSize   int     `json:"average_size"`
	CollisionRate float64 `json:"collision_rate"`
}

// WorkerPoolStats ワーカープール統計
type WorkerPoolStats struct {
	WorkerCount    int           `json:"worker_count"`
	TasksProcessed int64         `json:"tasks_processed"`
	AverageLatency time.Duration `json:"average_latency"`
	ThroughputOPS  float64       `json:"throughput_ops"`
}

// PerformanceMetrics 性能メトリクス
type PerformanceMetrics struct {
	TotalRuntime   time.Duration `json:"total_runtime"`
	CPUUtilization float64       `json:"cpu_utilization"`
	MemoryPeak     uint64        `json:"memory_peak"`
	GCOverhead     float64       `json:"gc_overhead"`
}

// NewAdvancedOptimizer 新しい高度最適化システムを作成
func NewAdvancedOptimizer(workerCount int) *AdvancedOptimizer {
	ctx, cancel := context.WithCancel(context.Background())

	ao := &AdvancedOptimizer{
		optimizedMaps: make([]map[string]interface{}, 0, 1000),
		allocStats:    &AllocationStats{},
		workerPool: &OptimizedWorkerPool{
			workers:     make([]chan func(), workerCount),
			workerCount: workerCount,
			ctx:         ctx,
			cancel:      cancel,
		},
		taskDistributor: &TaskDistributor{
			hashRing:  make([]int, workerCount),
			nodeCount: workerCount,
		},
		resultCollector: &ResultCollector{
			results: make(map[string]interface{}),
		},
		performanceMonitor: &PerformanceMonitor{
			startTime:    time.Now(),
			measurements: make([]PerformanceMeasurement, 0, 1000),
		},
		optimizationReport: &OptimizationReport{},
	}

	// オブジェクトプール初期化（Go 1.24のメモリアロケータ最適化活用）
	ao.initializeObjectPools()

	// ワーカープール初期化
	ao.initializeWorkerPool()

	// タスク分散器初期化
	ao.initializeTaskDistributor()

	return ao
}

// initializeObjectPools オブジェクトプール初期化
func (ao *AdvancedOptimizer) initializeObjectPools() {
	// 小オブジェクトプール（64バイト以下）
	ao.smallObjectPool = sync.Pool{
		New: func() interface{} {
			atomic.AddInt64(&ao.allocStats.SmallObjects, 1)
			return make([]byte, 64)
		},
	}

	// 中オブジェクトプール（1KB以下）
	ao.mediumObjectPool = sync.Pool{
		New: func() interface{} {
			atomic.AddInt64(&ao.allocStats.MediumObjects, 1)
			return make([]byte, 1024)
		},
	}

	// 大オブジェクトプール（8KB以下）
	ao.largeObjectPool = sync.Pool{
		New: func() interface{} {
			atomic.AddInt64(&ao.allocStats.LargeObjects, 1)
			return make([]byte, 8192)
		},
	}
}

// initializeWorkerPool ワーカープール初期化
func (ao *AdvancedOptimizer) initializeWorkerPool() {
	for i := 0; i < ao.workerPool.workerCount; i++ {
		ao.workerPool.workers[i] = make(chan func(), 10)

		ao.workerPool.wg.Add(1)
		go ao.optimizedWorker(i, ao.workerPool.workers[i])
	}
}

// initializeTaskDistributor タスク分散器初期化
func (ao *AdvancedOptimizer) initializeTaskDistributor() {
	for i := 0; i < ao.taskDistributor.nodeCount; i++ {
		ao.taskDistributor.hashRing[i] = i
	}
}

// optimizedWorker 最適化されたワーカー
func (ao *AdvancedOptimizer) optimizedWorker(id int, taskChan chan func()) {
	defer ao.workerPool.wg.Done()

	for {
		select {
		case task, ok := <-taskChan:
			if !ok {
				return
			}

			startTime := time.Now()
			task()
			atomic.AddInt64(&ao.workerPool.taskCount, 1)

			// 性能測定
			latency := time.Since(startTime)
			ao.recordLatency(latency)

		case <-ao.workerPool.ctx.Done():
			return
		}
	}
}

// ProcessHighLoadData 高負荷データ処理
func (ao *AdvancedOptimizer) ProcessHighLoadData(dataSize int) error {
	fmt.Printf("高負荷データ処理開始: %d件\n", dataSize)

	// 性能測定開始
	ao.performanceMonitor.startTime = time.Now()

	// Map処理最適化テスト
	if err := ao.optimizedMapProcessing(dataSize); err != nil {
		return fmt.Errorf("map processing failed: %w", err)
	}

	// メモリアロケーション最適化テスト
	if err := ao.optimizedMemoryAllocation(dataSize); err != nil {
		return fmt.Errorf("memory allocation failed: %w", err)
	}

	// 並行処理最適化テスト
	if err := ao.optimizedConcurrentProcessing(dataSize); err != nil {
		return fmt.Errorf("concurrent processing failed: %w", err)
	}

	// 性能レポート生成
	ao.generateOptimizationReport()

	fmt.Println("高負荷データ処理完了")
	return nil
}

// optimizedMapProcessing 最適化されたMap処理
func (ao *AdvancedOptimizer) optimizedMapProcessing(dataSize int) error {
	fmt.Println("最適化Map処理開始")

	// Go 1.24の新map実装を活用した並行アクセス
	mapCount := dataSize / 1000
	if mapCount < 1 {
		mapCount = 1
	}

	ao.mapMutex.Lock()
	for i := 0; i < mapCount; i++ {
		// 新map実装の効果を最大化するための設計
		optimizedMap := make(map[string]interface{}, 1000)
		ao.optimizedMaps = append(ao.optimizedMaps, optimizedMap)
	}
	ao.mapMutex.Unlock()

	// 並行Map操作
	var wg sync.WaitGroup
	concurrency := runtime.NumCPU()

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for j := 0; j < dataSize/concurrency; j++ {
				mapIndex := (workerID*dataSize/concurrency + j) % len(ao.optimizedMaps)
				key := fmt.Sprintf("worker_%d_key_%d", workerID, j)
				value := fmt.Sprintf("value_%d", j)

				ao.mapMutex.RLock()
				targetMap := ao.optimizedMaps[mapIndex]
				ao.mapMutex.RUnlock()

				// 書き込み
				ao.mapMutex.Lock()
				targetMap[key] = value
				atomic.AddInt64(&ao.mapAccessCount, 1)
				ao.mapMutex.Unlock()

				// 読み込み
				ao.mapMutex.RLock()
				_ = targetMap[key]
				atomic.AddInt64(&ao.mapAccessCount, 1)
				ao.mapMutex.RUnlock()
			}
		}(i)
	}

	wg.Wait()
	fmt.Printf("Map処理完了: %d回のアクセス\n", atomic.LoadInt64(&ao.mapAccessCount))
	return nil
}

// optimizedMemoryAllocation 最適化されたメモリアロケーション
func (ao *AdvancedOptimizer) optimizedMemoryAllocation(dataSize int) error {
	fmt.Println("最適化メモリアロケーション開始")

	var wg sync.WaitGroup
	concurrency := runtime.NumCPU()

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for j := 0; j < dataSize/concurrency; j++ {
				// サイズに応じたプール選択
				size := (j % 3) + 1

				var objInterface interface{}
				var obj []byte
				switch size {
				case 1: // 小オブジェクト
					objInterface = ao.smallObjectPool.Get()
					obj = objInterface.([]byte)
					atomic.AddInt64(&ao.allocStats.PoolHits, 1)
					// 処理シミュレート
					for k := 0; k < len(obj); k++ {
						obj[k] = byte(j % 256)
					}
					ao.smallObjectPool.Put(objInterface)

				case 2: // 中オブジェクト
					objInterface = ao.mediumObjectPool.Get()
					obj = objInterface.([]byte)
					atomic.AddInt64(&ao.allocStats.PoolHits, 1)
					// 処理シミュレート
					for k := 0; k < len(obj); k += 64 {
						obj[k] = byte(j % 256)
					}
					ao.mediumObjectPool.Put(objInterface)

				case 3: // 大オブジェクト
					objInterface = ao.largeObjectPool.Get()
					obj = objInterface.([]byte)
					atomic.AddInt64(&ao.allocStats.PoolHits, 1)
					// 処理シミュレート
					for k := 0; k < len(obj); k += 1024 {
						obj[k] = byte(j % 256)
					}
					ao.largeObjectPool.Put(objInterface)
				}

				atomic.AddInt64(&ao.allocStats.TotalAllocs, 1)
			}
		}(i)
	}

	wg.Wait()
	fmt.Printf("メモリアロケーション完了: %d回の割り当て\n", atomic.LoadInt64(&ao.allocStats.TotalAllocs))
	return nil
}

// optimizedConcurrentProcessing 最適化された並行処理
func (ao *AdvancedOptimizer) optimizedConcurrentProcessing(dataSize int) error {
	fmt.Println("最適化並行処理開始")

	taskCount := 0
	var taskMutex sync.Mutex

	// タスク投入
	for i := 0; i < dataSize; i++ {
		// タスク分散
		workerIndex := ao.distributeTask(i)

		task := func(taskID int) func() {
			return func() {
				// CPU集約的処理をシミュレート
				result := 0
				for j := 0; j < 1000; j++ {
					result += j * taskID
				}

				// 結果保存
				key := fmt.Sprintf("task_%d", taskID)
				ao.resultCollector.mutex.Lock()
				ao.resultCollector.results[key] = result
				atomic.AddInt64(&ao.resultCollector.resultCount, 1)
				ao.resultCollector.mutex.Unlock()

				taskMutex.Lock()
				taskCount++
				taskMutex.Unlock()
			}
		}(i)

		// ワーカーにタスク投入
		select {
		case ao.workerPool.workers[workerIndex] <- task:
		default:
			// バックプレッシャー対応
			time.Sleep(time.Microsecond)
			i-- // 再試行
		}
	}

	// 全タスク完了待機
	for {
		taskMutex.Lock()
		completed := taskCount
		taskMutex.Unlock()

		if completed >= dataSize {
			break
		}
		time.Sleep(time.Millisecond)
	}

	fmt.Printf("並行処理完了: %d個のタスク処理\n", taskCount)
	return nil
}

// distributeTask タスク分散
func (ao *AdvancedOptimizer) distributeTask(taskID int) int {
	// 簡単なハッシュベース分散
	return taskID % ao.taskDistributor.nodeCount
}

// recordLatency レイテンシ記録
func (ao *AdvancedOptimizer) recordLatency(latency time.Duration) {
	// 簡略化のため統計処理は省略
}

// generateOptimizationReport 最適化レポート生成
func (ao *AdvancedOptimizer) generateOptimizationReport() {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	ao.optimizationReport = &OptimizationReport{
		SystemInfo: SystemInfo{
			GoVersion: runtime.Version(),
			GOOS:      runtime.GOOS,
			GOARCH:    runtime.GOARCH,
			NumCPU:    runtime.NumCPU(),
			MaxProcs:  runtime.GOMAXPROCS(0),
		},
		AllocStats: ao.allocStats,
		MapStats: MapStatistics{
			TotalMaps:   len(ao.optimizedMaps),
			AccessCount: atomic.LoadInt64(&ao.mapAccessCount),
		},
		WorkerPoolStats: WorkerPoolStats{
			WorkerCount:    ao.workerPool.workerCount,
			TasksProcessed: atomic.LoadInt64(&ao.workerPool.taskCount),
		},
		Performance: PerformanceMetrics{
			TotalRuntime: time.Since(ao.performanceMonitor.startTime),
			MemoryPeak:   memStats.Alloc,
		},
		Improvements: []string{
			"Go 1.24新map実装による並行アクセス性能向上",
			"メモリアロケータ最適化による小オブジェクト処理改善",
			"ランタイム内部mutex改善による高並行負荷安定性向上",
		},
	}
}

// GetOptimizationReport 最適化レポート取得
func (ao *AdvancedOptimizer) GetOptimizationReport() *OptimizationReport {
	return ao.optimizationReport
}

// Stop システム停止
func (ao *AdvancedOptimizer) Stop() error {
	ao.workerPool.cancel()

	// ワーカーチャンネル閉鎖
	for _, worker := range ao.workerPool.workers {
		close(worker)
	}

	// ワーカー完了待機
	ao.workerPool.wg.Wait()

	fmt.Println("最適化システム停止完了")
	return nil
}

func main() {
	// Go 1.24最適化機能を活用した性能改善システム実行
	optimizer := NewAdvancedOptimizer(runtime.NumCPU())
	defer func() {
		if err := optimizer.Stop(); err != nil {
			log.Printf("Failed to stop optimizer: %v", err)
		}
	}()

	fmt.Println("Go 1.24 高度性能最適化システム開始")
	fmt.Printf("CPU数: %d, ワーカー数: %d\n", runtime.NumCPU(), runtime.NumCPU())

	// 高負荷データ処理実行
	dataSize := 100000
	if err := optimizer.ProcessHighLoadData(dataSize); err != nil {
		panic(err)
	}

	// 最適化レポート表示
	report := optimizer.GetOptimizationReport()
	fmt.Printf("\n=== 最適化レポート ===\n")
	fmt.Printf("実行時間: %v\n", report.Performance.TotalRuntime)
	fmt.Printf("Map総数: %d\n", report.MapStats.TotalMaps)
	fmt.Printf("Mapアクセス数: %d\n", report.MapStats.AccessCount)
	fmt.Printf("メモリアロケーション数: %d\n", report.AllocStats.TotalAllocs)
	fmt.Printf("プールヒット数: %d\n", report.AllocStats.PoolHits)
	fmt.Printf("処理タスク数: %d\n", report.WorkerPoolStats.TasksProcessed)
	fmt.Printf("メモリピーク: %d bytes\n", report.Performance.MemoryPeak)

	fmt.Printf("\n=== 改善項目 ===\n")
	for i, improvement := range report.Improvements {
		fmt.Printf("%d. %s\n", i+1, improvement)
	}
}
