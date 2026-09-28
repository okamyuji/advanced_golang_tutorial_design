// 復習問題1: Go 1.24新機能を活用した性能最適化システム
// 問題: Go 1.24の新map実装とメモリアロケータ最適化を活用し、
// 高負荷データ処理システムの性能改善を実装してください。

package main

import (
	"errors"
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
	optimizedMaps  []map[string]any
	mapAccessCount atomic.Int64
	mapMutex       sync.RWMutex

	// サイズ別のオブジェクトプール（割り当ての回数を減らす）
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
	optimizationReport atomic.Pointer[OptimizationReport]
}

var errPoolStopped = errors.New("ワーカープールは停止済みです")

// AllocationStats アロケーション統計
type AllocationStats struct {
	SmallObjects  atomic.Int64
	MediumObjects atomic.Int64
	LargeObjects  atomic.Int64
	PoolMisses    atomic.Int64 // プールが空で New が作り直した回数
	TotalAllocs   atomic.Int64 // プールから取り出した回数
}

// AllocationStatsSnapshot レポート用に読み出したアロケーション統計
type AllocationStatsSnapshot struct {
	SmallObjects  int64 `json:"small_objects"`
	MediumObjects int64 `json:"medium_objects"`
	LargeObjects  int64 `json:"large_objects"`
	PoolHits      int64 `json:"pool_hits"`
	PoolMisses    int64 `json:"pool_misses"`
	TotalAllocs   int64 `json:"total_allocs"`
}

// snapshot 現在の値を読み出す。ヒット数は取り出し回数から New の回数を引いて求める
func (s *AllocationStats) snapshot() AllocationStatsSnapshot {
	total := s.TotalAllocs.Load()
	misses := s.PoolMisses.Load()
	return AllocationStatsSnapshot{
		SmallObjects:  s.SmallObjects.Load(),
		MediumObjects: s.MediumObjects.Load(),
		LargeObjects:  s.LargeObjects.Load(),
		PoolHits:      total - misses,
		PoolMisses:    misses,
		TotalAllocs:   total,
	}
}

// OptimizedWorkerPool 最適化されたワーカープール
type OptimizedWorkerPool struct {
	workers     []chan func()
	workerCount int
	taskCount   atomic.Int64
	wg          sync.WaitGroup

	// mu 投入中（RLock）とチャネルの close（Lock）を排他する
	mu     sync.RWMutex
	closed bool
}

// submit 指定したワーカーにタスクを投入する。停止後はエラーを返す
func (p *OptimizedWorkerPool) submit(workerIndex int, task func()) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return errPoolStopped
	}
	// ワーカーは close されるまで受信し続けるので、この送信は必ず終わる
	p.workers[workerIndex] <- task
	return nil
}

// TaskDistributor タスク分散器
type TaskDistributor struct {
	hashRing  []int
	nodeCount int
}

// ResultCollector 結果収集器
type ResultCollector struct {
	results     map[string]any
	resultCount atomic.Int64
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
	SystemInfo      SystemInfo              `json:"system_info"`
	AllocStats      AllocationStatsSnapshot `json:"allocation_stats"`
	MapStats        MapStatistics           `json:"map_stats"`
	WorkerPoolStats WorkerPoolStats         `json:"worker_pool_stats"`
	Performance     PerformanceMetrics      `json:"performance"`
	Improvements    []string                `json:"improvements"`
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
	MemoryInUse    uint64        `json:"memory_in_use"` // レポート作成時点のヒープ使用量（runtime.MemStats.Alloc）
	GCOverhead     float64       `json:"gc_overhead"`
}

// NewAdvancedOptimizer 新しい高度最適化システムを作成
func NewAdvancedOptimizer(workerCount int) *AdvancedOptimizer {
	ao := &AdvancedOptimizer{
		optimizedMaps: make([]map[string]any, 0, 1000),
		allocStats:    &AllocationStats{},
		workerPool: &OptimizedWorkerPool{
			workers:     make([]chan func(), workerCount),
			workerCount: workerCount,
		},
		taskDistributor: &TaskDistributor{
			hashRing:  make([]int, workerCount),
			nodeCount: workerCount,
		},
		resultCollector: &ResultCollector{
			results: make(map[string]any),
		},
		performanceMonitor: &PerformanceMonitor{
			startTime:    time.Now(),
			measurements: make([]PerformanceMeasurement, 0, 1000),
		},
	}
	ao.optimizationReport.Store(&OptimizationReport{})

	// オブジェクトプール初期化（使い終えたバッファを再利用して割り当ての回数を減らす）
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
		New: func() any {
			ao.allocStats.SmallObjects.Add(1)
			ao.allocStats.PoolMisses.Add(1)
			return make([]byte, 64)
		},
	}

	// 中オブジェクトプール（1KB以下）
	ao.mediumObjectPool = sync.Pool{
		New: func() any {
			ao.allocStats.MediumObjects.Add(1)
			ao.allocStats.PoolMisses.Add(1)
			return make([]byte, 1024)
		},
	}

	// 大オブジェクトプール（8KB以下）
	ao.largeObjectPool = sync.Pool{
		New: func() any {
			ao.allocStats.LargeObjects.Add(1)
			ao.allocStats.PoolMisses.Add(1)
			return make([]byte, 8192)
		},
	}
}

// initializeWorkerPool ワーカープール初期化
func (ao *AdvancedOptimizer) initializeWorkerPool() {
	for i := range ao.workerPool.workerCount {
		taskChan := make(chan func(), 10)
		ao.workerPool.workers[i] = taskChan
		ao.workerPool.wg.Go(func() { ao.optimizedWorker(taskChan) })
	}
}

// initializeTaskDistributor タスク分散器初期化
func (ao *AdvancedOptimizer) initializeTaskDistributor() {
	for i := range ao.taskDistributor.nodeCount {
		ao.taskDistributor.hashRing[i] = i
	}
}

// optimizedWorker 最適化されたワーカー。Stop がチャネルを close するまで、
// 投入済みのタスクを最後まで実行する（投入側が完了を待って止まらないようにするため）
func (ao *AdvancedOptimizer) optimizedWorker(taskChan chan func()) {
	for task := range taskChan {
		startTime := time.Now()
		task()
		ao.workerPool.taskCount.Add(1)

		// 性能測定
		latency := time.Since(startTime)
		ao.recordLatency(latency)
	}
}

// ProcessHighLoadData 高負荷データ処理
func (ao *AdvancedOptimizer) ProcessHighLoadData(dataSize int) error {
	fmt.Printf("高負荷データ処理開始: %d件\n", dataSize)

	// 性能測定開始
	startTime := time.Now()

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
	ao.generateOptimizationReport(startTime)

	fmt.Println("高負荷データ処理完了")
	return nil
}

// optimizedMapProcessing 最適化されたMap処理
func (ao *AdvancedOptimizer) optimizedMapProcessing(dataSize int) error {
	fmt.Println("最適化Map処理開始")

	// 組み込みmapはGo 1.24からSwiss Tablesの実装になった。並行アクセスはmapMutexで守る
	mapCount := max(dataSize/1000, 1)

	ao.mapMutex.Lock()
	for range mapCount {
		// 新map実装の効果を最大化するための設計
		optimizedMap := make(map[string]any, 1000)
		ao.optimizedMaps = append(ao.optimizedMaps, optimizedMap)
	}
	ao.mapMutex.Unlock()

	// 並行Map操作
	var wg sync.WaitGroup
	concurrency := runtime.GOMAXPROCS(0)

	for workerID := range concurrency {
		wg.Go(func() {
			for j := range dataSize / concurrency {
				mapIndex := (workerID*dataSize/concurrency + j) % len(ao.optimizedMaps)
				key := fmt.Sprintf("worker_%d_key_%d", workerID, j)
				value := fmt.Sprintf("value_%d", j)

				ao.mapMutex.RLock()
				targetMap := ao.optimizedMaps[mapIndex]
				ao.mapMutex.RUnlock()

				// 書き込み
				ao.mapMutex.Lock()
				targetMap[key] = value
				ao.mapAccessCount.Add(1)
				ao.mapMutex.Unlock()

				// 読み込み
				ao.mapMutex.RLock()
				_ = targetMap[key]
				ao.mapAccessCount.Add(1)
				ao.mapMutex.RUnlock()
			}
		})
	}

	wg.Wait()
	fmt.Printf("Map処理完了: %d回のアクセス\n", ao.mapAccessCount.Load())
	return nil
}

// optimizedMemoryAllocation 最適化されたメモリアロケーション
func (ao *AdvancedOptimizer) optimizedMemoryAllocation(dataSize int) error {
	fmt.Println("最適化メモリアロケーション開始")

	var wg sync.WaitGroup
	concurrency := runtime.GOMAXPROCS(0)

	for range concurrency {
		wg.Go(func() {
			for j := range dataSize / concurrency {
				// サイズに応じたプール選択
				size := (j % 3) + 1

				var objInterface any
				var obj []byte
				switch size {
				case 1: // 小オブジェクト
					objInterface = ao.smallObjectPool.Get()
					obj = objInterface.([]byte)
					// 処理シミュレート
					for k := range obj {
						obj[k] = byte(j % 256)
					}
					ao.smallObjectPool.Put(objInterface)

				case 2: // 中オブジェクト
					objInterface = ao.mediumObjectPool.Get()
					obj = objInterface.([]byte)
					// 処理シミュレート
					for k := 0; k < len(obj); k += 64 {
						obj[k] = byte(j % 256)
					}
					ao.mediumObjectPool.Put(objInterface)

				case 3: // 大オブジェクト
					objInterface = ao.largeObjectPool.Get()
					obj = objInterface.([]byte)
					// 処理シミュレート
					for k := 0; k < len(obj); k += 1024 {
						obj[k] = byte(j % 256)
					}
					ao.largeObjectPool.Put(objInterface)
				}

				ao.allocStats.TotalAllocs.Add(1)
			}
		})
	}

	wg.Wait()
	fmt.Printf("メモリアロケーション完了: %d回の割り当て\n", ao.allocStats.TotalAllocs.Load())
	return nil
}

// optimizedConcurrentProcessing 最適化された並行処理
func (ao *AdvancedOptimizer) optimizedConcurrentProcessing(dataSize int) error {
	fmt.Println("最適化並行処理開始")

	var taskCount atomic.Int64
	var tasks sync.WaitGroup

	// タスク投入。ワーカーのキューが満杯なら空くまで待つ（バックプレッシャー）
	var submitErr error
	for taskID := range dataSize {
		// タスク分散
		workerIndex := ao.distributeTask(taskID)

		task := func() {
			defer tasks.Done()
			// CPU集約的処理をシミュレート
			result := 0
			for j := range 1000 {
				result += j * taskID
			}

			// 結果保存
			key := fmt.Sprintf("task_%d", taskID)
			ao.resultCollector.mutex.Lock()
			ao.resultCollector.results[key] = result
			ao.resultCollector.resultCount.Add(1)
			ao.resultCollector.mutex.Unlock()

			taskCount.Add(1)
		}

		tasks.Add(1)
		if err := ao.workerPool.submit(workerIndex, task); err != nil {
			tasks.Done()
			submitErr = err
			break
		}
	}

	// 投入済みのタスクはワーカーが必ず実行するので、全件の完了を待つ
	tasks.Wait()
	if submitErr != nil {
		return submitErr
	}

	fmt.Printf("並行処理完了: %d個のタスク処理\n", taskCount.Load())
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
func (ao *AdvancedOptimizer) generateOptimizationReport(startTime time.Time) {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	ao.mapMutex.RLock()
	totalMaps := len(ao.optimizedMaps)
	ao.mapMutex.RUnlock()

	ao.optimizationReport.Store(&OptimizationReport{
		SystemInfo: SystemInfo{
			GoVersion: runtime.Version(),
			GOOS:      runtime.GOOS,
			GOARCH:    runtime.GOARCH,
			NumCPU:    runtime.NumCPU(),
			MaxProcs:  runtime.GOMAXPROCS(0),
		},
		AllocStats: ao.allocStats.snapshot(),
		MapStats: MapStatistics{
			TotalMaps:   totalMaps,
			AccessCount: ao.mapAccessCount.Load(),
		},
		WorkerPoolStats: WorkerPoolStats{
			WorkerCount:    ao.workerPool.workerCount,
			TasksProcessed: ao.workerPool.taskCount.Load(),
		},
		Performance: PerformanceMetrics{
			TotalRuntime: time.Since(startTime),
			MemoryInUse:  memStats.Alloc,
		},
		Improvements: []string{
			"Go 1.24の組み込みmapのSwiss Tables化",
			"メモリアロケータ最適化による小オブジェクト処理改善",
			"ランタイム内部mutex改善による高並行負荷安定性向上",
		},
	})
}

// GetOptimizationReport 最適化レポート取得
func (ao *AdvancedOptimizer) GetOptimizationReport() *OptimizationReport {
	return ao.optimizationReport.Load()
}

// Stop システム停止。投入済みのタスクを実行し終えてからワーカーを止める。二度目以降の呼び出しは何もしない
func (ao *AdvancedOptimizer) Stop() error {
	ao.workerPool.mu.Lock()
	if ao.workerPool.closed {
		ao.workerPool.mu.Unlock()
		return nil
	}
	ao.workerPool.closed = true

	// ワーカーチャンネル閉鎖
	for _, worker := range ao.workerPool.workers {
		close(worker)
	}
	ao.workerPool.mu.Unlock()

	// ワーカー完了待機
	ao.workerPool.wg.Wait()

	fmt.Println("最適化システム停止完了")
	return nil
}

func main() {
	// Go 1.24最適化機能を活用した性能改善システム実行
	workerCount := runtime.GOMAXPROCS(0)
	optimizer := NewAdvancedOptimizer(workerCount)
	defer func() {
		if err := optimizer.Stop(); err != nil {
			log.Printf("Failed to stop optimizer: %v", err)
		}
	}()

	fmt.Println("Go 1.24 高度性能最適化システム開始")
	fmt.Printf("CPU数: %d, ワーカー数: %d\n", runtime.NumCPU(), workerCount)

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
	fmt.Printf("メモリ使用量（レポート作成時点）: %d bytes\n", report.Performance.MemoryInUse)

	fmt.Printf("\n=== 改善項目 ===\n")
	for i, improvement := range report.Improvements {
		fmt.Printf("%d. %s\n", i+1, improvement)
	}
}
