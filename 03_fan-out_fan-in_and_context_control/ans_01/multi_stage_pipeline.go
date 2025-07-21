package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// DataProcessingTaskはデータ処理タスクです
type DataProcessingTask struct {
	ID          int64
	Data        []byte
	Priority    int
	ProcessTime time.Duration
	Deadline    time.Time
	Metadata    map[string]interface{}
	CreatedAt   time.Time
}

// ProcessingResultは処理結果です
type ProcessingResult struct {
	TaskID      int64
	Success     bool
	Result      interface{}
	Error       error
	ProcessTime time.Duration
	ProcessorID int
	CompletedAt time.Time
}

// ProcessingStageは処理段階定義です
type ProcessingStage struct {
	Name        string
	ProcessorFn func(context.Context, interface{}) (interface{}, error)
	WorkerCount int
	BufferSize  int
	Timeout     time.Duration
}

// MultiStagePipelineは多段階パイプライン処理システムです
type MultiStagePipeline struct {
	// パイプライン設定
	stages []ProcessingStage

	// チャネル（段階間の接続）
	stageChannels []chan interface{}

	// コンテキスト制御
	ctx    context.Context
	cancel context.CancelFunc

	// 同期制御
	wg sync.WaitGroup

	// 統計・監視
	stats      *PipelineStats
	stageStats []*StageStats

	// 制御フラグ
	isRunning int32
	startTime time.Time
}

// StageProcessorは段階別プロセッサーです
type StageProcessor struct {
	StageIndex  int
	ProcessorID int
	StageName   string
	ProcessorFn func(context.Context, interface{}) (interface{}, error)
	Pipeline    *MultiStagePipeline
	Stats       *ProcessorStats
}

// PipelineStatsはパイプライン統計です
type PipelineStats struct {
	mu               sync.RWMutex
	totalInputItems  int64
	totalOutputItems int64
	totalErrors      int64
	startTime        time.Time
}

// StageStatsは段階別統計です
type StageStats struct {
	mu               sync.RWMutex
	StageName        string
	ProcessedItems   int64
	SuccessItems     int64
	ErrorItems       int64
	TotalProcessTime time.Duration
	AvgProcessTime   time.Duration
	WorkerCount      int
	QueueSize        int
}

// ProcessorStatsはプロセッサー統計です
type ProcessorStats struct {
	ProcessorID      int
	ProcessedItems   int64
	SuccessItems     int64
	ErrorItems       int64
	TotalProcessTime time.Duration
	AvgProcessTime   time.Duration
}

// PipelineConfigはパイプライン設定です
type PipelineConfig struct {
	InputBufferSize  int
	OutputBufferSize int
	EnableMetrics    bool
	MetricsInterval  time.Duration
	ShutdownTimeout  time.Duration
	ErrorThreshold   float64
}

// NewPipelineConfigはデフォルト設定を作成します
func NewPipelineConfig() *PipelineConfig {
	return &PipelineConfig{
		InputBufferSize:  1000,
		OutputBufferSize: 1000,
		EnableMetrics:    true,
		MetricsInterval:  10 * time.Second,
		ShutdownTimeout:  30 * time.Second,
		ErrorThreshold:   0.1, // 10%のエラー率閾値
	}
}

// NewMultiStagePipelineは新しい多段階パイプラインを作成します
func NewMultiStagePipeline(stages []ProcessingStage, config *PipelineConfig) *MultiStagePipeline {
	ctx, cancel := context.WithCancel(context.Background())

	// 段階間チャネルを作成（段階数+1個必要）
	stageChannels := make([]chan interface{}, len(stages)+1)
	for i := range stageChannels {
		if i == 0 {
			stageChannels[i] = make(chan interface{}, config.InputBufferSize)
		} else if i == len(stages) {
			stageChannels[i] = make(chan interface{}, config.OutputBufferSize)
		} else {
			stageChannels[i] = make(chan interface{}, stages[i-1].BufferSize)
		}
	}

	// 段階別統計を初期化
	stageStats := make([]*StageStats, len(stages))
	for i, stage := range stages {
		stageStats[i] = &StageStats{
			StageName:   stage.Name,
			WorkerCount: stage.WorkerCount,
			QueueSize:   stage.BufferSize,
		}
	}

	return &MultiStagePipeline{
		stages:        stages,
		stageChannels: stageChannels,
		ctx:           ctx,
		cancel:        cancel,
		stats: &PipelineStats{
			startTime: time.Now(),
		},
		stageStats: stageStats,
	}
}

// Startはパイプラインを開始します
func (msp *MultiStagePipeline) Start() error {
	if !atomic.CompareAndSwapInt32(&msp.isRunning, 0, 1) {
		return fmt.Errorf("pipeline is already running")
	}

	msp.startTime = time.Now()
	msp.stats.startTime = msp.startTime

	log.Printf("Starting multi-stage pipeline with %d stages", len(msp.stages))

	// 各段階のワーカーを開始
	for stageIndex, stage := range msp.stages {
		log.Printf("Starting stage %d (%s) with %d workers", stageIndex, stage.Name, stage.WorkerCount)

		for workerID := 0; workerID < stage.WorkerCount; workerID++ {
			processor := &StageProcessor{
				StageIndex:  stageIndex,
				ProcessorID: workerID,
				StageName:   stage.Name,
				ProcessorFn: stage.ProcessorFn,
				Pipeline:    msp,
				Stats: &ProcessorStats{
					ProcessorID: workerID,
				},
			}

			msp.wg.Add(1)
			go processor.run()
		}
	}

	// 結果収集を開始
	msp.wg.Add(1)
	go msp.collectResults()

	// メトリクス監視を開始
	msp.wg.Add(1)
	go msp.monitorMetrics()

	log.Printf("Multi-stage pipeline started successfully")
	return nil
}

// runはプロセッサーのメインループです
func (sp *StageProcessor) run() {
	defer sp.Pipeline.wg.Done()

	log.Printf("Stage %d (%s) Processor %d started", sp.StageIndex, sp.StageName, sp.ProcessorID)

	inputChannel := sp.Pipeline.stageChannels[sp.StageIndex]
	outputChannel := sp.Pipeline.stageChannels[sp.StageIndex+1]

	for {
		select {
		case <-sp.Pipeline.ctx.Done():
			log.Printf("Stage %d Processor %d stopping due to context cancellation", sp.StageIndex, sp.ProcessorID)
			return
		case data, ok := <-inputChannel:
			if !ok {
				log.Printf("Stage %d Processor %d stopping due to input channel closure", sp.StageIndex, sp.ProcessorID)
				return
			}

			// データを処理
			result := sp.processData(data)

			// 結果を次の段階に送信
			if result != nil {
				select {
				case outputChannel <- result:
					// 正常に送信完了
				case <-sp.Pipeline.ctx.Done():
					log.Printf("Stage %d Processor %d: context cancelled while sending result", sp.StageIndex, sp.ProcessorID)
					return
				}
			}
		}
	}
}

// processDataはデータを処理します
func (sp *StageProcessor) processData(data interface{}) interface{} {
	start := time.Now()

	// 段階固有のタイムアウトを設定
	stage := sp.Pipeline.stages[sp.StageIndex]
	processingCtx := sp.Pipeline.ctx
	if stage.Timeout > 0 {
		var cancel context.CancelFunc
		processingCtx, cancel = context.WithTimeout(sp.Pipeline.ctx, stage.Timeout)
		defer cancel()
	}

	// 実際の処理を実行
	result, err := sp.ProcessorFn(processingCtx, data)

	processingTime := time.Since(start)

	// 統計更新
	atomic.AddInt64(&sp.Stats.ProcessedItems, 1)
	sp.Stats.TotalProcessTime += processingTime

	if sp.Stats.ProcessedItems > 0 {
		sp.Stats.AvgProcessTime = sp.Stats.TotalProcessTime / time.Duration(sp.Stats.ProcessedItems)
	}

	// 段階別統計更新
	stageStats := sp.Pipeline.stageStats[sp.StageIndex]
	stageStats.mu.Lock()
	stageStats.ProcessedItems++
	stageStats.TotalProcessTime += processingTime
	if stageStats.ProcessedItems > 0 {
		stageStats.AvgProcessTime = stageStats.TotalProcessTime / time.Duration(stageStats.ProcessedItems)
	}
	stageStats.mu.Unlock()

	if err != nil {
		// エラー処理
		atomic.AddInt64(&sp.Stats.ErrorItems, 1)

		stageStats.mu.Lock()
		stageStats.ErrorItems++
		stageStats.mu.Unlock()

		log.Printf("Stage %d (%s) Processor %d: processing error: %v", sp.StageIndex, sp.StageName, sp.ProcessorID, err)
		return nil // エラーの場合は次の段階に送信しない
	}

	atomic.AddInt64(&sp.Stats.SuccessItems, 1)

	stageStats.mu.Lock()
	stageStats.SuccessItems++
	stageStats.mu.Unlock()

	return result
}

// SubmitDataはデータを処理パイプラインに送信します
func (msp *MultiStagePipeline) SubmitData(data interface{}) error {
	if atomic.LoadInt32(&msp.isRunning) == 0 {
		return fmt.Errorf("pipeline is not running")
	}

	select {
	case msp.stageChannels[0] <- data:
		atomic.AddInt64(&msp.stats.totalInputItems, 1)
		return nil
	case <-msp.ctx.Done():
		return fmt.Errorf("pipeline is shutting down")
	default:
		return fmt.Errorf("input channel is full")
	}
}

// GetOutputChannelは出力チャネルを取得します
func (msp *MultiStagePipeline) GetOutputChannel() <-chan interface{} {
	return msp.stageChannels[len(msp.stages)]
}

// collectResultsは結果を収集します
func (msp *MultiStagePipeline) collectResults() {
	defer msp.wg.Done()

	log.Println("Result collector started")

	outputChannel := msp.stageChannels[len(msp.stages)]

	for {
		select {
		case <-msp.ctx.Done():
			log.Println("Result collector stopping due to context cancellation")
			return
		case result, ok := <-outputChannel:
			if !ok {
				log.Println("Result collector stopping due to output channel closure")
				return
			}

			// 統計更新
			atomic.AddInt64(&msp.stats.totalOutputItems, 1)

			// 結果処理（ログ出力など）
			log.Printf("Pipeline output: %+v", result)
		}
	}
}

// monitorMetricsはメトリクスを監視します
func (msp *MultiStagePipeline) monitorMetrics() {
	defer msp.wg.Done()

	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	log.Println("Metrics monitor started")

	for {
		select {
		case <-msp.ctx.Done():
			log.Println("Metrics monitor stopping")
			return
		case <-ticker.C:
			msp.reportMetrics()
		}
	}
}

// reportMetricsはメトリクスを報告します
func (msp *MultiStagePipeline) reportMetrics() {
	msp.stats.mu.RLock()
	defer msp.stats.mu.RUnlock()

	totalInput := atomic.LoadInt64(&msp.stats.totalInputItems)
	totalOutput := atomic.LoadInt64(&msp.stats.totalOutputItems)
	totalErrors := atomic.LoadInt64(&msp.stats.totalErrors)

	uptime := time.Since(msp.stats.startTime)
	var throughput float64
	if uptime.Seconds() > 0 {
		throughput = float64(totalOutput) / uptime.Seconds()
	}

	log.Printf("Pipeline Metrics: Input=%d, Output=%d, Errors=%d, Throughput=%.2f items/sec, Uptime=%v",
		totalInput, totalOutput, totalErrors, throughput, uptime)

	// 段階別統計
	for i, stageStats := range msp.stageStats {
		stageStats.mu.RLock()
		processed := stageStats.ProcessedItems
		success := stageStats.SuccessItems
		errors := stageStats.ErrorItems
		avgTime := stageStats.AvgProcessTime
		stageStats.mu.RUnlock()

		var successRate float64
		if processed > 0 {
			successRate = float64(success) / float64(processed) * 100
		}

		log.Printf("Stage %d (%s): Processed=%d, Success=%.1f%%, Errors=%d, AvgTime=%v",
			i, stageStats.StageName, processed, successRate, errors, avgTime)
	}
}

// Shutdownはパイプラインを停止します
func (msp *MultiStagePipeline) Shutdown(timeout time.Duration) error {
	if !atomic.CompareAndSwapInt32(&msp.isRunning, 1, 0) {
		return fmt.Errorf("pipeline is not running")
	}

	log.Println("Shutting down multi-stage pipeline...")

	// 1. 入力チャネルを閉じて新しいデータの受付を停止
	close(msp.stageChannels[0])

	// 2. 段階間チャネルの連鎖閉じ処理を開始
	go func() {
		for i := 1; i < len(msp.stageChannels); i++ {
			time.Sleep(2 * time.Second) // 各段階の処理完了を待つ
			close(msp.stageChannels[i])
		}
	}()

	// 3. ワーカーの終了を待機
	done := make(chan struct{})
	go func() {
		msp.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("All pipeline workers stopped gracefully")
	case <-time.After(timeout):
		log.Println("Timeout reached, forcing shutdown...")
		msp.cancel()

		// 追加の待機時間
		select {
		case <-done:
			log.Println("Workers stopped after cancellation")
		case <-time.After(2 * time.Second):
			log.Println("Some workers may not have stopped properly")
		}
	}

	log.Println("Multi-stage pipeline shutdown completed")
	return nil
}

// GetStatsは統計情報を取得します
func (msp *MultiStagePipeline) GetStats() map[string]interface{} {
	msp.stats.mu.RLock()
	defer msp.stats.mu.RUnlock()

	totalInput := atomic.LoadInt64(&msp.stats.totalInputItems)
	totalOutput := atomic.LoadInt64(&msp.stats.totalOutputItems)
	totalErrors := atomic.LoadInt64(&msp.stats.totalErrors)

	uptime := time.Since(msp.stats.startTime)
	var throughput float64
	if uptime.Seconds() > 0 {
		throughput = float64(totalOutput) / uptime.Seconds()
	}

	stageStats := make(map[string]interface{})
	for i, stats := range msp.stageStats {
		stats.mu.RLock()
		stageStats[fmt.Sprintf("stage_%d_%s", i, stats.StageName)] = map[string]interface{}{
			"processed_items":      stats.ProcessedItems,
			"success_items":        stats.SuccessItems,
			"error_items":          stats.ErrorItems,
			"average_process_time": stats.AvgProcessTime,
			"worker_count":         stats.WorkerCount,
		}
		stats.mu.RUnlock()
	}

	return map[string]interface{}{
		"total_input":    totalInput,
		"total_output":   totalOutput,
		"total_errors":   totalErrors,
		"throughput_ips": throughput,
		"uptime":         uptime,
		"stage_count":    len(msp.stages),
		"stage_stats":    stageStats,
	}
}

// 使用例とテスト用の処理関数

// DataValidationは第1段階のデータ検証処理です
func DataValidation(ctx context.Context, data interface{}) (interface{}, error) {
	task, ok := data.(DataProcessingTask)
	if !ok {
		return nil, fmt.Errorf("invalid data type")
	}

	// データ検証をシミュレート
	time.Sleep(time.Duration(rand.Intn(50)+10) * time.Millisecond)

	// ランダムに検証エラーを発生（5%の確率）
	if rand.Float64() < 0.05 {
		return nil, fmt.Errorf("validation failed for task %d", task.ID)
	}

	// 検証済みタスクを返す
	validatedTask := task
	validatedTask.Metadata["validated"] = true
	validatedTask.Metadata["validation_time"] = time.Now()

	return validatedTask, nil
}

// DataTransformationは第2段階のデータ変換処理です
func DataTransformation(ctx context.Context, data interface{}) (interface{}, error) {
	task, ok := data.(DataProcessingTask)
	if !ok {
		return nil, fmt.Errorf("invalid data type")
	}

	// データ変換をシミュレート
	time.Sleep(time.Duration(rand.Intn(100)+50) * time.Millisecond)

	// タイムアウトチェック
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// ランダムに変換エラーを発生（3%の確率）
	if rand.Float64() < 0.03 {
		return nil, fmt.Errorf("transformation failed for task %d", task.ID)
	}

	// 変換済みタスクを返す
	transformedTask := task
	transformedTask.Data = append(transformedTask.Data, []byte("_transformed")...)
	transformedTask.Metadata["transformed"] = true
	transformedTask.Metadata["transformation_time"] = time.Now()

	return transformedTask, nil
}

// DataEnrichmentは第3段階のデータ拡張処理です
func DataEnrichment(ctx context.Context, data interface{}) (interface{}, error) {
	task, ok := data.(DataProcessingTask)
	if !ok {
		return nil, fmt.Errorf("invalid data type")
	}

	// データ拡張をシミュレート（外部API呼び出しなど）
	time.Sleep(time.Duration(rand.Intn(200)+100) * time.Millisecond)

	// タイムアウトチェック
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// ランダムに拡張エラーを発生（2%の確率）
	if rand.Float64() < 0.02 {
		return nil, fmt.Errorf("enrichment failed for task %d", task.ID)
	}

	// 拡張済みタスクを返す
	enrichedTask := task
	enrichedTask.Metadata["enriched"] = true
	enrichedTask.Metadata["enrichment_time"] = time.Now()
	enrichedTask.Metadata["external_data"] = map[string]interface{}{
		"source":    "external_api",
		"timestamp": time.Now().Unix(),
		"version":   "1.0",
	}

	return enrichedTask, nil
}

// DataOutputは第4段階の出力処理です
func DataOutput(ctx context.Context, data interface{}) (interface{}, error) {
	task, ok := data.(DataProcessingTask)
	if !ok {
		return nil, fmt.Errorf("invalid data type")
	}

	// 出力処理をシミュレート
	time.Sleep(time.Duration(rand.Intn(30)+10) * time.Millisecond)

	// ランダムに出力エラーを発生（1%の確率）
	if rand.Float64() < 0.01 {
		return nil, fmt.Errorf("output failed for task %d", task.ID)
	}

	// 最終結果を作成
	result := ProcessingResult{
		TaskID:      task.ID,
		Success:     true,
		Result:      task,
		ProcessTime: time.Since(task.CreatedAt),
		CompletedAt: time.Now(),
	}

	return result, nil
}

func main() {
	// パイプライン段階を定義
	stages := []ProcessingStage{
		{
			Name:        "validation",
			ProcessorFn: DataValidation,
			WorkerCount: 3,
			BufferSize:  500,
			Timeout:     5 * time.Second,
		},
		{
			Name:        "transformation",
			ProcessorFn: DataTransformation,
			WorkerCount: 4,
			BufferSize:  300,
			Timeout:     10 * time.Second,
		},
		{
			Name:        "enrichment",
			ProcessorFn: DataEnrichment,
			WorkerCount: 2,
			BufferSize:  200,
			Timeout:     15 * time.Second,
		},
		{
			Name:        "output",
			ProcessorFn: DataOutput,
			WorkerCount: 3,
			BufferSize:  100,
			Timeout:     3 * time.Second,
		},
	}

	// パイプライン設定
	config := NewPipelineConfig()
	config.InputBufferSize = 1000
	config.OutputBufferSize = 500

	// パイプライン作成・開始
	pipeline := NewMultiStagePipeline(stages, config)
	if err := pipeline.Start(); err != nil {
		log.Fatalf("Failed to start pipeline: %v", err)
	}

	// 結果処理のgoroutineを開始
	go func() {
		for result := range pipeline.GetOutputChannel() {
			if processingResult, ok := result.(ProcessingResult); ok {
				log.Printf("Final Result: TaskID=%d, Success=%t, ProcessTime=%v",
					processingResult.TaskID, processingResult.Success, processingResult.ProcessTime)
			} else {
				log.Printf("Unexpected result type: %+v", result)
			}
		}
	}()

	// テストデータを生成・送信
	go func() {
		for i := 0; i < 1000; i++ {
			task := DataProcessingTask{
				ID:       int64(i),
				Data:     []byte(fmt.Sprintf("task_data_%d", i)),
				Priority: rand.Intn(5),
				Deadline: time.Now().Add(time.Duration(rand.Intn(60)+30) * time.Second),
				Metadata: map[string]interface{}{
					"batch_id": i / 100,
					"source":   "test_generator",
				},
				CreatedAt: time.Now(),
			}

			if err := pipeline.SubmitData(task); err != nil {
				log.Printf("Failed to submit task %d: %v", i, err)
				break
			}

			// 送信頻度を制御
			time.Sleep(time.Duration(rand.Intn(50)+10) * time.Millisecond)
		}

		log.Println("All test tasks submitted")
	}()

	// 45秒間実行
	time.Sleep(45 * time.Second)

	// 最終統計表示
	stats := pipeline.GetStats()
	log.Printf("Final Stats: %+v", stats)

	// グレースフルシャットダウン
	if err := pipeline.Shutdown(15 * time.Second); err != nil {
		log.Printf("Shutdown error: %v", err)
	}
}
