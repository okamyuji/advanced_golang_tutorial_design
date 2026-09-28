package main

import (
	"context"
	"fmt"
	"log"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"
)

// DataProcessingTask データ処理タスクです
type DataProcessingTask struct {
	ID          int64
	Data        []byte
	Priority    int
	ProcessTime time.Duration
	Deadline    time.Time
	Metadata    map[string]any
	CreatedAt   time.Time
}

// ProcessingResult 処理結果です
type ProcessingResult struct {
	TaskID      int64
	Success     bool
	Result      any
	Error       error
	ProcessTime time.Duration
	ProcessorID int
	CompletedAt time.Time
}

// ProcessingStage 処理段階定義です
type ProcessingStage struct {
	Name        string
	ProcessorFn func(context.Context, any) (any, error)
	WorkerCount int
	BufferSize  int
	Timeout     time.Duration
}

// MultiStagePipeline 多段階パイプライン処理システムです
type MultiStagePipeline struct {
	// パイプライン設定
	stages []ProcessingStage

	// チャネル（段階間の接続）
	stageChannels []chan any

	// コンテキスト制御
	ctx    context.Context
	cancel context.CancelFunc

	// 同期制御
	wg sync.WaitGroup

	// 統計・監視
	stats      *PipelineStats
	stageStats []*StageStats

	// 制御フラグ
	isRunning atomic.Bool
	startTime time.Time
}

// StageProcessor 段階別プロセッサーです
type StageProcessor struct {
	StageIndex  int
	ProcessorID int
	StageName   string
	ProcessorFn func(context.Context, any) (any, error)
	Pipeline    *MultiStagePipeline
	Stats       *ProcessorStats
}

// PipelineStats パイプライン統計です
type PipelineStats struct {
	mu               sync.RWMutex
	totalInputItems  atomic.Int64
	totalOutputItems atomic.Int64
	totalErrors      atomic.Int64
	startTime        time.Time
}

// StageStats 段階別統計です
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

// ProcessorStats プロセッサー統計です。同じプロセッサーgoroutineだけが
// 更新するため、カウンタは型付きatomicで保持しつつ他フィールドはそのままにします。
type ProcessorStats struct {
	ProcessorID      int
	ProcessedItems   atomic.Int64
	SuccessItems     atomic.Int64
	ErrorItems       atomic.Int64
	TotalProcessTime time.Duration
	AvgProcessTime   time.Duration
}

// PipelineConfig パイプライン設定です
type PipelineConfig struct {
	InputBufferSize  int
	OutputBufferSize int
	EnableMetrics    bool
	MetricsInterval  time.Duration
	ShutdownTimeout  time.Duration
	ErrorThreshold   float64
}

// NewPipelineConfig デフォルト設定を作成します
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

// NewMultiStagePipeline 新しい多段階パイプラインを作成します
func NewMultiStagePipeline(stages []ProcessingStage, config *PipelineConfig) *MultiStagePipeline {
	ctx, cancel := context.WithCancel(context.Background())

	// 段階間チャネルを作成（段階数+1個必要）
	stageChannels := make([]chan any, len(stages)+1)
	for i := range stageChannels {
		if i == 0 {
			stageChannels[i] = make(chan any, config.InputBufferSize)
		} else if i == len(stages) {
			stageChannels[i] = make(chan any, config.OutputBufferSize)
		} else {
			stageChannels[i] = make(chan any, stages[i-1].BufferSize)
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

// Start パイプラインを開始します
func (msp *MultiStagePipeline) Start() error {
	if !msp.isRunning.CompareAndSwap(false, true) {
		return fmt.Errorf("pipeline is already running")
	}

	msp.startTime = time.Now()
	msp.stats.startTime = msp.startTime

	log.Printf("Starting multi-stage pipeline with %d stages", len(msp.stages))

	// 各段階のワーカーを開始
	for stageIndex, stage := range msp.stages {
		log.Printf("Starting stage %d (%s) with %d workers", stageIndex, stage.Name, stage.WorkerCount)

		for workerID := range stage.WorkerCount {
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

			msp.wg.Go(processor.run)
		}
	}

	// 結果収集を開始
	msp.wg.Go(msp.collectResults)

	// メトリクス監視を開始
	msp.wg.Go(msp.monitorMetrics)

	log.Printf("Multi-stage pipeline started successfully")
	return nil
}

// run プロセッサーのメインループです
func (sp *StageProcessor) run() {
	log.Printf("Stage %d (%s) Processor %d started", sp.StageIndex, sp.StageName, sp.ProcessorID)

	inputChannel := sp.Pipeline.stageChannels[sp.StageIndex]
	outputChannel := sp.Pipeline.stageChannels[sp.StageIndex+1]

	for {
		select {
		case <-sp.Pipeline.ctx.Done():
			log.Printf("Stage %d Processor %d stopping due to context cancellation", sp.StageIndex, sp.ProcessorID)
			return
		case data := <-inputChannel:
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

// processData データを処理します
func (sp *StageProcessor) processData(data any) any {
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
	processedItems := sp.Stats.ProcessedItems.Add(1)
	sp.Stats.TotalProcessTime += processingTime
	sp.Stats.AvgProcessTime = sp.Stats.TotalProcessTime / time.Duration(processedItems)

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
		sp.Stats.ErrorItems.Add(1)

		stageStats.mu.Lock()
		stageStats.ErrorItems++
		stageStats.mu.Unlock()

		log.Printf("Stage %d (%s) Processor %d: processing error: %v", sp.StageIndex, sp.StageName, sp.ProcessorID, err)
		return nil // エラーの場合は次の段階に送信しない
	}

	sp.Stats.SuccessItems.Add(1)

	stageStats.mu.Lock()
	stageStats.SuccessItems++
	stageStats.mu.Unlock()

	return result
}

// SubmitData データを処理パイプラインに送信します
func (msp *MultiStagePipeline) SubmitData(data any) error {
	if !msp.isRunning.Load() {
		return fmt.Errorf("pipeline is not running")
	}
	// stageChannels[0]はcloseしないので送信自体はpanicしないが、停止後の投入は
	// 処理されずに残ってしまうため、ctx側でも早期に拒否する。
	if msp.ctx.Err() != nil {
		return fmt.Errorf("pipeline is shutting down")
	}

	select {
	case msp.stageChannels[0] <- data:
		msp.stats.totalInputItems.Add(1)
		return nil
	case <-msp.ctx.Done():
		return fmt.Errorf("pipeline is shutting down")
	default:
		return fmt.Errorf("input channel is full")
	}
}

// GetOutputChannel 出力チャネルを取得します
func (msp *MultiStagePipeline) GetOutputChannel() <-chan any {
	return msp.stageChannels[len(msp.stages)]
}

// collectResults 結果を収集します
func (msp *MultiStagePipeline) collectResults() {
	log.Println("Result collector started")

	outputChannel := msp.stageChannels[len(msp.stages)]

	for {
		select {
		case <-msp.ctx.Done():
			log.Println("Result collector stopping due to context cancellation")
			return
		case result := <-outputChannel:
			// 統計更新
			msp.stats.totalOutputItems.Add(1)

			// 結果処理（ログ出力など）
			log.Printf("Pipeline output: %+v", result)
		}
	}
}

// monitorMetrics メトリクスを監視します
func (msp *MultiStagePipeline) monitorMetrics() {
	tick := time.Tick(15 * time.Second)

	log.Println("Metrics monitor started")

	for {
		select {
		case <-msp.ctx.Done():
			log.Println("Metrics monitor stopping")
			return
		case <-tick:
			msp.reportMetrics()
		}
	}
}

// reportMetrics メトリクスを報告します
func (msp *MultiStagePipeline) reportMetrics() {
	msp.stats.mu.RLock()
	defer msp.stats.mu.RUnlock()

	totalInput := msp.stats.totalInputItems.Load()
	totalOutput := msp.stats.totalOutputItems.Load()
	totalErrors := msp.stats.totalErrors.Load()

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

// Shutdown パイプラインを停止します
func (msp *MultiStagePipeline) Shutdown(timeout time.Duration) error {
	if !msp.isRunning.CompareAndSwap(true, false) {
		return fmt.Errorf("pipeline is not running")
	}

	log.Println("Shutting down multi-stage pipeline...")

	// ワーカーに停止を指示する。stageChannelsはどれもcloseしない
	// （SubmitDataや前段のプロセッサーとの競合でpanic: send on closed channelになるため。
	//   以前は2秒間隔でチャネルを連鎖closeしていたが、固定Sleepでは処理完了を保証できず、
	//   送信側と競合してpanicする余地があった）。
	msp.cancel()

	// ワーカーの終了を待機
	done := make(chan struct{})
	go func() {
		msp.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("All pipeline workers stopped gracefully")
	case <-time.After(timeout):
		log.Println("Some workers may not have stopped properly")
	}

	log.Println("Multi-stage pipeline shutdown completed")
	return nil
}

// GetStats 統計情報を取得します
func (msp *MultiStagePipeline) GetStats() map[string]any {
	msp.stats.mu.RLock()
	defer msp.stats.mu.RUnlock()

	totalInput := msp.stats.totalInputItems.Load()
	totalOutput := msp.stats.totalOutputItems.Load()
	totalErrors := msp.stats.totalErrors.Load()

	uptime := time.Since(msp.stats.startTime)
	var throughput float64
	if uptime.Seconds() > 0 {
		throughput = float64(totalOutput) / uptime.Seconds()
	}

	stageStats := make(map[string]any)
	for i, stats := range msp.stageStats {
		stats.mu.RLock()
		stageStats[fmt.Sprintf("stage_%d_%s", i, stats.StageName)] = map[string]any{
			"processed_items":      stats.ProcessedItems,
			"success_items":        stats.SuccessItems,
			"error_items":          stats.ErrorItems,
			"average_process_time": stats.AvgProcessTime,
			"worker_count":         stats.WorkerCount,
		}
		stats.mu.RUnlock()
	}

	return map[string]any{
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

// DataValidation 第1段階のデータ検証処理です
func DataValidation(ctx context.Context, data any) (any, error) {
	task, ok := data.(DataProcessingTask)
	if !ok {
		return nil, fmt.Errorf("invalid data type")
	}

	// データ検証をシミュレート
	time.Sleep(time.Duration(rand.IntN(50)+10) * time.Millisecond)

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

// DataTransformation 第2段階のデータ変換処理です
func DataTransformation(ctx context.Context, data any) (any, error) {
	task, ok := data.(DataProcessingTask)
	if !ok {
		return nil, fmt.Errorf("invalid data type")
	}

	// データ変換をシミュレート
	time.Sleep(time.Duration(rand.IntN(100)+50) * time.Millisecond)

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

// DataEnrichment 第3段階のデータ拡張処理です
func DataEnrichment(ctx context.Context, data any) (any, error) {
	task, ok := data.(DataProcessingTask)
	if !ok {
		return nil, fmt.Errorf("invalid data type")
	}

	// データ拡張をシミュレート（外部API呼び出しなど）
	time.Sleep(time.Duration(rand.IntN(200)+100) * time.Millisecond)

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
	enrichedTask.Metadata["external_data"] = map[string]any{
		"source":    "external_api",
		"timestamp": time.Now().Unix(),
		"version":   "1.0",
	}

	return enrichedTask, nil
}

// DataOutput 第4段階の出力処理です
func DataOutput(ctx context.Context, data any) (any, error) {
	task, ok := data.(DataProcessingTask)
	if !ok {
		return nil, fmt.Errorf("invalid data type")
	}

	// 出力処理をシミュレート
	time.Sleep(time.Duration(rand.IntN(30)+10) * time.Millisecond)

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
		for i := range 1000 {
			task := DataProcessingTask{
				ID:       int64(i),
				Data:     []byte(fmt.Sprintf("task_data_%d", i)),
				Priority: rand.IntN(5),
				Deadline: time.Now().Add(time.Duration(rand.IntN(60)+30) * time.Second),
				Metadata: map[string]any{
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
			time.Sleep(time.Duration(rand.IntN(50)+10) * time.Millisecond)
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
