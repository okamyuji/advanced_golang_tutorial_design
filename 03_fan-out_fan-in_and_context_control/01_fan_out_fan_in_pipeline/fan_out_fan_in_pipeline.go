package main

import (
	"context"
	"fmt"
	"log"
	"math/rand/v2"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// DataItem 処理対象データです
type DataItem struct {
	ID        int64
	Value     string
	Timestamp time.Time
	Metadata  map[string]any
	Source    string
}

// ProcessedData 処理済みデータです
type ProcessedData struct {
	OriginalID     int64
	ProcessedValue string
	ProcessingTime time.Duration
	ProcessorID    int
	Success        bool
	Error          error
	Timestamp      time.Time
}

// AggregatedResult 集約結果です
type AggregatedResult struct {
	TotalProcessed int64
	SuccessCount   int64
	ErrorCount     int64
	AverageTime    time.Duration
	ThroughputRPS  float64
	ProcessorStats map[int]*ProcessorStatsSnapshot
	StartTime      time.Time
	EndTime        time.Time
}

// ProcessorStats プロセッサー統計です。ワーカーgoroutineと監視goroutineの両方から
// 参照されるため、各カウンタは型付きatomicで保持します。
type ProcessorStats struct {
	ProcessorID    int
	ProcessedCount atomic.Int64
	SuccessCount   atomic.Int64
	ErrorCount     atomic.Int64
	TotalTime      atomic.Int64 // 累積処理時間（ナノ秒）
}

// ProcessorStatsSnapshot ProcessorStats の値コピー可能なスナップショットです
type ProcessorStatsSnapshot struct {
	ProcessorID    int
	ProcessedCount int64
	SuccessCount   int64
	ErrorCount     int64
	TotalTime      time.Duration
	AverageTime    time.Duration
}

// Snapshot 現在の値をコピーしたスナップショットを返します
func (s *ProcessorStats) Snapshot() ProcessorStatsSnapshot {
	processed := s.ProcessedCount.Load()
	total := time.Duration(s.TotalTime.Load())

	var avg time.Duration
	if processed > 0 {
		avg = total / time.Duration(processed)
	}

	return ProcessorStatsSnapshot{
		ProcessorID:    s.ProcessorID,
		ProcessedCount: processed,
		SuccessCount:   s.SuccessCount.Load(),
		ErrorCount:     s.ErrorCount.Load(),
		TotalTime:      total,
		AverageTime:    avg,
	}
}

// PipelineConfig パイプライン設定です
type PipelineConfig struct {
	WorkerCount     int
	BufferSize      int
	ProcessingDelay time.Duration
	ErrorRate       float64
	TimeoutDuration time.Duration
	RetryAttempts   int
	EnableMetrics   bool
	LogLevel        string
}

// FanOutFanInPipeline Fan-out/Fan-inパイプライン処理システムです
type FanOutFanInPipeline struct {
	config *PipelineConfig

	// チャネル
	inputChannel  chan DataItem
	outputChannel chan ProcessedData
	resultChannel chan AggregatedResult

	// コンテキスト制御
	ctx    context.Context
	cancel context.CancelFunc

	// 同期制御
	wg sync.WaitGroup

	// 統計情報
	metrics *PipelineMetrics

	// ワーカー管理
	workers []*PipelineWorker

	// 状態管理
	isRunning atomic.Bool
	startTime time.Time
}

// PipelineWorker パイプライン処理ワーカーです
type PipelineWorker struct {
	ID        int
	pipeline  *FanOutFanInPipeline
	stats     *ProcessorStats
	processor *DataProcessor
}

// DataProcessor データ処理エンジンです
type DataProcessor struct {
	ID             int
	config         *PipelineConfig
	processedCount atomic.Int64
	errorCount     atomic.Int64
}

// PipelineMetrics パイプライン監視メトリクスです
type PipelineMetrics struct {
	mu               sync.RWMutex
	totalInputItems  atomic.Int64
	totalOutputItems atomic.Int64
	errorCount       atomic.Int64
	startTime        time.Time
	processorMetrics map[int]*ProcessorStats
}

// NewPipelineConfig デフォルト設定を作成します
func NewPipelineConfig() *PipelineConfig {
	return &PipelineConfig{
		WorkerCount:     runtime.GOMAXPROCS(0),
		BufferSize:      1000,
		ProcessingDelay: 10 * time.Millisecond,
		ErrorRate:       0.05, // 5%のエラー率
		TimeoutDuration: 30 * time.Second,
		RetryAttempts:   3,
		EnableMetrics:   true,
		LogLevel:        "INFO",
	}
}

// NewFanOutFanInPipeline 新しいパイプラインを作成します
func NewFanOutFanInPipeline(config *PipelineConfig) *FanOutFanInPipeline {
	ctx, cancel := context.WithCancel(context.Background())

	pipeline := &FanOutFanInPipeline{
		config:        config,
		inputChannel:  make(chan DataItem, config.BufferSize),
		outputChannel: make(chan ProcessedData, config.BufferSize),
		resultChannel: make(chan AggregatedResult, 10),
		ctx:           ctx,
		cancel:        cancel,
		metrics: &PipelineMetrics{
			processorMetrics: make(map[int]*ProcessorStats),
		},
		workers: make([]*PipelineWorker, config.WorkerCount),
	}

	// ワーカーを初期化
	for i := range config.WorkerCount {
		worker := &PipelineWorker{
			ID:       i,
			pipeline: pipeline,
			stats: &ProcessorStats{
				ProcessorID: i,
			},
			processor: &DataProcessor{
				ID:     i,
				config: config,
			},
		}
		pipeline.workers[i] = worker
		pipeline.metrics.processorMetrics[i] = worker.stats
	}

	return pipeline
}

// Start パイプライン処理を開始します
func (p *FanOutFanInPipeline) Start() error {
	if !p.isRunning.CompareAndSwap(false, true) {
		return fmt.Errorf("pipeline is already running")
	}

	p.startTime = time.Now()
	p.metrics.startTime = p.startTime

	log.Printf("Starting Fan-out/Fan-in pipeline with %d workers", p.config.WorkerCount)

	// Fan-out: ワーカーを開始（複数のgoroutineでデータを並列処理）
	for _, worker := range p.workers {
		p.wg.Go(worker.run)
	}

	// Fan-in: 結果集約を開始（複数のワーカーからの結果を集約）
	p.wg.Go(p.aggregateResults)

	// メトリクス監視を開始
	if p.config.EnableMetrics {
		p.wg.Go(p.monitorMetrics)
	}

	log.Printf("Pipeline started successfully with %d workers", len(p.workers))
	return nil
}

// run ワーカーのメインループです
func (w *PipelineWorker) run() {
	log.Printf("Worker %d started", w.ID)

	for {
		select {
		case <-w.pipeline.ctx.Done():
			log.Printf("Worker %d stopping due to context cancellation", w.ID)
			return
		case item := <-w.pipeline.inputChannel:
			// データ処理実行
			result := w.processData(item)

			// 結果を出力チャネルに送信
			select {
			case w.pipeline.outputChannel <- result:
				// 正常に送信完了
			case <-w.pipeline.ctx.Done():
				log.Printf("Worker %d: context cancelled while sending result", w.ID)
				return
			}
		}
	}
}

// processData データ処理を実行します
func (w *PipelineWorker) processData(item DataItem) ProcessedData {
	start := time.Now()

	result := ProcessedData{
		OriginalID:  item.ID,
		ProcessorID: w.ID,
		Timestamp:   start,
	}

	// 設定された遅延をシミュレート（実際の処理時間をシミュレート）
	if w.pipeline.config.ProcessingDelay > 0 {
		time.Sleep(w.pipeline.config.ProcessingDelay)
	}

	// エラー率に基づいてランダムにエラーを発生
	if rand.Float64() < w.pipeline.config.ErrorRate {
		result.Success = false
		result.Error = fmt.Errorf("processing error in worker %d for item %d", w.ID, item.ID)
		w.processor.errorCount.Add(1)
		w.stats.ErrorCount.Add(1)
	} else {
		// 正常処理
		result.Success = true
		result.ProcessedValue = fmt.Sprintf("processed_%s_by_worker_%d", item.Value, w.ID)
		w.stats.SuccessCount.Add(1)
	}

	processingTime := time.Since(start)
	result.ProcessingTime = processingTime

	// 統計更新
	w.processor.processedCount.Add(1)
	w.stats.ProcessedCount.Add(1)
	w.stats.TotalTime.Add(int64(processingTime))

	return result
}

// aggregateResults 結果を集約します（Fan-in）
func (p *FanOutFanInPipeline) aggregateResults() {
	log.Println("Result aggregator started")

	var (
		totalProcessed int64
		successCount   int64
		errorCount     int64
		totalTime      time.Duration
		itemCount      int64
	)

	// 定期的に統計を集計・報告。Stop/Resetは使わず参照もしないので、GCが回収できるtime.Tickでよい。
	tick := time.Tick(5 * time.Second)

	for {
		select {
		case <-p.ctx.Done():
			log.Println("Result aggregator stopping due to context cancellation")
			// 最終結果を計算して送信
			p.sendFinalAggregatedResult(totalProcessed, successCount, errorCount, totalTime, itemCount)
			return

		case result, ok := <-p.outputChannel:
			if !ok {
				log.Println("Result aggregator stopping due to output channel closure")
				// 最終結果を計算して送信
				p.sendFinalAggregatedResult(totalProcessed, successCount, errorCount, totalTime, itemCount)
				return
			}

			// 結果を集約
			totalProcessed++
			itemCount++
			totalTime += result.ProcessingTime

			if result.Success {
				successCount++
			} else {
				errorCount++
				if result.Error != nil {
					log.Printf("Processing error: %v", result.Error)
				}
			}

			// メトリクス更新
			p.metrics.totalOutputItems.Add(1)
			if result.Error != nil {
				p.metrics.errorCount.Add(1)
			}

		case <-tick:
			// 定期的な統計報告
			if totalProcessed > 0 {
				avgTime := totalTime / time.Duration(totalProcessed)
				successRate := float64(successCount) / float64(totalProcessed) * 100

				log.Printf("Pipeline Progress: Processed=%d, Success=%.1f%%, AvgTime=%v",
					totalProcessed, successRate, avgTime)
			}
		}
	}
}

// sendFinalAggregatedResult 最終集約結果を送信します
func (p *FanOutFanInPipeline) sendFinalAggregatedResult(totalProcessed, successCount, errorCount int64, totalTime time.Duration, itemCount int64) {
	endTime := time.Now()
	duration := endTime.Sub(p.startTime)

	var avgTime time.Duration
	var throughput float64

	if totalProcessed > 0 {
		avgTime = totalTime / time.Duration(totalProcessed)
		throughput = float64(totalProcessed) / duration.Seconds()
	}

	result := AggregatedResult{
		TotalProcessed: itemCount, // 総アイテム数を使用
		SuccessCount:   successCount,
		ErrorCount:     errorCount,
		AverageTime:    avgTime,
		ThroughputRPS:  throughput,
		ProcessorStats: make(map[int]*ProcessorStatsSnapshot),
		StartTime:      p.startTime,
		EndTime:        endTime,
	}

	// プロセッサー統計のスナップショットをコピー
	for id, stats := range p.metrics.processorMetrics {
		snap := stats.Snapshot()
		result.ProcessorStats[id] = &snap
	}

	select {
	case p.resultChannel <- result:
		log.Printf("Final aggregated result sent: Total=%d, Success=%d, Error=%d, Throughput=%.2f RPS",
			totalProcessed, successCount, errorCount, throughput)
	case <-time.After(1 * time.Second):
		log.Println("Failed to send final aggregated result due to timeout")
	}
}

// SubmitData データを処理キューに追加します
func (p *FanOutFanInPipeline) SubmitData(item DataItem) error {
	if !p.isRunning.Load() {
		return fmt.Errorf("pipeline is not running")
	}
	// inputChannel closeしないので送信自体はpanicしないが、停止後の投入は
	// 処理されずに残ってしまうため、ctx側でも早期に拒否する。
	if p.ctx.Err() != nil {
		return fmt.Errorf("pipeline is shutting down")
	}

	select {
	case p.inputChannel <- item:
		p.metrics.totalInputItems.Add(1)
		return nil
	case <-p.ctx.Done():
		return fmt.Errorf("pipeline is shutting down")
	default:
		return fmt.Errorf("input channel is full")
	}
}

// GetResultChannel 結果チャネルを取得します
func (p *FanOutFanInPipeline) GetResultChannel() <-chan AggregatedResult {
	return p.resultChannel
}

// monitorMetrics メトリクスを監視します
func (p *FanOutFanInPipeline) monitorMetrics() {
	tick := time.Tick(10 * time.Second)

	log.Println("Metrics monitor started")

	for {
		select {
		case <-p.ctx.Done():
			log.Println("Metrics monitor stopping")
			return
		case <-tick:
			p.reportMetrics()
		}
	}
}

// reportMetrics メトリクスを報告します
func (p *FanOutFanInPipeline) reportMetrics() {
	p.metrics.mu.RLock()
	defer p.metrics.mu.RUnlock()

	totalInput := p.metrics.totalInputItems.Load()
	totalOutput := p.metrics.totalOutputItems.Load()
	errorCount := p.metrics.errorCount.Load()

	duration := time.Since(p.metrics.startTime)
	var throughput float64
	if duration.Seconds() > 0 {
		throughput = float64(totalOutput) / duration.Seconds()
	}

	log.Printf("Pipeline Metrics: Input=%d, Output=%d, Errors=%d, Throughput=%.2f RPS, Uptime=%v",
		totalInput, totalOutput, errorCount, throughput, duration)

	// ワーカー別統計
	for _, worker := range p.workers {
		snap := worker.stats.Snapshot()

		if snap.ProcessedCount > 0 {
			successRate := float64(snap.SuccessCount) / float64(snap.ProcessedCount) * 100
			log.Printf("Worker %d: Processed=%d, Success=%.1f%%, Errors=%d, AvgTime=%v",
				worker.ID, snap.ProcessedCount, successRate, snap.ErrorCount, snap.AverageTime)
		}
	}
}

// Shutdown パイプラインを停止します
func (p *FanOutFanInPipeline) Shutdown(timeout time.Duration) error {
	if !p.isRunning.CompareAndSwap(true, false) {
		return fmt.Errorf("pipeline is not running")
	}

	log.Println("Shutting down pipeline...")

	// ワーカーに停止を指示する。inputChannelはcloseしない
	// （SubmitDataとの競合でpanic: send on closed channelになるため）。
	p.cancel()

	// ワーカーの終了を待機（タイムアウト付き）
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("All workers stopped gracefully")
	case <-time.After(timeout):
		// 送信側のgoroutineが残っている可能性があるので、チャネルは閉じずにエラーを返す
		return fmt.Errorf("shutdown timeout: some workers may not have stopped")
	}

	// 出力チャネル・結果チャネルへの送信元はwg.Waitの完了で全て止まっているのでcloseしてよい
	close(p.outputChannel)
	close(p.resultChannel)

	log.Println("Pipeline shutdown completed")
	return nil
}

// GetMetrics 現在のメトリクスを取得します
func (p *FanOutFanInPipeline) GetMetrics() map[string]any {
	p.metrics.mu.RLock()
	defer p.metrics.mu.RUnlock()

	totalInput := p.metrics.totalInputItems.Load()
	totalOutput := p.metrics.totalOutputItems.Load()
	errorCount := p.metrics.errorCount.Load()

	duration := time.Since(p.metrics.startTime)
	var throughput float64
	if duration.Seconds() > 0 {
		throughput = float64(totalOutput) / duration.Seconds()
	}

	workerStats := make(map[string]any)
	for id, stats := range p.metrics.processorMetrics {
		snap := stats.Snapshot()
		workerStats[fmt.Sprintf("worker_%d", id)] = map[string]any{
			"processed_count": snap.ProcessedCount,
			"success_count":   snap.SuccessCount,
			"error_count":     snap.ErrorCount,
			"average_time":    snap.AverageTime,
		}
	}

	return map[string]any{
		"total_input":    totalInput,
		"total_output":   totalOutput,
		"error_count":    errorCount,
		"throughput_rps": throughput,
		"uptime":         duration,
		"worker_count":   len(p.workers),
		"worker_stats":   workerStats,
	}
}

func main() {
	// パイプライン設定
	config := NewPipelineConfig()
	config.WorkerCount = 4
	config.BufferSize = 500
	config.ProcessingDelay = 50 * time.Millisecond
	config.ErrorRate = 0.1 // 10%のエラー率

	// パイプライン作成・開始
	pipeline := NewFanOutFanInPipeline(config)
	if err := pipeline.Start(); err != nil {
		log.Fatalf("Failed to start pipeline: %v", err)
	}

	// 結果集約のgoroutineを開始
	go func() {
		for result := range pipeline.GetResultChannel() {
			log.Printf("Aggregated Result: Total=%d, Success=%d, Error=%d, AvgTime=%v, Throughput=%.2f RPS",
				result.TotalProcessed, result.SuccessCount, result.ErrorCount,
				result.AverageTime, result.ThroughputRPS)

			for id, stats := range result.ProcessorStats {
				log.Printf("Processor %d: Processed=%d, Success=%d, Error=%d, AvgTime=%v",
					id, stats.ProcessedCount, stats.SuccessCount, stats.ErrorCount, stats.AverageTime)
			}
		}
	}()

	// テストデータを並行で送信
	go func() {
		for i := range 1000 {
			item := DataItem{
				ID:        int64(i),
				Value:     fmt.Sprintf("data_%d", i),
				Timestamp: time.Now(),
				Metadata: map[string]any{
					"batch_id": i / 100,
					"priority": rand.IntN(5),
				},
				Source: "test_generator",
			}

			if err := pipeline.SubmitData(item); err != nil {
				log.Printf("Failed to submit data %d: %v", i, err)
				break
			}

			// 送信頻度を制御
			time.Sleep(10 * time.Millisecond)
		}

		log.Println("All test data submitted")
	}()

	// 30秒間実行
	time.Sleep(30 * time.Second)

	// 最終メトリクス表示
	metrics := pipeline.GetMetrics()
	log.Printf("Final Metrics: %+v", metrics)

	// グレースフルシャットダウン
	if err := pipeline.Shutdown(10 * time.Second); err != nil {
		log.Printf("Shutdown error: %v", err)
	}
}
