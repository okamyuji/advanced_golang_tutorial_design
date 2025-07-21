package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// DataItemは処理対象データです
type DataItem struct {
	ID        int64
	Value     string
	Timestamp time.Time
	Metadata  map[string]interface{}
	Source    string
}

// ProcessedDataは処理済みデータです
type ProcessedData struct {
	OriginalID     int64
	ProcessedValue string
	ProcessingTime time.Duration
	ProcessorID    int
	Success        bool
	Error          error
	Timestamp      time.Time
}

// AggregatedResultは集約結果です
type AggregatedResult struct {
	TotalProcessed int64
	SuccessCount   int64
	ErrorCount     int64
	AverageTime    time.Duration
	ThroughputRPS  float64
	ProcessorStats map[int]*ProcessorStats
	StartTime      time.Time
	EndTime        time.Time
}

// ProcessorStatsはプロセッサー統計です
type ProcessorStats struct {
	ProcessorID    int
	ProcessedCount int64
	SuccessCount   int64
	ErrorCount     int64
	TotalTime      time.Duration
	AverageTime    time.Duration
}

// PipelineConfigはパイプライン設定です
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

// FanOutFanInPipelineはFan-out/Fan-inパイプライン処理システムです
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
	isRunning int32
	startTime time.Time
}

// PipelineWorkerはパイプライン処理ワーカーです
type PipelineWorker struct {
	ID        int
	pipeline  *FanOutFanInPipeline
	stats     *ProcessorStats
	processor *DataProcessor
}

// DataProcessorはデータ処理エンジンです
type DataProcessor struct {
	ID             int
	config         *PipelineConfig
	processedCount int64
	errorCount     int64
}

// PipelineMetricsはパイプライン監視メトリクスです
type PipelineMetrics struct {
	mu               sync.RWMutex
	totalInputItems  int64
	totalOutputItems int64
	errorCount       int64
	startTime        time.Time
	processorMetrics map[int]*ProcessorStats
}

// NewPipelineConfigはデフォルト設定を作成します
func NewPipelineConfig() *PipelineConfig {
	return &PipelineConfig{
		WorkerCount:     runtime.NumCPU(),
		BufferSize:      1000,
		ProcessingDelay: 10 * time.Millisecond,
		ErrorRate:       0.05, // 5%のエラー率
		TimeoutDuration: 30 * time.Second,
		RetryAttempts:   3,
		EnableMetrics:   true,
		LogLevel:        "INFO",
	}
}

// NewFanOutFanInPipelineは新しいパイプラインを作成します
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
	for i := 0; i < config.WorkerCount; i++ {
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

// Startはパイプライン処理を開始します
func (p *FanOutFanInPipeline) Start() error {
	if !atomic.CompareAndSwapInt32(&p.isRunning, 0, 1) {
		return fmt.Errorf("pipeline is already running")
	}

	p.startTime = time.Now()
	p.metrics.startTime = p.startTime

	log.Printf("Starting Fan-out/Fan-in pipeline with %d workers", p.config.WorkerCount)

	// Fan-out: ワーカーを開始（複数のgoroutineでデータを並列処理）
	for _, worker := range p.workers {
		p.wg.Add(1)
		go worker.run()
	}

	// Fan-in: 結果集約を開始（複数のワーカーからの結果を集約）
	p.wg.Add(1)
	go p.aggregateResults()

	// メトリクス監視を開始
	if p.config.EnableMetrics {
		p.wg.Add(1)
		go p.monitorMetrics()
	}

	log.Printf("Pipeline started successfully with %d workers", len(p.workers))
	return nil
}

// runはワーカーのメインループです
func (w *PipelineWorker) run() {
	defer w.pipeline.wg.Done()

	log.Printf("Worker %d started", w.ID)

	for {
		select {
		case <-w.pipeline.ctx.Done():
			log.Printf("Worker %d stopping due to context cancellation", w.ID)
			return
		case item, ok := <-w.pipeline.inputChannel:
			if !ok {
				log.Printf("Worker %d stopping due to input channel closure", w.ID)
				return
			}

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

// processDataはデータ処理を実行します
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
		atomic.AddInt64(&w.processor.errorCount, 1)
		atomic.AddInt64(&w.stats.ErrorCount, 1)
	} else {
		// 正常処理
		result.Success = true
		result.ProcessedValue = fmt.Sprintf("processed_%s_by_worker_%d", item.Value, w.ID)
		atomic.AddInt64(&w.stats.SuccessCount, 1)
	}

	processingTime := time.Since(start)
	result.ProcessingTime = processingTime

	// 統計更新
	atomic.AddInt64(&w.processor.processedCount, 1)
	atomic.AddInt64(&w.stats.ProcessedCount, 1)
	w.stats.TotalTime += processingTime

	if w.stats.ProcessedCount > 0 {
		w.stats.AverageTime = w.stats.TotalTime / time.Duration(w.stats.ProcessedCount)
	}

	return result
}

// aggregateResultsは結果を集約します（Fan-in）
func (p *FanOutFanInPipeline) aggregateResults() {
	defer p.wg.Done()

	log.Println("Result aggregator started")

	var (
		totalProcessed int64
		successCount   int64
		errorCount     int64
		totalTime      time.Duration
		itemCount      int64
	)

	// 定期的に統計を集計・報告
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

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
			atomic.AddInt64(&p.metrics.totalOutputItems, 1)
			if result.Error != nil {
				atomic.AddInt64(&p.metrics.errorCount, 1)
			}

		case <-ticker.C:
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

// sendFinalAggregatedResultは最終集約結果を送信します
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
		ProcessorStats: make(map[int]*ProcessorStats),
		StartTime:      p.startTime,
		EndTime:        endTime,
	}

	// プロセッサー統計をコピー
	for id, stats := range p.metrics.processorMetrics {
		statsCopy := *stats
		result.ProcessorStats[id] = &statsCopy
	}

	select {
	case p.resultChannel <- result:
		log.Printf("Final aggregated result sent: Total=%d, Success=%d, Error=%d, Throughput=%.2f RPS",
			totalProcessed, successCount, errorCount, throughput)
	case <-time.After(1 * time.Second):
		log.Println("Failed to send final aggregated result due to timeout")
	}
}

// SubmitDataはデータを処理キューに追加します
func (p *FanOutFanInPipeline) SubmitData(item DataItem) error {
	if atomic.LoadInt32(&p.isRunning) == 0 {
		return fmt.Errorf("pipeline is not running")
	}

	select {
	case p.inputChannel <- item:
		atomic.AddInt64(&p.metrics.totalInputItems, 1)
		return nil
	case <-p.ctx.Done():
		return fmt.Errorf("pipeline is shutting down")
	default:
		return fmt.Errorf("input channel is full")
	}
}

// GetResultChannelは結果チャネルを取得します
func (p *FanOutFanInPipeline) GetResultChannel() <-chan AggregatedResult {
	return p.resultChannel
}

// monitorMetricsはメトリクスを監視します
func (p *FanOutFanInPipeline) monitorMetrics() {
	defer p.wg.Done()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	log.Println("Metrics monitor started")

	for {
		select {
		case <-p.ctx.Done():
			log.Println("Metrics monitor stopping")
			return
		case <-ticker.C:
			p.reportMetrics()
		}
	}
}

// reportMetricsはメトリクスを報告します
func (p *FanOutFanInPipeline) reportMetrics() {
	p.metrics.mu.RLock()
	defer p.metrics.mu.RUnlock()

	totalInput := atomic.LoadInt64(&p.metrics.totalInputItems)
	totalOutput := atomic.LoadInt64(&p.metrics.totalOutputItems)
	errorCount := atomic.LoadInt64(&p.metrics.errorCount)

	duration := time.Since(p.metrics.startTime)
	var throughput float64
	if duration.Seconds() > 0 {
		throughput = float64(totalOutput) / duration.Seconds()
	}

	log.Printf("Pipeline Metrics: Input=%d, Output=%d, Errors=%d, Throughput=%.2f RPS, Uptime=%v",
		totalInput, totalOutput, errorCount, throughput, duration)

	// ワーカー別統計
	for _, worker := range p.workers {
		processed := atomic.LoadInt64(&worker.stats.ProcessedCount)
		success := atomic.LoadInt64(&worker.stats.SuccessCount)
		errors := atomic.LoadInt64(&worker.stats.ErrorCount)

		if processed > 0 {
			successRate := float64(success) / float64(processed) * 100
			log.Printf("Worker %d: Processed=%d, Success=%.1f%%, Errors=%d, AvgTime=%v",
				worker.ID, processed, successRate, errors, worker.stats.AverageTime)
		}
	}
}

// Shutdownはパイプラインを停止します
func (p *FanOutFanInPipeline) Shutdown(timeout time.Duration) error {
	if !atomic.CompareAndSwapInt32(&p.isRunning, 1, 0) {
		return fmt.Errorf("pipeline is not running")
	}

	log.Println("Shutting down pipeline...")

	// 1. 入力チャネルを閉じて新しいデータの受付を停止
	close(p.inputChannel)

	// 2. ワーカーの終了を待機（タイムアウト付き）
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("All workers stopped gracefully")
	case <-time.After(timeout):
		log.Println("Timeout reached, forcing shutdown...")
		p.cancel()

		// 追加の待機時間
		select {
		case <-done:
			log.Println("Workers stopped after cancellation")
		case <-time.After(2 * time.Second):
			log.Println("Some workers may not have stopped properly")
		}
	}

	// 3. 出力チャネルを閉じる
	close(p.outputChannel)

	// 4. 結果チャネルを閉じる
	close(p.resultChannel)

	log.Println("Pipeline shutdown completed")
	return nil
}

// GetMetricsは現在のメトリクスを取得します
func (p *FanOutFanInPipeline) GetMetrics() map[string]interface{} {
	p.metrics.mu.RLock()
	defer p.metrics.mu.RUnlock()

	totalInput := atomic.LoadInt64(&p.metrics.totalInputItems)
	totalOutput := atomic.LoadInt64(&p.metrics.totalOutputItems)
	errorCount := atomic.LoadInt64(&p.metrics.errorCount)

	duration := time.Since(p.metrics.startTime)
	var throughput float64
	if duration.Seconds() > 0 {
		throughput = float64(totalOutput) / duration.Seconds()
	}

	workerStats := make(map[string]interface{})
	for id, stats := range p.metrics.processorMetrics {
		workerStats[fmt.Sprintf("worker_%d", id)] = map[string]interface{}{
			"processed_count": atomic.LoadInt64(&stats.ProcessedCount),
			"success_count":   atomic.LoadInt64(&stats.SuccessCount),
			"error_count":     atomic.LoadInt64(&stats.ErrorCount),
			"average_time":    stats.AverageTime,
		}
	}

	return map[string]interface{}{
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
		for i := 0; i < 1000; i++ {
			item := DataItem{
				ID:        int64(i),
				Value:     fmt.Sprintf("data_%d", i),
				Timestamp: time.Now(),
				Metadata: map[string]interface{}{
					"batch_id": i / 100,
					"priority": rand.Intn(5),
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
