package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// StreamEventはストリームイベントです
type StreamEvent struct {
	ID        int64
	EventType string
	Timestamp time.Time
	UserID    string
	SessionID string
	Value     float64
	Data      map[string]interface{}
	Source    string
}

// AggregationWindowは集約ウィンドウです
type AggregationWindow struct {
	WindowID    string
	StartTime   time.Time
	EndTime     time.Time
	WindowSize  time.Duration
	EventCount  int64
	TotalValue  float64
	MinValue    float64
	MaxValue    float64
	AvgValue    float64
	MedianValue float64
	Events      []StreamEvent
	UserCounts  map[string]int64
	TypeCounts  map[string]int64
}

// StreamAggregatorはストリーム集約システムです
type StreamAggregator struct {
	// 基本設定
	windowSize    time.Duration
	slideInterval time.Duration
	maxWindowSize int
	workerCount   int

	// チャネル
	eventChannel  chan StreamEvent
	windowChannel chan *AggregationWindow
	resultChannel chan *AggregationResult

	// コンテキスト制御
	ctx    context.Context
	cancel context.CancelFunc

	// 同期制御
	wg sync.WaitGroup

	// ウィンドウ管理
	windows     map[string]*AggregationWindow
	windowMutex sync.RWMutex

	// ワーカー管理
	workers []*AggregationWorker

	// 統計・監視
	stats     *AggregatorStats
	isRunning int32
	startTime time.Time

	// ウィンドウ清理
	cleanupTicker *time.Ticker
}

// AggregationWorkerは集約処理ワーカーです
type AggregationWorker struct {
	ID         int
	aggregator *StreamAggregator
	processor  *EventProcessor
	stats      *WorkerStats
}

// EventProcessorはイベント処理エンジンです
type EventProcessor struct {
	ID               int
	processedEvents  int64
	processedWindows int64
}

// AggregationResultは集約結果です
type AggregationResult struct {
	WindowID       string
	StartTime      time.Time
	EndTime        time.Time
	EventCount     int64
	TotalValue     float64
	AverageValue   float64
	MinValue       float64
	MaxValue       float64
	MedianValue    float64
	StandardDev    float64
	UniqueUsers    int64
	TopEventTypes  []EventTypeCount
	ProcessingTime time.Duration
	WorkerID       int
	CompletedAt    time.Time
}

// EventTypeCountはイベントタイプ別カウントです
type EventTypeCount struct {
	EventType  string
	Count      int64
	Percentage float64
}

// AggregatorStatsは集約システム統計です
type AggregatorStats struct {
	mu                     sync.RWMutex
	totalEvents            int64
	totalWindows           int64
	totalResultsGenerated  int64
	averageEventsPerWindow float64
	averageProcessingTime  time.Duration
	startTime              time.Time
	workerStats            map[int]*WorkerStats
}

// WorkerStatsはワーカー統計です
type WorkerStats struct {
	WorkerID              int
	ProcessedEvents       int64
	ProcessedWindows      int64
	TotalProcessingTime   time.Duration
	AverageProcessingTime time.Duration
}

// AggregationConfigは集約設定です
type AggregationConfig struct {
	WindowSize      time.Duration
	SlideInterval   time.Duration
	MaxWindowSize   int
	WorkerCount     int
	BufferSize      int
	CleanupInterval time.Duration
	EnableMetrics   bool
	MetricsInterval time.Duration
}

// NewAggregationConfigはデフォルト集約設定を作成します
func NewAggregationConfig() *AggregationConfig {
	return &AggregationConfig{
		WindowSize:      30 * time.Second,
		SlideInterval:   10 * time.Second,
		MaxWindowSize:   10000,
		WorkerCount:     4,
		BufferSize:      5000,
		CleanupInterval: 60 * time.Second,
		EnableMetrics:   true,
		MetricsInterval: 15 * time.Second,
	}
}

// NewStreamAggregatorは新しいストリーム集約システムを作成します
func NewStreamAggregator(config *AggregationConfig) *StreamAggregator {
	ctx, cancel := context.WithCancel(context.Background())

	aggregator := &StreamAggregator{
		windowSize:    config.WindowSize,
		slideInterval: config.SlideInterval,
		maxWindowSize: config.MaxWindowSize,
		workerCount:   config.WorkerCount,
		eventChannel:  make(chan StreamEvent, config.BufferSize),
		windowChannel: make(chan *AggregationWindow, config.WorkerCount*2),
		resultChannel: make(chan *AggregationResult, config.WorkerCount*2),
		ctx:           ctx,
		cancel:        cancel,
		windows:       make(map[string]*AggregationWindow),
		workers:       make([]*AggregationWorker, config.WorkerCount),
		stats: &AggregatorStats{
			workerStats: make(map[int]*WorkerStats),
			startTime:   time.Now(),
		},
		cleanupTicker: time.NewTicker(config.CleanupInterval),
	}

	// ワーカーを初期化
	for i := 0; i < config.WorkerCount; i++ {
		worker := &AggregationWorker{
			ID:         i,
			aggregator: aggregator,
			processor: &EventProcessor{
				ID: i,
			},
			stats: &WorkerStats{
				WorkerID: i,
			},
		}
		aggregator.workers[i] = worker
		aggregator.stats.workerStats[i] = worker.stats
	}

	return aggregator
}

// Startは集約システムを開始します
func (sa *StreamAggregator) Start() error {
	if !atomic.CompareAndSwapInt32(&sa.isRunning, 0, 1) {
		return fmt.Errorf("aggregator is already running")
	}

	sa.startTime = time.Now()
	sa.stats.startTime = sa.startTime

	log.Printf("Starting stream aggregator with %d workers", sa.workerCount)

	// イベント分散器を開始
	sa.wg.Add(1)
	go sa.distributeEvents()

	// ウィンドウ管理を開始
	sa.wg.Add(1)
	go sa.manageWindows()

	// 集約ワーカーを開始
	for _, worker := range sa.workers {
		sa.wg.Add(1)
		go worker.run()
	}

	// 結果処理を開始
	sa.wg.Add(1)
	go sa.handleResults()

	// ウィンドウ清理を開始
	sa.wg.Add(1)
	go sa.cleanupWindows()

	// メトリクス監視を開始
	sa.wg.Add(1)
	go sa.monitorMetrics()

	log.Printf("Stream aggregator started successfully")
	return nil
}

// distributeEventsはイベントを分散します
func (sa *StreamAggregator) distributeEvents() {
	defer sa.wg.Done()

	log.Println("Event distributor started")

	for {
		select {
		case <-sa.ctx.Done():
			log.Println("Event distributor stopping due to context cancellation")
			return
		case event, ok := <-sa.eventChannel:
			if !ok {
				log.Println("Event distributor stopping due to event channel closure")
				return
			}

			// イベントを適切なウィンドウに分配
			sa.assignEventToWindows(event)

			// 統計更新
			atomic.AddInt64(&sa.stats.totalEvents, 1)
		}
	}
}

// assignEventToWindowsはイベントをウィンドウに割り当てます
func (sa *StreamAggregator) assignEventToWindows(event StreamEvent) {
	sa.windowMutex.Lock()
	defer sa.windowMutex.Unlock()

	// 現在時刻に基づいてウィンドウを計算
	now := event.Timestamp
	windowStart := now.Truncate(sa.slideInterval)

	// スライディングウィンドウの複数のウィンドウに割り当て
	for i := 0; i < int(sa.windowSize/sa.slideInterval); i++ {
		start := windowStart.Add(-time.Duration(i) * sa.slideInterval)
		end := start.Add(sa.windowSize)

		// イベントがウィンドウの範囲内にある場合
		if !event.Timestamp.Before(start) && event.Timestamp.Before(end) {
			windowID := fmt.Sprintf("%d", start.Unix())

			// ウィンドウが存在しない場合は作成
			if _, exists := sa.windows[windowID]; !exists {
				sa.windows[windowID] = &AggregationWindow{
					WindowID:   windowID,
					StartTime:  start,
					EndTime:    end,
					WindowSize: sa.windowSize,
					Events:     make([]StreamEvent, 0),
					UserCounts: make(map[string]int64),
					TypeCounts: make(map[string]int64),
					MinValue:   math.Inf(1),
					MaxValue:   math.Inf(-1),
				}
			}

			window := sa.windows[windowID]

			// イベントをウィンドウに追加
			window.Events = append(window.Events, event)
			window.EventCount++
			window.TotalValue += event.Value

			// 最小・最大値を更新
			if event.Value < window.MinValue {
				window.MinValue = event.Value
			}
			if event.Value > window.MaxValue {
				window.MaxValue = event.Value
			}

			// ユーザー別・タイプ別カウントを更新
			window.UserCounts[event.UserID]++
			window.TypeCounts[event.EventType]++

			// ウィンドウサイズが上限に達した場合は処理キューに送信
			if len(window.Events) >= sa.maxWindowSize ||
				time.Since(window.StartTime) >= sa.windowSize {

				// ウィンドウのコピーを作成してワーカーに送信
				windowCopy := sa.copyWindow(window)

				select {
				case sa.windowChannel <- windowCopy:
					// ウィンドウを削除
					delete(sa.windows, windowID)
				case <-sa.ctx.Done():
					return
				default:
					// チャネルが満杯の場合はログ出力
					log.Printf("Window channel is full, dropping window %s", windowID)
				}
			}
		}
	}
}

// copyWindowはウィンドウのコピーを作成します
func (sa *StreamAggregator) copyWindow(window *AggregationWindow) *AggregationWindow {
	copy := &AggregationWindow{
		WindowID:   window.WindowID,
		StartTime:  window.StartTime,
		EndTime:    window.EndTime,
		WindowSize: window.WindowSize,
		EventCount: window.EventCount,
		TotalValue: window.TotalValue,
		MinValue:   window.MinValue,
		MaxValue:   window.MaxValue,
		Events:     make([]StreamEvent, len(window.Events)),
		UserCounts: make(map[string]int64),
		TypeCounts: make(map[string]int64),
	}

	// イベントをコピー
	copy.Events = append(copy.Events, window.Events...)

	// カウントをコピー
	for k, v := range window.UserCounts {
		copy.UserCounts[k] = v
	}
	for k, v := range window.TypeCounts {
		copy.TypeCounts[k] = v
	}

	return copy
}

// runはワーカーのメインループです
func (aw *AggregationWorker) run() {
	defer aw.aggregator.wg.Done()

	log.Printf("Aggregation worker %d started", aw.ID)

	for {
		select {
		case <-aw.aggregator.ctx.Done():
			log.Printf("Worker %d stopping due to context cancellation", aw.ID)
			return
		case window, ok := <-aw.aggregator.windowChannel:
			if !ok {
				log.Printf("Worker %d stopping due to window channel closure", aw.ID)
				return
			}

			// ウィンドウを処理
			result := aw.processWindow(window)

			// 結果を送信
			select {
			case aw.aggregator.resultChannel <- result:
				// 正常に送信完了
			case <-aw.aggregator.ctx.Done():
				log.Printf("Worker %d: context cancelled while sending result", aw.ID)
				return
			}
		}
	}
}

// processWindowはウィンドウを処理します
func (aw *AggregationWorker) processWindow(window *AggregationWindow) *AggregationResult {
	start := time.Now()

	result := &AggregationResult{
		WindowID:    window.WindowID,
		StartTime:   window.StartTime,
		EndTime:     window.EndTime,
		EventCount:  window.EventCount,
		TotalValue:  window.TotalValue,
		MinValue:    window.MinValue,
		MaxValue:    window.MaxValue,
		WorkerID:    aw.ID,
		CompletedAt: time.Now(),
	}

	// 平均値を計算
	if window.EventCount > 0 {
		result.AverageValue = window.TotalValue / float64(window.EventCount)
	}

	// 中央値を計算
	if len(window.Events) > 0 {
		values := make([]float64, len(window.Events))
		for i, event := range window.Events {
			values[i] = event.Value
		}
		sort.Float64s(values)

		n := len(values)
		if n%2 == 0 {
			result.MedianValue = (values[n/2-1] + values[n/2]) / 2
		} else {
			result.MedianValue = values[n/2]
		}

		// 標準偏差を計算
		result.StandardDev = aw.calculateStandardDeviation(values, result.AverageValue)
	}

	// ユニークユーザー数
	result.UniqueUsers = int64(len(window.UserCounts))

	// トップイベントタイプを計算
	result.TopEventTypes = aw.calculateTopEventTypes(window.TypeCounts, window.EventCount)

	processingTime := time.Since(start)
	result.ProcessingTime = processingTime

	// 統計更新
	atomic.AddInt64(&aw.stats.ProcessedEvents, window.EventCount)
	atomic.AddInt64(&aw.stats.ProcessedWindows, 1)
	aw.stats.TotalProcessingTime += processingTime

	if aw.stats.ProcessedWindows > 0 {
		aw.stats.AverageProcessingTime = aw.stats.TotalProcessingTime / time.Duration(aw.stats.ProcessedWindows)
	}

	atomic.AddInt64(&aw.processor.processedEvents, window.EventCount)
	atomic.AddInt64(&aw.processor.processedWindows, 1)

	return result
}

// calculateStandardDeviationは標準偏差を計算します
func (aw *AggregationWorker) calculateStandardDeviation(values []float64, mean float64) float64 {
	if len(values) <= 1 {
		return 0
	}

	sum := 0.0
	for _, value := range values {
		diff := value - mean
		sum += diff * diff
	}

	variance := sum / float64(len(values))
	return math.Sqrt(variance)
}

// calculateTopEventTypesはトップイベントタイプを計算します
func (aw *AggregationWorker) calculateTopEventTypes(typeCounts map[string]int64, totalCount int64) []EventTypeCount {
	type typeCount struct {
		eventType string
		count     int64
	}

	var types []typeCount
	for eventType, count := range typeCounts {
		types = append(types, typeCount{eventType: eventType, count: count})
	}

	// カウント順でソート
	sort.Slice(types, func(i, j int) bool {
		return types[i].count > types[j].count
	})

	// 上位5つを取得
	maxTypes := 5
	if len(types) < maxTypes {
		maxTypes = len(types)
	}

	result := make([]EventTypeCount, maxTypes)
	for i := 0; i < maxTypes; i++ {
		percentage := float64(types[i].count) / float64(totalCount) * 100
		result[i] = EventTypeCount{
			EventType:  types[i].eventType,
			Count:      types[i].count,
			Percentage: percentage,
		}
	}

	return result
}

// manageWindowsはウィンドウを管理します
func (sa *StreamAggregator) manageWindows() {
	defer sa.wg.Done()

	ticker := time.NewTicker(sa.slideInterval)
	defer ticker.Stop()

	log.Println("Window manager started")

	for {
		select {
		case <-sa.ctx.Done():
			log.Println("Window manager stopping due to context cancellation")
			return
		case <-ticker.C:
			sa.checkAndFlushWindows()
		}
	}
}

// checkAndFlushWindowsは期限切れウィンドウをフラッシュします
func (sa *StreamAggregator) checkAndFlushWindows() {
	sa.windowMutex.Lock()
	defer sa.windowMutex.Unlock()

	now := time.Now()
	var toFlush []string

	for windowID, window := range sa.windows {
		// ウィンドウの期限をチェック
		if now.Sub(window.StartTime) >= sa.windowSize {
			// 処理キューに送信
			windowCopy := sa.copyWindow(window)

			select {
			case sa.windowChannel <- windowCopy:
				toFlush = append(toFlush, windowID)
			case <-sa.ctx.Done():
				return
			default:
				log.Printf("Window channel is full, keeping window %s", windowID)
			}
		}
	}

	// フラッシュされたウィンドウを削除
	for _, windowID := range toFlush {
		delete(sa.windows, windowID)
	}

	if len(toFlush) > 0 {
		log.Printf("Flushed %d expired windows", len(toFlush))
	}
}

// SubmitEventはイベントをキューに追加します
func (sa *StreamAggregator) SubmitEvent(event StreamEvent) error {
	if atomic.LoadInt32(&sa.isRunning) == 0 {
		return fmt.Errorf("aggregator is not running")
	}

	// タイムスタンプが設定されていない場合は現在時刻を使用
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now()
	}

	select {
	case sa.eventChannel <- event:
		return nil
	case <-sa.ctx.Done():
		return fmt.Errorf("aggregator is shutting down")
	default:
		return fmt.Errorf("event channel is full")
	}
}

// GetResultChannelは結果チャネルを取得します
func (sa *StreamAggregator) GetResultChannel() <-chan *AggregationResult {
	return sa.resultChannel
}

// handleResultsは結果を処理します
func (sa *StreamAggregator) handleResults() {
	defer sa.wg.Done()

	log.Println("Result handler started")

	for {
		select {
		case <-sa.ctx.Done():
			log.Println("Result handler stopping due to context cancellation")
			return
		case result, ok := <-sa.resultChannel:
			if !ok {
				log.Println("Result handler stopping due to result channel closure")
				return
			}

			// 統計更新
			atomic.AddInt64(&sa.stats.totalWindows, 1)
			atomic.AddInt64(&sa.stats.totalResultsGenerated, 1)

			// 平均イベント数を更新
			totalWindows := atomic.LoadInt64(&sa.stats.totalWindows)
			totalEvents := atomic.LoadInt64(&sa.stats.totalEvents)
			if totalWindows > 0 {
				sa.stats.mu.Lock()
				sa.stats.averageEventsPerWindow = float64(totalEvents) / float64(totalWindows)
				sa.stats.mu.Unlock()
			}

			// 平均処理時間を更新
			sa.stats.mu.Lock()
			if totalWindows > 0 {
				sa.stats.averageProcessingTime = time.Duration(
					(int64(sa.stats.averageProcessingTime)*totalWindows + int64(result.ProcessingTime)) / (totalWindows + 1))
			} else {
				sa.stats.averageProcessingTime = result.ProcessingTime
			}
			sa.stats.mu.Unlock()

			// 結果をログ出力
			log.Printf("Window %s processed: Events=%d, Avg=%.2f, Min=%.2f, Max=%.2f, Median=%.2f, Users=%d, Time=%v",
				result.WindowID, result.EventCount, result.AverageValue, result.MinValue, result.MaxValue,
				result.MedianValue, result.UniqueUsers, result.ProcessingTime)

			// トップイベントタイプ
			if len(result.TopEventTypes) > 0 {
				log.Printf("Top event types for window %s:", result.WindowID)
				for i, eventType := range result.TopEventTypes {
					log.Printf("  %d. %s: %d (%.1f%%)", i+1, eventType.EventType, eventType.Count, eventType.Percentage)
				}
			}
		}
	}
}

// cleanupWindowsは古いウィンドウを清理します
func (sa *StreamAggregator) cleanupWindows() {
	defer sa.wg.Done()

	log.Println("Window cleanup started")

	for {
		select {
		case <-sa.ctx.Done():
			log.Println("Window cleanup stopping due to context cancellation")
			return
		case <-sa.cleanupTicker.C:
			sa.performCleanup()
		}
	}
}

// performCleanupは清理を実行します
func (sa *StreamAggregator) performCleanup() {
	sa.windowMutex.Lock()
	defer sa.windowMutex.Unlock()

	now := time.Now()
	var toDelete []string

	for windowID, window := range sa.windows {
		// 過度に古いウィンドウを削除
		if now.Sub(window.StartTime) > sa.windowSize*2 {
			toDelete = append(toDelete, windowID)
		}
	}

	for _, windowID := range toDelete {
		delete(sa.windows, windowID)
	}

	if len(toDelete) > 0 {
		log.Printf("Cleaned up %d stale windows", len(toDelete))
	}

	log.Printf("Active windows: %d", len(sa.windows))
}

// monitorMetricsはメトリクスを監視します
func (sa *StreamAggregator) monitorMetrics() {
	defer sa.wg.Done()

	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	log.Println("Metrics monitor started")

	for {
		select {
		case <-sa.ctx.Done():
			log.Println("Metrics monitor stopping")
			return
		case <-ticker.C:
			sa.reportMetrics()
		}
	}
}

// reportMetricsはメトリクスを報告します
func (sa *StreamAggregator) reportMetrics() {
	sa.stats.mu.RLock()
	defer sa.stats.mu.RUnlock()

	totalEvents := atomic.LoadInt64(&sa.stats.totalEvents)
	totalWindows := atomic.LoadInt64(&sa.stats.totalWindows)
	totalResults := atomic.LoadInt64(&sa.stats.totalResultsGenerated)

	uptime := time.Since(sa.stats.startTime)
	var eventThroughput float64
	if uptime.Seconds() > 0 {
		eventThroughput = float64(totalEvents) / uptime.Seconds()
	}

	sa.windowMutex.RLock()
	activeWindows := len(sa.windows)
	sa.windowMutex.RUnlock()

	log.Printf("Aggregator Metrics: Events=%d, Windows=%d, Results=%d, Active=%d",
		totalEvents, totalWindows, totalResults, activeWindows)
	log.Printf("Performance: EvtThroughput=%.2f/sec, AvgEvts/Win=%.1f, AvgProcTime=%v, Uptime=%v",
		eventThroughput, sa.stats.averageEventsPerWindow, sa.stats.averageProcessingTime, uptime)

	// ワーカー別統計
	for _, worker := range sa.workers {
		processedEvents := atomic.LoadInt64(&worker.stats.ProcessedEvents)
		processedWindows := atomic.LoadInt64(&worker.stats.ProcessedWindows)

		if processedWindows > 0 {
			avgEventsPerWindow := float64(processedEvents) / float64(processedWindows)
			log.Printf("Worker %d: Events=%d, Windows=%d, AvgEvts/Win=%.1f, AvgTime=%v",
				worker.ID, processedEvents, processedWindows, avgEventsPerWindow, worker.stats.AverageProcessingTime)
		}
	}
}

// Shutdownは集約システムを停止します
func (sa *StreamAggregator) Shutdown(timeout time.Duration) error {
	if !atomic.CompareAndSwapInt32(&sa.isRunning, 1, 0) {
		return fmt.Errorf("aggregator is not running")
	}

	log.Println("Shutting down stream aggregator...")

	// 1. 新しいイベントの受付を停止
	close(sa.eventChannel)

	// 2. 残っているウィンドウをフラッシュ
	sa.flushRemainingWindows()

	// 3. ワーカーの終了を待機
	done := make(chan struct{})
	go func() {
		sa.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("All workers stopped gracefully")
	case <-time.After(timeout):
		log.Println("Timeout reached, forcing shutdown...")
		sa.cancel()

		// 追加の待機時間
		select {
		case <-done:
			log.Println("Workers stopped after cancellation")
		case <-time.After(2 * time.Second):
			log.Println("Some workers may not have stopped properly")
		}
	}

	// 4. チャネルを閉じる
	close(sa.windowChannel)
	close(sa.resultChannel)

	// 5. タイマーを停止
	sa.cleanupTicker.Stop()

	log.Println("Stream aggregator shutdown completed")
	return nil
}

// flushRemainingWindowsは残りのウィンドウをフラッシュします
func (sa *StreamAggregator) flushRemainingWindows() {
	sa.windowMutex.Lock()
	defer sa.windowMutex.Unlock()

	log.Printf("Flushing %d remaining windows", len(sa.windows))

	for windowID, window := range sa.windows {
		windowCopy := sa.copyWindow(window)

		select {
		case sa.windowChannel <- windowCopy:
			// 正常に送信
		case <-time.After(1 * time.Second):
			log.Printf("Timeout while flushing window %s", windowID)
		}
	}

	// 全ウィンドウを削除
	sa.windows = make(map[string]*AggregationWindow)
}

// GetStatsは統計情報を取得します
func (sa *StreamAggregator) GetStats() map[string]interface{} {
	sa.stats.mu.RLock()
	defer sa.stats.mu.RUnlock()

	totalEvents := atomic.LoadInt64(&sa.stats.totalEvents)
	totalWindows := atomic.LoadInt64(&sa.stats.totalWindows)
	totalResults := atomic.LoadInt64(&sa.stats.totalResultsGenerated)

	uptime := time.Since(sa.stats.startTime)
	var eventThroughput float64
	if uptime.Seconds() > 0 {
		eventThroughput = float64(totalEvents) / uptime.Seconds()
	}

	sa.windowMutex.RLock()
	activeWindows := len(sa.windows)
	sa.windowMutex.RUnlock()

	workerStats := make(map[string]interface{})
	for id, stats := range sa.stats.workerStats {
		workerStats[fmt.Sprintf("worker_%d", id)] = map[string]interface{}{
			"processed_events":        atomic.LoadInt64(&stats.ProcessedEvents),
			"processed_windows":       atomic.LoadInt64(&stats.ProcessedWindows),
			"average_processing_time": stats.AverageProcessingTime,
		}
	}

	return map[string]interface{}{
		"total_events":              totalEvents,
		"total_windows":             totalWindows,
		"total_results":             totalResults,
		"active_windows":            activeWindows,
		"average_events_per_window": sa.stats.averageEventsPerWindow,
		"average_processing_time":   sa.stats.averageProcessingTime,
		"event_throughput_per_sec":  eventThroughput,
		"uptime":                    uptime,
		"window_size":               sa.windowSize,
		"slide_interval":            sa.slideInterval,
		"worker_count":              len(sa.workers),
		"worker_stats":              workerStats,
	}
}

func main() {
	// ストリーム集約設定
	config := NewAggregationConfig()
	config.WindowSize = 20 * time.Second
	config.SlideInterval = 5 * time.Second
	config.WorkerCount = 3
	config.MaxWindowSize = 5000
	config.BufferSize = 10000

	// 集約システム作成・開始
	aggregator := NewStreamAggregator(config)
	if err := aggregator.Start(); err != nil {
		log.Fatalf("Failed to start aggregator: %v", err)
	}

	// 結果処理のgoroutineを開始
	go func() {
		for result := range aggregator.GetResultChannel() {
			log.Printf("=== Aggregation Result ===")
			log.Printf("Window: %s (%v to %v)", result.WindowID, result.StartTime.Format("15:04:05"), result.EndTime.Format("15:04:05"))
			log.Printf("Events: %d, Unique Users: %d", result.EventCount, result.UniqueUsers)
			log.Printf("Values: Avg=%.2f, Min=%.2f, Max=%.2f, Median=%.2f, StdDev=%.2f",
				result.AverageValue, result.MinValue, result.MaxValue, result.MedianValue, result.StandardDev)
			log.Printf("Processing: Worker=%d, Time=%v", result.WorkerID, result.ProcessingTime)

			if len(result.TopEventTypes) > 0 {
				log.Printf("Top Event Types:")
				for i, eventType := range result.TopEventTypes {
					log.Printf("  %d. %s: %d events (%.1f%%)", i+1, eventType.EventType, eventType.Count, eventType.Percentage)
				}
			}
			log.Printf("========================")
		}
	}()

	// テストイベントを生成・送信
	go func() {
		eventTypes := []string{"click", "view", "purchase", "login", "logout", "search", "comment"}
		userIDs := []string{"user1", "user2", "user3", "user4", "user5", "user6", "user7", "user8", "user9", "user10"}

		for i := 0; i < 5000; i++ {
			event := StreamEvent{
				ID:        int64(i),
				EventType: eventTypes[rand.Intn(len(eventTypes))],
				Timestamp: time.Now(),
				UserID:    userIDs[rand.Intn(len(userIDs))],
				SessionID: fmt.Sprintf("session_%d", rand.Intn(20)),
				Value:     rand.Float64()*100 + 1, // 1-101の値
				Data: map[string]interface{}{
					"page":     fmt.Sprintf("/page%d", rand.Intn(10)),
					"referrer": fmt.Sprintf("ref%d", rand.Intn(5)),
					"device":   []string{"mobile", "desktop", "tablet"}[rand.Intn(3)],
				},
				Source: "test_generator",
			}

			if err := aggregator.SubmitEvent(event); err != nil {
				log.Printf("Failed to submit event %d: %v", i, err)
				break
			}

			// 送信頻度を制御（リアルタイムストリームをシミュレート）
			time.Sleep(time.Duration(rand.Intn(50)+10) * time.Millisecond)
		}

		log.Println("All test events submitted")
	}()

	// 60秒間実行
	time.Sleep(60 * time.Second)

	// 最終統計表示
	stats := aggregator.GetStats()
	log.Printf("Final Stats: %+v", stats)

	// グレースフルシャットダウン
	if err := aggregator.Shutdown(15 * time.Second); err != nil {
		log.Printf("Shutdown error: %v", err)
	}
}
