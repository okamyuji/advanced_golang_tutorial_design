package main

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestNewAggregationConfig(t *testing.T) {
	config := NewAggregationConfig()

	if config.WindowSize <= 0 {
		t.Errorf("Expected positive WindowSize, got %v", config.WindowSize)
	}
	if config.SlideInterval <= 0 {
		t.Errorf("Expected positive SlideInterval, got %v", config.SlideInterval)
	}
	if config.MaxWindowSize <= 0 {
		t.Errorf("Expected positive MaxWindowSize, got %d", config.MaxWindowSize)
	}
	if config.WorkerCount <= 0 {
		t.Errorf("Expected positive WorkerCount, got %d", config.WorkerCount)
	}
	if config.BufferSize <= 0 {
		t.Errorf("Expected positive BufferSize, got %d", config.BufferSize)
	}
}

func TestNewStreamAggregator(t *testing.T) {
	config := &AggregationConfig{
		WindowSize:      10 * time.Second,
		SlideInterval:   5 * time.Second,
		MaxWindowSize:   1000,
		WorkerCount:     2,
		BufferSize:      100,
		CleanupInterval: 30 * time.Second,
		EnableMetrics:   true,
		MetricsInterval: 10 * time.Second,
	}

	aggregator := NewStreamAggregator(config)

	if aggregator == nil {
		t.Fatal("Expected aggregator to be created")
	}
	if len(aggregator.workers) != config.WorkerCount {
		t.Errorf("Expected %d workers, got %d", config.WorkerCount, len(aggregator.workers))
	}
	if aggregator.windowSize != config.WindowSize {
		t.Errorf("Expected WindowSize %v, got %v", config.WindowSize, aggregator.windowSize)
	}
	if aggregator.slideInterval != config.SlideInterval {
		t.Errorf("Expected SlideInterval %v, got %v", config.SlideInterval, aggregator.slideInterval)
	}
}

func TestAggregatorStartAndShutdown(t *testing.T) {
	config := &AggregationConfig{
		WindowSize:      5 * time.Second,
		SlideInterval:   2 * time.Second,
		MaxWindowSize:   100,
		WorkerCount:     2,
		BufferSize:      50,
		CleanupInterval: 30 * time.Second,
		EnableMetrics:   false, // テストを簡素化
		MetricsInterval: 10 * time.Second,
	}

	aggregator := NewStreamAggregator(config)

	// Start test
	err := aggregator.Start()
	if err != nil {
		t.Fatalf("Failed to start aggregator: %v", err)
	}

	// 重複起動のテスト
	err = aggregator.Start()
	if err == nil {
		t.Error("Expected error when starting already running aggregator")
	}

	// 少し待ってワーカーが開始されることを確認
	time.Sleep(10 * time.Millisecond)

	// Shutdown test
	err = aggregator.Shutdown(3 * time.Second)
	if err != nil {
		t.Fatalf("Failed to shutdown aggregator: %v", err)
	}

	// 重複停止のテスト
	err = aggregator.Shutdown(1 * time.Second)
	if err == nil {
		t.Error("Expected error when shutting down already stopped aggregator")
	}
}

func TestEventSubmissionAndProcessing(t *testing.T) {
	config := &AggregationConfig{
		WindowSize:      2 * time.Second,
		SlideInterval:   1 * time.Second,
		MaxWindowSize:   10, // 小さなウィンドウサイズでテスト
		WorkerCount:     2,
		BufferSize:      100,
		CleanupInterval: 30 * time.Second,
		EnableMetrics:   false,
		MetricsInterval: 10 * time.Second,
	}

	aggregator := NewStreamAggregator(config)

	err := aggregator.Start()
	if err != nil {
		t.Fatalf("Failed to start aggregator: %v", err)
	}
	defer func() {
		if err := aggregator.Shutdown(3 * time.Second); err != nil {
			t.Logf("Failed to shutdown aggregator: %v", err)
		}
	}()

	// 結果を収集
	var results []*AggregationResult
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		timeout := time.After(5 * time.Second)
		for {
			select {
			case result, ok := <-aggregator.GetResultChannel():
				if !ok {
					return
				}
				results = append(results, result)
				if len(results) >= 1 { // 少なくとも1つの結果を待つ
					return
				}
			case <-timeout:
				return
			}
		}
	}()

	// テストイベントを送信
	eventCount := 15 // MaxWindowSizeを超える数
	for i := 0; i < eventCount; i++ {
		event := StreamEvent{
			ID:        int64(i),
			EventType: "test_event",
			Timestamp: time.Now(),
			UserID:    fmt.Sprintf("user_%d", i%3), // 3ユーザーでテスト
			SessionID: fmt.Sprintf("session_%d", i%2),
			Value:     float64(i + 1),
			Data:      map[string]interface{}{"index": i},
			Source:    "test",
		}

		err := aggregator.SubmitEvent(event)
		if err != nil {
			t.Errorf("Failed to submit event %d: %v", i, err)
		}

		// 少し間隔を開ける
		time.Sleep(10 * time.Millisecond)
	}

	// 処理完了を待つ
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// 正常完了
	case <-time.After(10 * time.Second):
		t.Fatal("Timeout waiting for event processing")
	}

	// 結果を検証
	if len(results) == 0 {
		t.Error("Expected at least one aggregation result")
	} else {
		result := results[0]
		if result.EventCount <= 0 {
			t.Errorf("Expected positive event count, got %d", result.EventCount)
		}
		if result.WorkerID < 0 {
			t.Errorf("Expected valid worker ID, got %d", result.WorkerID)
		}
		if result.ProcessingTime <= 0 {
			t.Errorf("Expected positive processing time, got %v", result.ProcessingTime)
		}
	}
}

func TestEventTypeAggregation(t *testing.T) {
	config := &AggregationConfig{
		WindowSize:      1 * time.Second,
		SlideInterval:   500 * time.Millisecond,
		MaxWindowSize:   20,
		WorkerCount:     1,
		BufferSize:      50,
		CleanupInterval: 30 * time.Second,
		EnableMetrics:   false,
		MetricsInterval: 10 * time.Second,
	}

	aggregator := NewStreamAggregator(config)

	err := aggregator.Start()
	if err != nil {
		t.Fatalf("Failed to start aggregator: %v", err)
	}
	defer func() {
		if err := aggregator.Shutdown(2 * time.Second); err != nil {
			t.Logf("Failed to shutdown aggregator: %v", err)
		}
	}()

	// 結果を収集
	var results []*AggregationResult
	var mu sync.Mutex

	go func() {
		for result := range aggregator.GetResultChannel() {
			mu.Lock()
			results = append(results, result)
			mu.Unlock()
		}
	}()

	// 異なるイベントタイプを送信
	eventTypes := []string{"click", "view", "purchase"}
	for i := 0; i < 21; i++ { // MaxWindowSizeを超える
		event := StreamEvent{
			ID:        int64(i),
			EventType: eventTypes[i%len(eventTypes)],
			Timestamp: time.Now(),
			UserID:    "test_user",
			Value:     100.0,
			Source:    "test",
		}

		err := aggregator.SubmitEvent(event)
		if err != nil {
			t.Errorf("Failed to submit event %d: %v", i, err)
		}
	}

	// 処理完了を待つ
	time.Sleep(2 * time.Second)

	// 結果を検証
	mu.Lock()
	defer mu.Unlock()

	if len(results) == 0 {
		t.Error("Expected at least one aggregation result")
	} else {
		result := results[0]
		if len(result.TopEventTypes) == 0 {
			t.Error("Expected top event types to be populated")
		}

		// イベントタイプの統計を確認
		foundTypes := make(map[string]bool)
		for _, eventType := range result.TopEventTypes {
			foundTypes[eventType.EventType] = true
			if eventType.Count <= 0 {
				t.Errorf("Expected positive count for event type %s, got %d", eventType.EventType, eventType.Count)
			}
			if eventType.Percentage <= 0 {
				t.Errorf("Expected positive percentage for event type %s, got %f", eventType.EventType, eventType.Percentage)
			}
		}

		// 送信したイベントタイプが結果に含まれていることを確認
		if len(foundTypes) == 0 {
			t.Error("Expected event types to be found in results")
		}
	}
}

func TestSubmitEventWhenNotRunning(t *testing.T) {
	config := &AggregationConfig{
		WindowSize:      5 * time.Second,
		SlideInterval:   1 * time.Second,
		MaxWindowSize:   100,
		WorkerCount:     1,
		BufferSize:      10,
		CleanupInterval: 30 * time.Second,
		EnableMetrics:   false,
		MetricsInterval: 10 * time.Second,
	}

	aggregator := NewStreamAggregator(config)

	event := StreamEvent{
		ID:    1,
		Value: 1.0,
	}

	err := aggregator.SubmitEvent(event)
	if err == nil {
		t.Error("Expected error when submitting event to non-running aggregator")
	}
}

func TestWindowManagement(t *testing.T) {
	config := &AggregationConfig{
		WindowSize:      500 * time.Millisecond, // 短いウィンドウ
		SlideInterval:   250 * time.Millisecond,
		MaxWindowSize:   5, // 小さなサイズ
		WorkerCount:     1,
		BufferSize:      50,
		CleanupInterval: 1 * time.Second,
		EnableMetrics:   false,
		MetricsInterval: 10 * time.Second,
	}

	aggregator := NewStreamAggregator(config)

	err := aggregator.Start()
	if err != nil {
		t.Fatalf("Failed to start aggregator: %v", err)
	}
	defer func() {
		if err := aggregator.Shutdown(2 * time.Second); err != nil {
			t.Logf("Failed to shutdown aggregator: %v", err)
		}
	}()

	// ウィンドウの作成と期限切れをテスト
	for i := 0; i < 10; i++ {
		event := StreamEvent{
			ID:        int64(i),
			EventType: "window_test",
			Timestamp: time.Now(),
			UserID:    "test_user",
			Value:     float64(i),
			Source:    "test",
		}

		err := aggregator.SubmitEvent(event)
		if err != nil {
			t.Errorf("Failed to submit event %d: %v", i, err)
		}

		time.Sleep(100 * time.Millisecond)
	}

	// ウィンドウ処理とクリーンアップを待つ
	time.Sleep(2 * time.Second)

	// 統計を確認
	stats := aggregator.GetStats()
	if stats == nil {
		t.Fatal("Expected stats to be returned")
	}

	totalEvents := stats["total_events"].(int64)
	if totalEvents != 10 {
		t.Errorf("Expected 10 total events, got %d", totalEvents)
	}
}

func TestAggregatorGetStats(t *testing.T) {
	config := &AggregationConfig{
		WindowSize:      5 * time.Second,
		SlideInterval:   2 * time.Second,
		MaxWindowSize:   100,
		WorkerCount:     2,
		BufferSize:      50,
		CleanupInterval: 30 * time.Second,
		EnableMetrics:   true,
		MetricsInterval: 10 * time.Second,
	}

	aggregator := NewStreamAggregator(config)

	err := aggregator.Start()
	if err != nil {
		t.Fatalf("Failed to start aggregator: %v", err)
	}
	defer func() {
		if err := aggregator.Shutdown(2 * time.Second); err != nil {
			t.Logf("Failed to shutdown aggregator: %v", err)
		}
	}()

	// 統計を取得
	stats := aggregator.GetStats()

	// 統計の基本構造をテスト
	if stats == nil {
		t.Fatal("Expected stats to be returned")
	}

	expectedFields := []string{
		"total_events", "total_windows", "total_results", "active_windows",
		"average_events_per_window", "average_processing_time", "event_throughput_per_sec",
		"uptime", "window_size", "slide_interval", "worker_count", "worker_stats",
	}

	for _, field := range expectedFields {
		if _, exists := stats[field]; !exists {
			t.Errorf("Expected stats field '%s' to exist", field)
		}
	}

	workerCount := stats["worker_count"].(int)
	if workerCount != config.WorkerCount {
		t.Errorf("Expected worker_count %d, got %d", config.WorkerCount, workerCount)
	}

	windowSize := stats["window_size"].(time.Duration)
	if windowSize != config.WindowSize {
		t.Errorf("Expected window_size %v, got %v", config.WindowSize, windowSize)
	}
}

func TestWorkerProcessWindow(t *testing.T) {
	config := &AggregationConfig{
		WindowSize:      5 * time.Second,
		SlideInterval:   2 * time.Second,
		MaxWindowSize:   100,
		WorkerCount:     1,
		BufferSize:      50,
		CleanupInterval: 30 * time.Second,
		EnableMetrics:   false,
		MetricsInterval: 10 * time.Second,
	}

	aggregator := NewStreamAggregator(config)
	worker := aggregator.workers[0]

	// テストウィンドウを作成
	window := &AggregationWindow{
		WindowID:   "test_window",
		StartTime:  time.Now().Add(-5 * time.Second),
		EndTime:    time.Now(),
		WindowSize: 5 * time.Second,
		Events: []StreamEvent{
			{ID: 1, EventType: "test1", UserID: "user1", Value: 10.0},
			{ID: 2, EventType: "test2", UserID: "user2", Value: 20.0},
			{ID: 3, EventType: "test1", UserID: "user1", Value: 30.0},
		},
		UserCounts: map[string]int64{"user1": 2, "user2": 1},
		TypeCounts: map[string]int64{"test1": 2, "test2": 1},
		EventCount: 3,
		TotalValue: 60.0,
		MinValue:   10.0,
		MaxValue:   30.0,
	}

	result := worker.processWindow(window)

	// 結果を検証
	if result.WindowID != window.WindowID {
		t.Errorf("Expected WindowID %s, got %s", window.WindowID, result.WindowID)
	}
	if result.EventCount != window.EventCount {
		t.Errorf("Expected EventCount %d, got %d", window.EventCount, result.EventCount)
	}
	if result.WorkerID != worker.ID {
		t.Errorf("Expected WorkerID %d, got %d", worker.ID, result.WorkerID)
	}
	if result.AverageValue != 20.0 { // 60/3 = 20
		t.Errorf("Expected AverageValue 20.0, got %f", result.AverageValue)
	}
	if result.MinValue != 10.0 {
		t.Errorf("Expected MinValue 10.0, got %f", result.MinValue)
	}
	if result.MaxValue != 30.0 {
		t.Errorf("Expected MaxValue 30.0, got %f", result.MaxValue)
	}
	if result.UniqueUsers != 2 {
		t.Errorf("Expected UniqueUsers 2, got %d", result.UniqueUsers)
	}
	if result.ProcessingTime <= 0 {
		t.Errorf("Expected positive processing time, got %v", result.ProcessingTime)
	}

	// トップイベントタイプの検証
	if len(result.TopEventTypes) == 0 {
		t.Error("Expected top event types to be populated")
	}
}

func TestCalculateStandardDeviation(t *testing.T) {
	config := &AggregationConfig{
		WindowSize:      5 * time.Second,
		SlideInterval:   1 * time.Second,
		MaxWindowSize:   100,
		WorkerCount:     1,
		BufferSize:      10,
		CleanupInterval: 30 * time.Second,
		EnableMetrics:   false,
		MetricsInterval: 10 * time.Second,
	}

	aggregator := NewStreamAggregator(config)
	worker := aggregator.workers[0]

	// テストケース1: 単一値
	values := []float64{5.0}
	mean := 5.0
	stdDev := worker.calculateStandardDeviation(values, mean)
	if stdDev != 0 {
		t.Errorf("Expected standard deviation 0 for single value, got %f", stdDev)
	}

	// テストケース2: 複数値
	values = []float64{1.0, 2.0, 3.0, 4.0, 5.0}
	mean = 3.0
	stdDev = worker.calculateStandardDeviation(values, mean)
	if stdDev <= 0 {
		t.Errorf("Expected positive standard deviation, got %f", stdDev)
	}

	// テストケース3: 空の配列
	values = []float64{}
	stdDev = worker.calculateStandardDeviation(values, 0)
	if stdDev != 0 {
		t.Errorf("Expected standard deviation 0 for empty array, got %f", stdDev)
	}
}

func TestCalculateTopEventTypes(t *testing.T) {
	config := &AggregationConfig{
		WindowSize:      5 * time.Second,
		SlideInterval:   1 * time.Second,
		MaxWindowSize:   100,
		WorkerCount:     1,
		BufferSize:      10,
		CleanupInterval: 30 * time.Second,
		EnableMetrics:   false,
		MetricsInterval: 10 * time.Second,
	}

	aggregator := NewStreamAggregator(config)
	worker := aggregator.workers[0]

	typeCounts := map[string]int64{
		"click":    100,
		"view":     50,
		"purchase": 30,
		"search":   20,
		"login":    10,
		"logout":   5,
	}
	totalCount := int64(215)

	topTypes := worker.calculateTopEventTypes(typeCounts, totalCount)

	// 上位5つが返されることを確認
	if len(topTypes) != 5 {
		t.Errorf("Expected 5 top event types, got %d", len(topTypes))
	}

	// 最初のアイテムが最もカウントが多いことを確認
	if topTypes[0].EventType != "click" || topTypes[0].Count != 100 {
		t.Errorf("Expected first item to be 'click' with count 100, got %s with count %d",
			topTypes[0].EventType, topTypes[0].Count)
	}

	// パーセンテージが正しく計算されていることを確認
	expectedPercentage := float64(100) / float64(215) * 100
	if topTypes[0].Percentage < expectedPercentage-1 || topTypes[0].Percentage > expectedPercentage+1 {
		t.Errorf("Expected percentage around %f, got %f", expectedPercentage, topTypes[0].Percentage)
	}

	// ソートされていることを確認
	for i := 1; i < len(topTypes); i++ {
		if topTypes[i-1].Count < topTypes[i].Count {
			t.Errorf("Top event types are not sorted correctly: %d >= %d expected",
				topTypes[i-1].Count, topTypes[i].Count)
		}
	}
}

func BenchmarkAggregatorEventProcessing(b *testing.B) {
	config := &AggregationConfig{
		WindowSize:      30 * time.Second,
		SlideInterval:   10 * time.Second,
		MaxWindowSize:   10000,
		WorkerCount:     2,
		BufferSize:      10000,
		CleanupInterval: 60 * time.Second,
		EnableMetrics:   false,
		MetricsInterval: 60 * time.Second,
	}

	aggregator := NewStreamAggregator(config)

	err := aggregator.Start()
	if err != nil {
		b.Fatalf("Failed to start aggregator: %v", err)
	}
	defer func() {
		if err := aggregator.Shutdown(5 * time.Second); err != nil {
			b.Logf("Failed to shutdown aggregator: %v", err)
		}
	}()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		event := StreamEvent{
			ID:        int64(i),
			EventType: "bench_event",
			Timestamp: time.Now(),
			UserID:    fmt.Sprintf("user_%d", i%100),
			Value:     float64(i),
			Source:    "benchmark",
		}

		err := aggregator.SubmitEvent(event)
		if err != nil {
			b.Errorf("Failed to submit event %d: %v", i, err)
		}
	}
}
