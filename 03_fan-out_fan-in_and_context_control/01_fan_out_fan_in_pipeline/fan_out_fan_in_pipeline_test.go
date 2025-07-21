package main

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestNewPipelineConfig(t *testing.T) {
	config := NewPipelineConfig()

	if config.WorkerCount <= 0 {
		t.Errorf("Expected positive WorkerCount, got %d", config.WorkerCount)
	}
	if config.BufferSize <= 0 {
		t.Errorf("Expected positive BufferSize, got %d", config.BufferSize)
	}
	if config.ProcessingDelay < 0 {
		t.Errorf("Expected non-negative ProcessingDelay, got %v", config.ProcessingDelay)
	}
	if config.ErrorRate < 0 || config.ErrorRate > 1 {
		t.Errorf("Expected ErrorRate between 0 and 1, got %f", config.ErrorRate)
	}
}

func TestNewFanOutFanInPipeline(t *testing.T) {
	config := &PipelineConfig{
		WorkerCount:     2,
		BufferSize:      10,
		ProcessingDelay: 1 * time.Millisecond,
		ErrorRate:       0.0,
		TimeoutDuration: 5 * time.Second,
		RetryAttempts:   1,
		EnableMetrics:   true,
		LogLevel:        "INFO",
	}

	pipeline := NewFanOutFanInPipeline(config)

	if pipeline == nil {
		t.Fatal("Expected pipeline to be created")
	}
	if len(pipeline.workers) != config.WorkerCount {
		t.Errorf("Expected %d workers, got %d", config.WorkerCount, len(pipeline.workers))
	}
	if pipeline.config.WorkerCount != config.WorkerCount {
		t.Errorf("Expected WorkerCount %d, got %d", config.WorkerCount, pipeline.config.WorkerCount)
	}
}

func TestPipelineStartAndShutdown(t *testing.T) {
	config := &PipelineConfig{
		WorkerCount:     2,
		BufferSize:      10,
		ProcessingDelay: 1 * time.Millisecond,
		ErrorRate:       0.0,
		TimeoutDuration: 5 * time.Second,
		RetryAttempts:   1,
		EnableMetrics:   false, // メトリクス無効にしてテストを簡素化
		LogLevel:        "INFO",
	}

	pipeline := NewFanOutFanInPipeline(config)

	// Start test
	err := pipeline.Start()
	if err != nil {
		t.Fatalf("Failed to start pipeline: %v", err)
	}

	// 重複起動のテスト
	err = pipeline.Start()
	if err == nil {
		t.Error("Expected error when starting already running pipeline")
	}

	// 少し待ってワーカーが開始されることを確認
	time.Sleep(10 * time.Millisecond)

	// Shutdown test
	err = pipeline.Shutdown(2 * time.Second)
	if err != nil {
		t.Fatalf("Failed to shutdown pipeline: %v", err)
	}

	// 重複停止のテスト
	err = pipeline.Shutdown(1 * time.Second)
	if err == nil {
		t.Error("Expected error when shutting down already stopped pipeline")
	}
}

func TestPipelineDataProcessing(t *testing.T) {
	config := &PipelineConfig{
		WorkerCount:     2,
		BufferSize:      100,
		ProcessingDelay: 1 * time.Millisecond,
		ErrorRate:       0.0, // エラーなしでテスト
		TimeoutDuration: 5 * time.Second,
		RetryAttempts:   1,
		EnableMetrics:   false,
		LogLevel:        "INFO",
	}

	pipeline := NewFanOutFanInPipeline(config)

	err := pipeline.Start()
	if err != nil {
		t.Fatalf("Failed to start pipeline: %v", err)
	}

	// 結果を収集
	var results []AggregatedResult
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for result := range pipeline.GetResultChannel() {
			results = append(results, result)
			return // 1つの結果を取得したら終了
		}
	}()

	// テストデータを送信
	testDataCount := 10
	for i := 0; i < testDataCount; i++ {
		item := DataItem{
			ID:        int64(i),
			Value:     fmt.Sprintf("test_data_%d", i),
			Timestamp: time.Now(),
			Metadata:  map[string]interface{}{"test": true},
			Source:    "test",
		}

		err := pipeline.SubmitData(item)
		if err != nil {
			t.Errorf("Failed to submit data %d: %v", i, err)
		}
	}

	// 処理完了を待つ（データが処理されるまで待機）
	time.Sleep(200 * time.Millisecond)

	// パイプラインを適切にシャットダウンして集約結果を取得
	go func() {
		time.Sleep(500 * time.Millisecond) // データ処理を待つ
		if err := pipeline.Shutdown(1 * time.Second); err != nil {
			t.Logf("Failed to shutdown pipeline: %v", err)
		}
	}()

	// 結果を待つ
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// 正常完了
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for results")
	}

	// 結果を検証
	if len(results) == 0 {
		t.Error("Expected at least one aggregated result")
	} else {
		result := results[0]
		if result.TotalProcessed != int64(testDataCount) {
			t.Errorf("Expected %d processed items, got %d", testDataCount, result.TotalProcessed)
		}
		if result.SuccessCount != int64(testDataCount) {
			t.Errorf("Expected %d successful items, got %d", testDataCount, result.SuccessCount)
		}
		if result.ErrorCount != 0 {
			t.Errorf("Expected 0 errors, got %d", result.ErrorCount)
		}
	}
}

func TestPipelineErrorHandling(t *testing.T) {
	config := &PipelineConfig{
		WorkerCount:     1,
		BufferSize:      10,
		ProcessingDelay: 1 * time.Millisecond,
		ErrorRate:       1.0, // 100% エラー率
		TimeoutDuration: 5 * time.Second,
		RetryAttempts:   1,
		EnableMetrics:   false,
		LogLevel:        "INFO",
	}

	pipeline := NewFanOutFanInPipeline(config)

	err := pipeline.Start()
	if err != nil {
		t.Fatalf("Failed to start pipeline: %v", err)
	}

	// 結果を収集
	var results []AggregatedResult
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for result := range pipeline.GetResultChannel() {
			results = append(results, result)
			return
		}
	}()

	// テストデータを送信
	testDataCount := 5
	for i := 0; i < testDataCount; i++ {
		item := DataItem{
			ID:        int64(i),
			Value:     fmt.Sprintf("error_test_%d", i),
			Timestamp: time.Now(),
			Source:    "error_test",
		}

		err := pipeline.SubmitData(item)
		if err != nil {
			t.Errorf("Failed to submit data %d: %v", i, err)
		}
	}

	// 処理完了を待つ
	time.Sleep(100 * time.Millisecond)

	// パイプラインをシャットダウンして集約結果を取得
	go func() {
		time.Sleep(300 * time.Millisecond)
		if err := pipeline.Shutdown(1 * time.Second); err != nil {
			t.Logf("Failed to shutdown pipeline: %v", err)
		}
	}()

	// 結果を待つ
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// 正常完了
	case <-time.After(3 * time.Second):
		t.Fatal("Timeout waiting for results")
	}

	// エラー結果を検証
	if len(results) == 0 {
		t.Error("Expected at least one aggregated result")
	} else {
		result := results[0]
		if result.ErrorCount != int64(testDataCount) {
			t.Errorf("Expected %d errors, got %d", testDataCount, result.ErrorCount)
		}
		if result.SuccessCount != 0 {
			t.Errorf("Expected 0 successful items, got %d", result.SuccessCount)
		}
	}
}

func TestPipelineSubmitDataWhenNotRunning(t *testing.T) {
	config := &PipelineConfig{
		WorkerCount: 1,
		BufferSize:  10,
	}

	pipeline := NewFanOutFanInPipeline(config)

	item := DataItem{
		ID:    1,
		Value: "test",
	}

	err := pipeline.SubmitData(item)
	if err == nil {
		t.Error("Expected error when submitting data to non-running pipeline")
	}
}

func TestPipelineGetMetrics(t *testing.T) {
	config := &PipelineConfig{
		WorkerCount:     2,
		BufferSize:      10,
		ProcessingDelay: 1 * time.Millisecond,
		ErrorRate:       0.0,
		TimeoutDuration: 5 * time.Second,
		RetryAttempts:   1,
		EnableMetrics:   true,
		LogLevel:        "INFO",
	}

	pipeline := NewFanOutFanInPipeline(config)

	err := pipeline.Start()
	if err != nil {
		t.Fatalf("Failed to start pipeline: %v", err)
	}
	defer func() {
		if err := pipeline.Shutdown(2 * time.Second); err != nil {
			t.Logf("Failed to shutdown pipeline: %v", err)
		}
	}()

	// メトリクスを取得
	metrics := pipeline.GetMetrics()

	// メトリクスの基本構造をテスト
	if metrics == nil {
		t.Fatal("Expected metrics to be returned")
	}

	expectedFields := []string{
		"total_input", "total_output", "error_count", "throughput_rps",
		"uptime", "worker_count", "worker_stats",
	}

	for _, field := range expectedFields {
		if _, exists := metrics[field]; !exists {
			t.Errorf("Expected metrics field '%s' to exist", field)
		}
	}

	workerCount := metrics["worker_count"].(int)
	if workerCount != config.WorkerCount {
		t.Errorf("Expected worker_count %d, got %d", config.WorkerCount, workerCount)
	}
}

func TestPipelineContextCancellation(t *testing.T) {
	config := &PipelineConfig{
		WorkerCount:     1,
		BufferSize:      10,
		ProcessingDelay: 100 * time.Millisecond, // 長い処理時間
		ErrorRate:       0.0,
		TimeoutDuration: 5 * time.Second,
		RetryAttempts:   1,
		EnableMetrics:   false,
		LogLevel:        "INFO",
	}

	pipeline := NewFanOutFanInPipeline(config)

	err := pipeline.Start()
	if err != nil {
		t.Fatalf("Failed to start pipeline: %v", err)
	}

	// データを送信
	item := DataItem{
		ID:    1,
		Value: "long_processing_test",
	}

	err = pipeline.SubmitData(item)
	if err != nil {
		t.Errorf("Failed to submit data: %v", err)
	}

	// すぐにシャットダウン（処理中にキャンセル）
	start := time.Now()
	err = pipeline.Shutdown(1 * time.Second)
	elapsed := time.Since(start)

	if err != nil {
		t.Errorf("Failed to shutdown pipeline: %v", err)
	}

	// シャットダウンが適切な時間内に完了することを確認
	if elapsed > 2*time.Second {
		t.Errorf("Shutdown took too long: %v", elapsed)
	}
}

func TestWorkerProcessData(t *testing.T) {
	config := &PipelineConfig{
		WorkerCount:     1,
		BufferSize:      10,
		ProcessingDelay: 1 * time.Millisecond,
		ErrorRate:       0.0,
		TimeoutDuration: 5 * time.Second,
		RetryAttempts:   1,
		EnableMetrics:   false,
		LogLevel:        "INFO",
	}

	pipeline := NewFanOutFanInPipeline(config)
	worker := pipeline.workers[0]

	item := DataItem{
		ID:        123,
		Value:     "test_value",
		Timestamp: time.Now(),
		Source:    "test",
	}

	result := worker.processData(item)

	// 結果を検証
	if result.OriginalID != item.ID {
		t.Errorf("Expected OriginalID %d, got %d", item.ID, result.OriginalID)
	}
	if result.ProcessorID != worker.ID {
		t.Errorf("Expected ProcessorID %d, got %d", worker.ID, result.ProcessorID)
	}
	if !result.Success {
		t.Errorf("Expected successful processing, got error: %v", result.Error)
	}
	if result.ProcessingTime <= 0 {
		t.Errorf("Expected positive processing time, got %v", result.ProcessingTime)
	}
}

func BenchmarkPipelineProcessing(b *testing.B) {
	config := &PipelineConfig{
		WorkerCount:     2,
		BufferSize:      1000,
		ProcessingDelay: 0, // ベンチマークでは遅延なし
		ErrorRate:       0.0,
		TimeoutDuration: 30 * time.Second,
		RetryAttempts:   1,
		EnableMetrics:   false,
		LogLevel:        "ERROR",
	}

	pipeline := NewFanOutFanInPipeline(config)

	err := pipeline.Start()
	if err != nil {
		b.Fatalf("Failed to start pipeline: %v", err)
	}
	defer func() {
		if err := pipeline.Shutdown(5 * time.Second); err != nil {
			b.Logf("Failed to shutdown pipeline: %v", err)
		}
	}()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		item := DataItem{
			ID:        int64(i),
			Value:     fmt.Sprintf("bench_data_%d", i),
			Timestamp: time.Now(),
			Source:    "benchmark",
		}

		err := pipeline.SubmitData(item)
		if err != nil {
			b.Errorf("Failed to submit data %d: %v", i, err)
		}
	}
}
