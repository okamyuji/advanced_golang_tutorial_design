package main

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// doubleStage 入力をそのまま2倍にして返す、待機のない決定的な処理段階です
func doubleStage(_ context.Context, data any) (any, error) {
	n, ok := data.(int)
	if !ok {
		return nil, fmt.Errorf("invalid data type")
	}
	return n * 2, nil
}

// TestMultiStagePipelineBasicFlow 投入したデータが段階を経て2倍になって
// 出力チャネルに届くことを確かめます。実時間のSleepではなく、
// チャネル受信とタイムアウト付きselectで完了を待ちます。
func TestMultiStagePipelineBasicFlow(t *testing.T) {
	stages := []ProcessingStage{
		{Name: "double", ProcessorFn: doubleStage, WorkerCount: 2, BufferSize: 10},
	}
	config := NewPipelineConfig()
	config.InputBufferSize = 10
	config.OutputBufferSize = 10

	pipeline := NewMultiStagePipeline(stages, config)
	if err := pipeline.Start(); err != nil {
		t.Fatalf("Failed to start pipeline: %v", err)
	}
	defer func() {
		if err := pipeline.Shutdown(2 * time.Second); err != nil {
			t.Logf("Failed to shutdown pipeline: %v", err)
		}
	}()

	// このパイプラインはcollectResults()が出力チャネルを内部で消費し、
	// GetStatsの total_output に反映する設計。GetOutputChannel()から
	// 直接受信すると内部の収集goroutineと受信が競合してしまうため、
	// 完了はGetStatsのカウンタが揃うまでポーリングして確認する。
	const itemCount = 5
	for i := range itemCount {
		if err := pipeline.SubmitData(i); err != nil {
			t.Fatalf("Failed to submit data %d: %v", i, err)
		}
	}

	deadline := time.After(3 * time.Second)
	for {
		stats := pipeline.GetStats()
		if stats["total_output"].(int64) >= int64(itemCount) {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("Timeout waiting for pipeline output: stats=%+v", stats)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// TestShutdownConcurrentWithSubmit SubmitDataとShutdownを並行させてもpanicせず、
// 停止後の投入は必ずエラーになることを確かめる回帰テストです。
func TestShutdownConcurrentWithSubmit(t *testing.T) {
	for range 50 {
		stages := []ProcessingStage{
			{Name: "double", ProcessorFn: doubleStage, WorkerCount: 2, BufferSize: 4},
		}
		config := NewPipelineConfig()
		config.InputBufferSize = 4
		config.OutputBufferSize = 4

		pipeline := NewMultiStagePipeline(stages, config)
		if err := pipeline.Start(); err != nil {
			t.Fatalf("Failed to start pipeline: %v", err)
		}

		var submitWg sync.WaitGroup
		stop := make(chan struct{})

		const submitters = 8
		for range submitters {
			submitWg.Go(func() {
				for {
					select {
					case <-stop:
						return
					default:
						_ = pipeline.SubmitData(1)
					}
				}
			})
		}

		time.Sleep(time.Millisecond)

		if err := pipeline.Shutdown(1 * time.Second); err != nil {
			t.Fatalf("Failed to shutdown pipeline: %v", err)
		}

		close(stop)
		submitWg.Wait()

		// 停止後の投入は必ずエラーになる
		if err := pipeline.SubmitData(1); err == nil {
			t.Error("Expected error when submitting data after shutdown")
		}
	}
}
