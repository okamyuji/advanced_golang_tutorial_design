package main

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestGoroutineManager_StartWorker(t *testing.T) {
	gm := NewGoroutineManager()

	// 正常系: ワーカーの開始
	taskExecuted := false
	var mu sync.Mutex

	taskFunc := func(ctx context.Context) {
		mu.Lock()
		taskExecuted = true
		mu.Unlock()

		// 短時間で終了するタスク
		select {
		case <-ctx.Done():
		case <-time.After(100 * time.Millisecond):
		}
	}

	err := gm.StartWorker("test-worker", taskFunc)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// ワーカーが実行されることを確認
	time.Sleep(150 * time.Millisecond)

	mu.Lock()
	if !taskExecuted {
		t.Error("Task was not executed")
	}
	mu.Unlock()

	// クリーンアップ
	gm.StopAll()
}

func TestGoroutineManager_StartWorker_Duplicate(t *testing.T) {
	gm := NewGoroutineManager()

	taskFunc := func(ctx context.Context) {
		<-ctx.Done()
	}

	// 最初のワーカーを開始
	err := gm.StartWorker("duplicate-worker", taskFunc)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// 異常系: 同じIDでワーカーを開始
	err = gm.StartWorker("duplicate-worker", taskFunc)
	if err == nil {
		t.Error("Expected error for duplicate worker ID, got nil")
	}

	// クリーンアップ
	gm.StopAll()
}

func TestGoroutineManager_StopWorker(t *testing.T) {
	gm := NewGoroutineManager()

	taskFunc := func(ctx context.Context) {
		<-ctx.Done()
	}

	// ワーカーを開始
	err := gm.StartWorker("stop-test-worker", taskFunc)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// 正常系: ワーカーの停止
	err = gm.StopWorker("stop-test-worker")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// ワーカー数が0になることを確認
	if count := gm.GetWorkerCount(); count != 0 {
		t.Errorf("Expected worker count 0, got %d", count)
	}
}

func TestGoroutineManager_StopWorker_NotFound(t *testing.T) {
	gm := NewGoroutineManager()

	// 異常系: 存在しないワーカーの停止
	err := gm.StopWorker("non-existent-worker")
	if err == nil {
		t.Error("Expected error for non-existent worker, got nil")
	}
}

func TestGoroutineManager_CollectMetrics(t *testing.T) {
	gm := NewGoroutineManager()

	// メトリクス収集の実行
	gm.CollectMetrics()

	// メトリクスが収集されることを確認
	goroutines, memory, timestamp := gm.GetMetrics()

	if goroutines <= 0 {
		t.Errorf("Expected positive goroutine count, got %d", goroutines)
	}

	if memory < 0 {
		t.Errorf("Expected non-negative memory usage, got %f", memory)
	}

	if timestamp.IsZero() {
		t.Error("Expected non-zero timestamp")
	}
}
