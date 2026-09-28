package main

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestNewPerformanceOptimizer(t *testing.T) {
	optimizer := NewPerformanceOptimizer(4)

	if optimizer == nil {
		t.Fatal("NewPerformanceOptimizer returned nil")
	}

	if optimizer.workerCount != 4 {
		t.Errorf("Expected workerCount 4, got %d", optimizer.workerCount)
	}

	if optimizer.dataMap == nil {
		t.Error("dataMap not initialized")
	}

	if optimizer.concurrentTasks == nil {
		t.Error("concurrentTasks channel not initialized")
	}
}

func TestPerformanceOptimizerStartStop(t *testing.T) {
	optimizer := NewPerformanceOptimizer(2)
	ctx := t.Context()

	// システム開始
	err := optimizer.Start(ctx)
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	if !optimizer.isRunning() {
		t.Error("Expected running to be true after Start")
	}

	// システム停止
	err = optimizer.Stop()
	if err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	if optimizer.isRunning() {
		t.Error("Expected running to be false after Stop")
	}
}

func TestBenchmarkMapOperations(t *testing.T) {
	optimizer := NewPerformanceOptimizer(2)

	result, err := optimizer.BenchmarkMapOperations(1000)
	if err != nil {
		t.Fatalf("BenchmarkMapOperations failed: %v", err)
	}

	if result == nil {
		t.Fatal("BenchmarkResult is nil")
	}

	if result.OperationType != "map_operations" {
		t.Errorf("Expected operation_type 'map_operations', got '%s'", result.OperationType)
	}

	if result.TotalOperations != 1000 {
		t.Errorf("Expected 1000 operations, got %d", result.TotalOperations)
	}

	if result.ExecutionTime <= 0 {
		t.Error("ExecutionTime should be positive")
	}

	if result.OperationsPerSec <= 0 {
		t.Error("OperationsPerSec should be positive")
	}

	t.Logf("Map operations: %.2f ops/sec, Memory: %d bytes",
		result.OperationsPerSec, result.MemoryUsage)
}

func TestBenchmarkMemoryAllocation(t *testing.T) {
	optimizer := NewPerformanceOptimizer(2)

	result, err := optimizer.BenchmarkMemoryAllocation(5000)
	if err != nil {
		t.Fatalf("BenchmarkMemoryAllocation failed: %v", err)
	}

	if result == nil {
		t.Fatal("BenchmarkResult is nil")
	}

	if result.OperationType != "memory_allocation" {
		t.Errorf("Expected operation_type 'memory_allocation', got '%s'", result.OperationType)
	}

	if result.TotalOperations != 5000 {
		t.Errorf("Expected 5000 operations, got %d", result.TotalOperations)
	}

	if result.ExecutionTime <= 0 {
		t.Error("ExecutionTime should be positive")
	}

	if result.OperationsPerSec <= 0 {
		t.Error("OperationsPerSec should be positive")
	}

	t.Logf("Memory allocation: %.2f ops/sec, Allocations: %d",
		result.OperationsPerSec, result.AllocCount)
}

func TestBenchmarkConcurrentProcessing(t *testing.T) {
	optimizer := NewPerformanceOptimizer(2)
	ctx := t.Context()

	// システム開始
	err := optimizer.Start(ctx)
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() {
		if err := optimizer.Stop(); err != nil {
			t.Logf("Failed to stop optimizer: %v", err)
		}
	}()

	result, err := optimizer.BenchmarkConcurrentProcessing(100)
	if err != nil {
		t.Fatalf("BenchmarkConcurrentProcessing failed: %v", err)
	}

	if result == nil {
		t.Fatal("BenchmarkResult is nil")
	}

	if result.OperationType != "concurrent_processing" {
		t.Errorf("Expected operation_type 'concurrent_processing', got '%s'", result.OperationType)
	}

	if result.TotalOperations != 100 {
		t.Errorf("Expected 100 operations, got %d", result.TotalOperations)
	}

	if result.ExecutionTime <= 0 {
		t.Error("ExecutionTime should be positive")
	}

	if result.OperationsPerSec <= 0 {
		t.Error("OperationsPerSec should be positive")
	}

	t.Logf("Concurrent processing: %.2f ops/sec, Time: %v",
		result.OperationsPerSec, result.ExecutionTime)
}

func TestBenchmarkConcurrentProcessingNotRunning(t *testing.T) {
	optimizer := NewPerformanceOptimizer(2)

	// システムを開始せずにベンチマーク実行
	_, err := optimizer.BenchmarkConcurrentProcessing(10)
	if err == nil {
		t.Error("Expected error when system not running")
	}

	if !errors.Is(err, errNotRunning) {
		t.Errorf("Expected errNotRunning, got %v", err)
	}
}

func TestGetOptimizationReport(t *testing.T) {
	optimizer := NewPerformanceOptimizer(4)
	ctx := t.Context()

	// システム開始
	err := optimizer.Start(ctx)
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() {
		if err := optimizer.Stop(); err != nil {
			t.Logf("Failed to stop optimizer: %v", err)
		}
	}()

	report := optimizer.GetOptimizationReport()
	if report == nil {
		t.Fatal("GetOptimizationReport returned nil")
	}

	// 必要なフィールドが存在することを確認
	expectedFields := []string{
		"goroutine_count",
		"memory_alloc",
		"memory_total_alloc",
		"memory_sys",
		"gc_cycles",
		"alloc_count",
		"worker_count",
		"system_running",
	}

	for _, field := range expectedFields {
		if _, ok := report[field]; !ok {
			t.Errorf("Report missing field: %s", field)
		}
	}

	// 値の妥当性チェック
	if workerCount, ok := report["worker_count"].(int); !ok || workerCount != 4 {
		t.Errorf("Expected worker_count 4, got %v", report["worker_count"])
	}

	if running, ok := report["system_running"].(bool); !ok || !running {
		t.Errorf("Expected system_running true, got %v", report["system_running"])
	}

	t.Logf("Optimization report: %+v", report)
}

func TestObjectPoolEfficiency(t *testing.T) {
	optimizer := NewPerformanceOptimizer(2)

	// オブジェクトプールの効率性をテスト
	iterations := 1000
	for range iterations {
		objInterface := optimizer.objectPool.Get()
		obj := objInterface.([]byte)
		if len(obj) != 64 {
			t.Errorf("Expected object size 64, got %d", len(obj))
		}
		optimizer.objectPool.Put(objInterface)
	}

	// プールが正常に機能していることを確認
	obj1Interface := optimizer.objectPool.Get()
	obj1 := obj1Interface.([]byte)
	obj2Interface := optimizer.objectPool.Get()
	obj2 := obj2Interface.([]byte)

	if len(obj1) != 64 || len(obj2) != 64 {
		t.Error("Object pool not working correctly")
	}

	optimizer.objectPool.Put(obj1Interface)
	optimizer.objectPool.Put(obj2Interface)
}

func BenchmarkMapOperations(b *testing.B) {
	optimizer := NewPerformanceOptimizer(2)

	for b.Loop() {
		result, err := optimizer.BenchmarkMapOperations(100)
		if err != nil {
			b.Fatalf("BenchmarkMapOperations failed: %v", err)
		}
		if result == nil {
			b.Fatal("Result is nil")
		}
	}
}

func BenchmarkMemoryAllocation(b *testing.B) {
	optimizer := NewPerformanceOptimizer(2)

	for b.Loop() {
		result, err := optimizer.BenchmarkMemoryAllocation(1000)
		if err != nil {
			b.Fatalf("BenchmarkMemoryAllocation failed: %v", err)
		}
		if result == nil {
			b.Fatal("Result is nil")
		}
	}
}

func BenchmarkConcurrentProcessing(b *testing.B) {
	optimizer := NewPerformanceOptimizer(4)

	err := optimizer.Start(b.Context())
	if err != nil {
		b.Fatalf("Start failed: %v", err)
	}
	defer func() {
		if err := optimizer.Stop(); err != nil {
			b.Logf("Failed to stop optimizer: %v", err)
		}
	}()

	for b.Loop() {
		result, err := optimizer.BenchmarkConcurrentProcessing(50)
		if err != nil {
			b.Fatalf("BenchmarkConcurrentProcessing failed: %v", err)
		}
		if result == nil {
			b.Fatal("Result is nil")
		}
	}
}

// TestStopConcurrentWithSubmit 投入と停止を並行させても panic せず、停止後の投入がエラーになることを確かめる
func TestStopConcurrentWithSubmit(t *testing.T) {
	for range 50 {
		optimizer := NewPerformanceOptimizer(4)
		if err := optimizer.Start(t.Context()); err != nil {
			t.Fatalf("Start failed: %v", err)
		}

		errc := make(chan error, 1)
		go func() {
			_, err := optimizer.BenchmarkConcurrentProcessing(500)
			errc <- err
		}()
		go optimizer.GetOptimizationReport()

		if err := optimizer.Stop(); err != nil {
			t.Fatalf("Stop failed: %v", err)
		}
		<-errc

		if _, err := optimizer.BenchmarkConcurrentProcessing(1); err == nil {
			t.Fatal("停止後の投入がエラーになっていない")
		}
	}
}

func TestStartStopTwice(t *testing.T) {
	optimizer := NewPerformanceOptimizer(2)
	if err := optimizer.Start(t.Context()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	if err := optimizer.Start(t.Context()); err == nil {
		t.Error("二重の Start がエラーになっていない")
	}
	if err := optimizer.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}
	if err := optimizer.Stop(); err != nil {
		t.Errorf("二度目の Stop がエラーを返した: %v", err)
	}
}

// TestBenchmarkConcurrentProcessingCanceledContext ワーカーの ctx が先に終わっても、投入側が止まらずにエラーで返ることを確かめる
func TestBenchmarkConcurrentProcessingCanceledContext(t *testing.T) {
	optimizer := NewPerformanceOptimizer(2)
	ctx, cancel := context.WithCancel(t.Context())
	if err := optimizer.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	cancel()

	done := make(chan error, 1)
	go func() {
		_, err := optimizer.BenchmarkConcurrentProcessing(1000)
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Error("ctx 終了後の投入がエラーになっていない")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("ctx 終了後に BenchmarkConcurrentProcessing が返らない")
	}
	if err := optimizer.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}
}

// TestBenchmarkMemoryUsageDoesNotUnderflow Alloc は GC で減るので、差分が uint64 のまま桁あふれしないことを確かめる
func TestBenchmarkMemoryUsageDoesNotUnderflow(t *testing.T) {
	optimizer := NewPerformanceOptimizer(2)
	for range 5 {
		result, err := optimizer.BenchmarkMapOperations(200000)
		if err != nil {
			t.Fatalf("BenchmarkMapOperations failed: %v", err)
		}
		if result.MemoryUsage > 1<<40 {
			t.Fatalf("MemoryUsage が桁あふれしている: %d", result.MemoryUsage)
		}
	}
}
