package main

import (
	"context"
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

	if optimizer.oldStyleMap == nil {
		t.Error("oldStyleMap not initialized")
	}

	if optimizer.newStyleMap == nil {
		t.Error("newStyleMap not initialized")
	}

	if optimizer.concurrentTasks == nil {
		t.Error("concurrentTasks channel not initialized")
	}
}

func TestPerformanceOptimizerStartStop(t *testing.T) {
	optimizer := NewPerformanceOptimizer(2)
	ctx := context.Background()

	// システム開始
	err := optimizer.Start(ctx)
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	if !optimizer.running {
		t.Error("Expected running to be true after Start")
	}

	// 少し待機してワーカーが起動するのを確認
	time.Sleep(100 * time.Millisecond)

	// システム停止
	err = optimizer.Stop()
	if err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	if optimizer.running {
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
	ctx := context.Background()

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

	expectedMsg := "performance optimizer not running"
	if err.Error() != expectedMsg {
		t.Errorf("Expected error message '%s', got '%s'", expectedMsg, err.Error())
	}
}

func TestGetOptimizationReport(t *testing.T) {
	optimizer := NewPerformanceOptimizer(4)
	ctx := context.Background()

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
	for i := 0; i < iterations; i++ {
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

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
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

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
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
	ctx := context.Background()

	err := optimizer.Start(ctx)
	if err != nil {
		b.Fatalf("Start failed: %v", err)
	}
	defer func() {
		if err := optimizer.Stop(); err != nil {
			b.Logf("Failed to stop optimizer: %v", err)
		}
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := optimizer.BenchmarkConcurrentProcessing(50)
		if err != nil {
			b.Fatalf("BenchmarkConcurrentProcessing failed: %v", err)
		}
		if result == nil {
			b.Fatal("Result is nil")
		}
	}
}
