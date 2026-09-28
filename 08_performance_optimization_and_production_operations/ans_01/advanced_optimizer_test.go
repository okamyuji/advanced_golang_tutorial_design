package main

import (
	"sync"
	"testing"
	"time"
)

// TestProcessHighLoadDataProcessesAllTasks 高負荷処理が全タスクを処理し、統計が処理件数と一致することを確かめる
func TestProcessHighLoadDataProcessesAllTasks(t *testing.T) {
	optimizer := NewAdvancedOptimizer(4)
	defer func() {
		if err := optimizer.Stop(); err != nil {
			t.Errorf("Stop failed: %v", err)
		}
	}()

	const dataSize = 2000
	if err := optimizer.ProcessHighLoadData(dataSize); err != nil {
		t.Fatalf("ProcessHighLoadData failed: %v", err)
	}

	report := optimizer.GetOptimizationReport()
	if report.WorkerPoolStats.TasksProcessed != dataSize {
		t.Errorf("TasksProcessed = %d, want %d", report.WorkerPoolStats.TasksProcessed, dataSize)
	}
	if got := optimizer.resultCollector.resultCount.Load(); got != dataSize {
		t.Errorf("resultCount = %d, want %d", got, dataSize)
	}
	if report.MapStats.TotalMaps != dataSize/1000 {
		t.Errorf("TotalMaps = %d, want %d", report.MapStats.TotalMaps, dataSize/1000)
	}
}

// TestPoolHitsExcludeMisses プールのヒット数に、New で作り直した分（ミス）が含まれないことを確かめる
func TestPoolHitsExcludeMisses(t *testing.T) {
	optimizer := NewAdvancedOptimizer(2)
	defer func() {
		if err := optimizer.Stop(); err != nil {
			t.Errorf("Stop failed: %v", err)
		}
	}()

	if err := optimizer.ProcessHighLoadData(3000); err != nil {
		t.Fatalf("ProcessHighLoadData failed: %v", err)
	}

	stats := optimizer.GetOptimizationReport().AllocStats
	created := stats.SmallObjects + stats.MediumObjects + stats.LargeObjects
	if created == 0 {
		t.Fatal("プールが一度も新しいオブジェクトを作っていない")
	}
	if stats.PoolMisses != created {
		t.Errorf("PoolMisses = %d, want %d (New で作った数)", stats.PoolMisses, created)
	}
	if stats.PoolHits+stats.PoolMisses != stats.TotalAllocs {
		t.Errorf("PoolHits(%d) + PoolMisses(%d) != TotalAllocs(%d)", stats.PoolHits, stats.PoolMisses, stats.TotalAllocs)
	}
}

// TestStopConcurrentWithProcess 投入と停止を並行させても panic せず、停止後の投入がエラーになることを確かめる
func TestStopConcurrentWithProcess(t *testing.T) {
	for range 50 {
		optimizer := NewAdvancedOptimizer(4)

		errc := make(chan error, 1)
		go func() { errc <- optimizer.ProcessHighLoadData(500) }()
		if err := optimizer.Stop(); err != nil {
			t.Fatalf("Stop failed: %v", err)
		}
		select {
		case <-errc:
		case <-time.After(10 * time.Second):
			t.Fatal("停止後に ProcessHighLoadData が返らない")
		}

		if err := optimizer.Stop(); err != nil {
			t.Fatalf("二度目の Stop がエラーを返した: %v", err)
		}
		if err := optimizer.ProcessHighLoadData(10); err == nil {
			t.Fatal("停止後の投入がエラーになっていない")
		}
	}
}

// TestReportConcurrentWithProcess レポートの読み出しと、処理による書き込みを並行させる
func TestReportConcurrentWithProcess(t *testing.T) {
	optimizer := NewAdvancedOptimizer(4)
	defer func() {
		if err := optimizer.Stop(); err != nil {
			t.Errorf("Stop failed: %v", err)
		}
	}()

	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		defer close(done)
		for range 3 {
			if err := optimizer.ProcessHighLoadData(1000); err != nil {
				t.Errorf("ProcessHighLoadData failed: %v", err)
			}
		}
	})
	// 書き込み側が終わるまで読み続け、両者を確実に重ねる
	wg.Go(func() {
		for {
			select {
			case <-done:
				return
			default:
			}
			if report := optimizer.GetOptimizationReport(); report != nil {
				_ = report.AllocStats.TotalAllocs
			}
		}
	})
	wg.Wait()
}
