package main

import (
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func workerCount(p *DynamicWorkerPool) int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.workers)
}

func TestDynamicWorkerPool_ScaleDownStopsLeastRecentlyActiveWorker(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pool := NewDynamicWorkerPool(1, 4, 10)
		for range 3 {
			if err := pool.addWorker(); err != nil {
				t.Fatal(err)
			}
		}
		// ワーカー2と3だけ最近タスクを受け取ったことにする
		time.Sleep(time.Second)
		pool.mu.RLock()
		for id, w := range pool.workers {
			if id != 1 {
				w.lastActivity.Store(time.Now().UnixNano())
			}
		}
		pool.mu.RUnlock()

		pool.scaleDown()
		synctest.Wait()

		pool.mu.RLock()
		_, stillThere := pool.workers[1]
		pool.mu.RUnlock()
		if stillThere || workerCount(pool) != 2 {
			t.Fatalf("expected worker 1 to be stopped and 2 workers left, got %d workers (worker1 present=%v)", workerCount(pool), stillThere)
		}

		// 最小ワーカー数を下回るスケールダウンはしない
		pool.scaleDown()
		pool.scaleDown()
		synctest.Wait()
		if got := workerCount(pool); got != 1 {
			t.Fatalf("expected minWorkers=1 to be kept, got %d", got)
		}

		if err := pool.Shutdown(time.Second); err != nil {
			t.Fatal(err)
		}
	})
}

func TestDynamicWorkerPool_ProcessesTasksAndRejectsAfterShutdown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pool := NewDynamicWorkerPool(2, 4, 10)
		if err := pool.Start(); err != nil {
			t.Fatal(err)
		}

		var done atomic.Int64
		for i := range 5 {
			if err := pool.SubmitTask(DynamicTask{ID: i, Execute: func(any) error {
				time.Sleep(100 * time.Millisecond)
				done.Add(1)
				return nil
			}}); err != nil {
				t.Fatal(err)
			}
		}
		synctest.Sleep(time.Second)
		if got := done.Load(); got != 5 {
			t.Fatalf("expected 5 tasks done, got %d", got)
		}

		if err := pool.Shutdown(time.Second); err != nil {
			t.Fatal(err)
		}
		if err := pool.SubmitTask(DynamicTask{ID: 99, Execute: func(any) error { return nil }}); err == nil {
			t.Error("expected error when submitting after shutdown")
		}
	})
}
