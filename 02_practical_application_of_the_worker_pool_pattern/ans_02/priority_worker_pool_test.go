package main

import (
	"context"
	"testing"
	"testing/synctest"
	"time"
)

func TestPriorityTaskQueue_Order(t *testing.T) {
	q := NewPriorityTaskQueue()
	base := time.Now()
	// 優先度（0が最高）と投入時刻を混ぜて入れる
	for i, p := range []int{2, 0, 1, 0, 2} {
		q.Enqueue(&PriorityTask{ID: i, Priority: p, CreatedAt: base.Add(time.Duration(i) * time.Millisecond)})
	}

	var got []int
	for range 5 {
		task, ok := q.Dequeue(t.Context())
		if !ok {
			t.Fatal("expected a task")
		}
		got = append(got, task.ID)
	}

	// 優先度が高い順、同じ優先度では先に入れた順（FIFO）
	want := []int{1, 3, 2, 0, 4}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("expected order %v, got %v", want, got)
		}
	}
}

func TestPriorityTaskQueue_DequeueWaitsAndUnblocks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := NewPriorityTaskQueue()
		result := make(chan int, 1)
		go func() {
			task, ok := q.Dequeue(t.Context())
			if ok {
				result <- task.ID
			}
		}()

		// 空のキューではDequeueが待機状態に入る（ポーリングしない）
		synctest.Wait()
		select {
		case id := <-result:
			t.Fatalf("Dequeue returned %d before any Enqueue", id)
		default:
		}

		q.Enqueue(&PriorityTask{ID: 42, CreatedAt: time.Now()})
		if id := <-result; id != 42 {
			t.Errorf("expected task 42, got %d", id)
		}
	})
}

func TestPriorityTaskQueue_DequeueReturnsOnCancelAndClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := NewPriorityTaskQueue()
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan bool, 1)
		go func() {
			_, ok := q.Dequeue(ctx)
			done <- ok
		}()
		synctest.Wait()
		cancel()
		if ok := <-done; ok {
			t.Error("expected ok=false after cancel")
		}

		go func() {
			_, ok := q.Dequeue(t.Context())
			done <- ok
		}()
		synctest.Wait()
		q.Close()
		if ok := <-done; ok {
			t.Error("expected ok=false after Close")
		}
	})
}

func TestPriorityWorkerPool_SubmitDuringShutdownDoesNotPanic(t *testing.T) {
	for range 50 {
		pwp := NewPriorityWorkerPool(2)
		if err := pwp.Start(); err != nil {
			t.Fatal(err)
		}
		stop, done := make(chan struct{}), make(chan struct{})
		go func() {
			defer close(done)
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
					_ = pwp.SubmitTask(PriorityTask{ID: i, Urgent: i%2 == 0, Execute: func(any) error { return nil }})
				}
			}
		}()
		if err := pwp.Shutdown(time.Second); err != nil {
			t.Fatal(err)
		}
		close(stop)
		<-done
		if err := pwp.SubmitTask(PriorityTask{ID: -1, Execute: func(any) error { return nil }}); err == nil {
			t.Fatal("expected error when submitting after shutdown")
		}
	}
}
