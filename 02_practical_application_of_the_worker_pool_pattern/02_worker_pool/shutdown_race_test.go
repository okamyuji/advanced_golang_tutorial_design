package main

import (
	"testing"
	"time"
)

func TestWorkerPool_SubmitDuringShutdownDoesNotPanic(t *testing.T) {
	for range 50 {
		wp := NewWorkerPool(2, 4, 10)
		if err := wp.Start(); err != nil {
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
					_ = wp.SubmitTask(Task{ID: i, Execute: func(any) error { return nil }})
				}
			}
		}()
		if err := wp.Shutdown(time.Second); err != nil {
			t.Fatal(err)
		}
		close(stop)
		<-done
		if err := wp.SubmitTask(Task{ID: -1, Execute: func(any) error { return nil }}); err == nil {
			t.Fatal("expected error when submitting after shutdown")
		}
	}
}
