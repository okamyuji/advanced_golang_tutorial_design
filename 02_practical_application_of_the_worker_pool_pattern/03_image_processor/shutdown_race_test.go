package main

import (
	"testing"
	"time"
)

func TestImageProcessor_SubmitDuringShutdownDoesNotPanic(t *testing.T) {
	for range 50 {
		ip := NewImageProcessor(t.TempDir())
		if err := ip.Start(); err != nil {
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
					_ = ip.SubmitTask(ImageTask{ID: i, Operation: OperationConvert})
				}
			}
		}()
		if err := ip.Shutdown(5 * time.Second); err != nil {
			t.Fatal(err)
		}
		close(stop)
		<-done
		if err := ip.SubmitTask(ImageTask{ID: -1}); err == nil {
			t.Fatal("expected error when submitting after shutdown")
		}
	}
}
