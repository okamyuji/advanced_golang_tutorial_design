package main

import (
	"testing"
	"time"
)

func TestWebScraper_SubmitDuringShutdownDoesNotPanic(t *testing.T) {
	for range 50 {
		ws := NewWebScraper(2, 1000)
		ws.Start()
		stop, done := make(chan struct{}), make(chan struct{})
		go func() {
			defer close(done)
			for {
				select {
				case <-stop:
					return
				default:
					_ = ws.SubmitURL("http://127.0.0.1:1/", 0)
				}
			}
		}()
		if err := ws.Shutdown(time.Second); err != nil {
			t.Fatal(err)
		}
		close(stop)
		<-done
		if err := ws.SubmitURL("http://127.0.0.1:1/", 0); err == nil {
			t.Fatal("expected error when submitting after shutdown")
		}
	}
}
