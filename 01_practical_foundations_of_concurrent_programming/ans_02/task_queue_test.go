package main

import (
	"testing"
	"testing/synctest"
	"time"
)

func TestTaskQueueSystem_DropOldestAndGracefulShutdown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tqs := NewTaskQueueSystem(5, 3)
		// ワーカー起動前にキュー容量を超えて投入すると、最も古いタスクが1件破棄される
		for id := range 6 {
			if err := tqs.SubmitTask(Task{ID: id + 1, CreatedAt: time.Now(), Execute: func() error {
				time.Sleep(500 * time.Millisecond)
				return nil
			}}); err != nil {
				t.Fatalf("submit %d: %v", id+1, err)
			}
		}
		if _, dropped, _, queued := tqs.GetStats(); dropped != 1 || queued != 5 {
			t.Fatalf("expected dropped=1 queued=5, got dropped=%d queued=%d", dropped, queued)
		}

		tqs.Start(t.Context())
		if err := tqs.Shutdown(5 * time.Second); err != nil {
			t.Fatalf("shutdown: %v", err)
		}

		// Shutdown キューに残ったタスクを処理し終えてから戻る
		if processed, _, _, _ := tqs.GetStats(); processed != 5 {
			t.Errorf("expected processed=5, got %d", processed)
		}
		if err := tqs.SubmitTask(Task{ID: 99, Execute: func() error { return nil }}); err == nil {
			t.Error("expected error when submitting after shutdown")
		}
	})
}
