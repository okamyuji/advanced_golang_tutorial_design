package main

import (
	"sync"
	"testing"
	"time"
)

// newTestManager テスト用にメトリクス監視を無効化したマネージャーを作成します
func newTestManager() *DistributedContextManager {
	config := NewManagerConfig("node-test")
	config.EventQueueSize = 10
	config.MessageQueueSize = 10
	config.HeartbeatInterval = 10 * time.Second
	config.SyncInterval = 10 * time.Second
	config.EnableMetrics = false
	return NewDistributedContextManager(config)
}

// TestDistributedContextManagerBasicFlow コンテキストの作成・伝播・更新・キャンセルが
// エラーなく行え、統計に反映されることを確かめます。
func TestDistributedContextManagerBasicFlow(t *testing.T) {
	manager := newTestManager()
	if err := manager.JoinCluster("node-2", "127.0.0.1:9000"); err != nil {
		t.Fatalf("Failed to join cluster: %v", err)
	}
	if err := manager.Start(); err != nil {
		t.Fatalf("Failed to start manager: %v", err)
	}
	defer func() {
		if err := manager.Shutdown(2 * time.Second); err != nil {
			t.Logf("Failed to shutdown manager: %v", err)
		}
	}()

	ctx, err := manager.CreateContext("", map[string]any{"key": "value"}, time.Time{})
	if err != nil {
		t.Fatalf("Failed to create context: %v", err)
	}

	if err := manager.PropagateContext(ctx.ID, nil); err != nil {
		t.Fatalf("Failed to propagate context: %v", err)
	}

	if err := manager.UpdateContext(ctx.ID, map[string]any{"updated": true}); err != nil {
		t.Fatalf("Failed to update context: %v", err)
	}

	got, err := manager.GetContext(ctx.ID)
	if err != nil {
		t.Fatalf("Failed to get context: %v", err)
	}
	if got.Values["updated"] != true {
		t.Errorf("Expected context to have updated=true, got %v", got.Values)
	}

	if err := manager.CancelContext(ctx.ID); err != nil {
		t.Fatalf("Failed to cancel context: %v", err)
	}

	if _, err := manager.GetContext(ctx.ID); err == nil {
		t.Error("Expected error getting cancelled context")
	}

	stats := manager.GetStats()
	if stats["contexts_created"].(int64) < 1 {
		t.Errorf("Expected at least 1 created context, got %v", stats["contexts_created"])
	}
}

// TestShutdownConcurrentWithSubmit CreateContext/SubmitIncomingMessageとShutdownを
// 並行させてもpanicせず、停止後の投入は必ずエラーになることを確かめる回帰テストです。
func TestShutdownConcurrentWithSubmit(t *testing.T) {
	for range 50 {
		manager := newTestManager()
		if err := manager.Start(); err != nil {
			t.Fatalf("Failed to start manager: %v", err)
		}

		var submitWg sync.WaitGroup
		stop := make(chan struct{})

		const submitters = 8
		for range submitters {
			submitWg.Go(func() {
				for {
					select {
					case <-stop:
						return
					default:
						_, _ = manager.CreateContext("", map[string]any{"k": "v"}, time.Time{})
						_ = manager.SubmitIncomingMessage(&ContextMessage{Type: MessageHeartbeat, SourceNode: "node-x"})
					}
				}
			})
		}

		time.Sleep(time.Millisecond)

		if err := manager.Shutdown(1 * time.Second); err != nil {
			t.Fatalf("Failed to shutdown manager: %v", err)
		}

		close(stop)
		submitWg.Wait()

		// 停止後の投入は必ずエラーになる
		if _, err := manager.CreateContext("", nil, time.Time{}); err == nil {
			t.Error("Expected error when creating context after shutdown")
		}
		if err := manager.SubmitIncomingMessage(&ContextMessage{Type: MessageHeartbeat}); err == nil {
			t.Error("Expected error when submitting message after shutdown")
		}
	}
}
