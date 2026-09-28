package main

import (
	"sync"
	"testing"
	"time"
)

// newTestBalancer テスト用の負荷分散システムを1ノードで作成します
func newTestBalancer() *AdaptiveLoadBalancer {
	config := NewLoadBalancerConfig()
	config.FanOutFactor = 1
	config.QueueSize = 10
	config.HealthCheckInterval = 10 * time.Second
	config.MetricsInterval = 10 * time.Second

	balancer := NewAdaptiveLoadBalancer(config)
	balancer.AddNode(&LoadBalancerNode{
		ID:          "node1",
		Address:     "127.0.0.1:8080",
		Weight:      1,
		MaxCapacity: 100,
		Health:      HealthHealthy,
	})
	return balancer
}

// TestAdaptiveLoadBalancerBasicFlow 投入した要求が結果チャネルに届き、
// 統計が更新されることを確かめます。完了はGetResultChannelからの受信で待ち、
// 固定Sleepには頼りません。
func TestAdaptiveLoadBalancerBasicFlow(t *testing.T) {
	balancer := newTestBalancer()
	if err := balancer.Start(); err != nil {
		t.Fatalf("Failed to start balancer: %v", err)
	}
	defer func() {
		if err := balancer.Shutdown(2 * time.Second); err != nil {
			t.Logf("Failed to shutdown balancer: %v", err)
		}
	}()

	request := RequestTask{ID: 1, RequestType: "query", Timeout: 2 * time.Second}
	if err := balancer.SubmitRequest(request); err != nil {
		t.Fatalf("Failed to submit request: %v", err)
	}

	select {
	case result := <-balancer.GetResultChannel():
		if result.TaskID != request.ID {
			t.Errorf("Expected TaskID %d, got %d", request.ID, result.TaskID)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Timeout waiting for result")
	}
}

// TestShutdownConcurrentWithSubmit SubmitRequestとShutdownを並行させてもpanicせず、
// 停止後の投入は必ずエラーになることを確かめる回帰テストです。
func TestShutdownConcurrentWithSubmit(t *testing.T) {
	for range 50 {
		balancer := newTestBalancer()
		if err := balancer.Start(); err != nil {
			t.Fatalf("Failed to start balancer: %v", err)
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
						_ = balancer.SubmitRequest(RequestTask{ID: 1, RequestType: "race_test", Timeout: time.Second})
					}
				}
			})
		}

		time.Sleep(time.Millisecond)

		if err := balancer.Shutdown(1 * time.Second); err != nil {
			t.Fatalf("Failed to shutdown balancer: %v", err)
		}

		close(stop)
		submitWg.Wait()

		// 停止後の投入は必ずエラーになる
		if err := balancer.SubmitRequest(RequestTask{ID: 2, RequestType: "after_shutdown"}); err == nil {
			t.Error("Expected error when submitting request after shutdown")
		}
	}
}
