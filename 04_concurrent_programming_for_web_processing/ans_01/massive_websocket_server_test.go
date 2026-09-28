package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func testConfig() *ServerConfig {
	return &ServerConfig{
		MaxConnections:        10,
		KeepAliveTimeout:      time.Hour,
		WriteTimeout:          time.Hour,
		ReadTimeout:           time.Hour,
		MessageQueueSize:      8,
		BroadcastWorkers:      4,
		CleanupInterval:       time.Hour,
		ResourceCheckInterval: time.Hour,
		MemoryLimitMB:         1024,
		HeartbeatInterval:     time.Hour,
	}
}

// TestSSEConnectionLifecycle SSE接続の登録・切断でクライアント数が正しく増減することを確認します
func TestSSEConnectionLifecycle(t *testing.T) {
	server := NewMassiveConnectionServer(testConfig())
	go server.hub.run(server)
	defer close(server.shutdown)

	req := httptest.NewRequest(http.MethodGet, "/events", nil)
	ctx, cancel := context.WithCancel(req.Context())
	req = req.WithContext(ctx)
	w := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		defer close(done)
		server.handleSSEConnection(w, req)
	}()

	// ハブがクライアントを登録するまで待つ
	for server.hub.activeClients.Load() < 1 {
		time.Sleep(time.Millisecond)
	}

	// クライアント切断をシミュレート
	cancel()
	<-done

	// unregisterがハブループで処理されるまで待つ
	for server.hub.activeClients.Load() != 0 {
		time.Sleep(time.Millisecond)
	}
}

// TestConcurrentBroadcastAndUnregister ブロードキャストとunregisterを並行させても
// close済みチャネルへの送信でpanicしないことを確認する回帰テスト。
func TestConcurrentBroadcastAndUnregister(t *testing.T) {
	for iter := range 50 {
		server := NewMassiveConnectionServer(testConfig())

		clients := make([]*SSEClient, 5)
		for i := range clients {
			ctx, cancel := context.WithCancel(context.Background())
			client := &SSEClient{
				ID:     fmt.Sprintf("client_%d_%d", iter, i),
				send:   make(chan []byte, 4),
				ctx:    ctx,
				cancel: cancel,
			}
			server.clients.Store(client.ID, client)
			server.hub.activeClients.Add(1)
			clients[i] = client
		}

		var wg sync.WaitGroup
		// 送信側: trySendを連打する
		for _, client := range clients {
			wg.Go(func() {
				for range 20 {
					_ = client.trySend([]byte("hello"))
				}
			})
		}
		// close側: unregisterClientを並行して呼ぶ
		for _, client := range clients {
			wg.Go(func() {
				server.hub.unregisterClient(client, server)
			})
		}

		wg.Wait()

		// 停止後の送信はエラーになる
		for _, client := range clients {
			if err := client.trySend([]byte("late")); err == nil {
				t.Errorf("iter %d: expected error sending to closed client %s", iter, client.ID)
			}
		}
	}
}
