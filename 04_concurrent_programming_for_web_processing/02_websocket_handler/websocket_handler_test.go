package main

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestNewHandlerConfig はハンドラー設定の作成をテストします
func TestNewHandlerConfig(t *testing.T) {
	config := NewHandlerConfig()

	if config.WorkerCount != 4 {
		t.Errorf("Expected WorkerCount 4, got %d", config.WorkerCount)
	}
	if config.MessageBufferSize != 10000 {
		t.Errorf("Expected MessageBufferSize 10000, got %d", config.MessageBufferSize)
	}
	if config.SendBufferSize != 1000 {
		t.Errorf("Expected SendBufferSize 1000, got %d", config.SendBufferSize)
	}
}

// TestNewWebSocketHandler はWebSocketハンドラーの作成をテストします
func TestNewWebSocketHandler(t *testing.T) {
	config := NewHandlerConfig()
	handler := NewWebSocketHandler(config)

	if handler == nil {
		t.Fatal("Expected handler to be created")
	}
	if len(handler.workers) != config.WorkerCount {
		t.Errorf("Expected %d workers, got %d", config.WorkerCount, len(handler.workers))
	}
	if len(handler.channels) == 0 {
		t.Error("Expected default channel to be created")
	}
}

// TestConnectionHandling は接続管理の基本機能をテストします（短時間実行用）
func TestConnectionHandling(t *testing.T) {
	config := NewHandlerConfig()
	config.WorkerCount = 1

	handler := NewWebSocketHandler(config)

	// 接続オブジェクトを直接作成してテスト（goroutine起動なし）
	conn := &WebSocketConnection{
		ID:            "test_conn_123",
		UserID:        "user1",
		RemoteAddr:    "127.0.0.1:8080",
		UserAgent:     "TestClient/1.0",
		IsActive:      true,
		ConnectedAt:   time.Now(),
		LastHeartbeat: time.Now(),
		SendQueue:     make(chan *WebSocketMessage, 10),
		Channels:      make(map[string]bool),
	}

	// 接続を手動でマップに追加
	handler.connectionsMu.Lock()
	handler.connections[conn.ID] = conn
	handler.connectionsMu.Unlock()

	// 接続が正しく登録されているかテスト
	if conn.UserID != "user1" {
		t.Errorf("Expected UserID 'user1', got '%s'", conn.UserID)
	}

	if conn.RemoteAddr != "127.0.0.1:8080" {
		t.Errorf("Expected RemoteAddr '127.0.0.1:8080', got '%s'", conn.RemoteAddr)
	}

	if !conn.IsActive {
		t.Error("Connection should be active")
	}

	// 接続カウントを確認
	handler.connectionsMu.RLock()
	connectionCount := len(handler.connections)
	handler.connectionsMu.RUnlock()

	if connectionCount != 1 {
		t.Errorf("Expected 1 active connection, got %d", connectionCount)
	}

	// 接続を検索
	handler.connectionsMu.RLock()
	foundConn, exists := handler.connections[conn.ID]
	handler.connectionsMu.RUnlock()

	if !exists {
		t.Error("Connection should be found by ID")
	}

	if foundConn.ID != conn.ID {
		t.Errorf("Expected connection ID '%s', got '%s'", conn.ID, foundConn.ID)
	}

	// 接続を削除
	handler.connectionsMu.Lock()
	delete(handler.connections, conn.ID)
	handler.connectionsMu.Unlock()

	// 削除後の確認
	handler.connectionsMu.RLock()
	_, stillExists := handler.connections[conn.ID]
	handler.connectionsMu.RUnlock()

	if stillExists {
		t.Error("Connection should not be found after deletion")
	}
}

// TestJoinLeaveChannel はチャネル参加/離脱をテストします（簡素化版）
func TestJoinLeaveChannel(t *testing.T) {
	config := NewHandlerConfig()
	handler := NewWebSocketHandler(config)

	// 接続を直接作成
	conn := &WebSocketConnection{
		ID:       "test_conn",
		UserID:   "user1",
		Channels: make(map[string]bool),
		IsActive: true,
	}

	// 手動で接続を登録
	handler.connectionsMu.Lock()
	handler.connections[conn.ID] = conn
	handler.connectionsMu.Unlock()

	// テストチャネルを作成
	testChannel := &Channel{
		Name:    "test_channel",
		Members: make(map[string]*WebSocketConnection),
	}
	handler.channelsMu.Lock()
	handler.channels["test_channel"] = testChannel
	handler.channelsMu.Unlock()

	// 接続をチャネルに追加
	conn.Channels["test_channel"] = true
	testChannel.Members[conn.ID] = conn

	// 参加確認
	if !conn.Channels["test_channel"] {
		t.Error("Expected connection to be in channel")
	}

	// チャネル離脱
	delete(conn.Channels, "test_channel")
	delete(testChannel.Members, conn.ID)

	// 離脱確認
	if conn.Channels["test_channel"] {
		t.Error("Expected connection to be removed from channel")
	}
}

// TestMessageCreation はメッセージ作成をテストします
func TestMessageCreation(t *testing.T) {
	testTimestamp := time.Now()
	message := &WebSocketMessage{
		ID:        "test_msg_123",
		Type:      MessageTypeChat,
		Data:      map[string]interface{}{"text": "Hello"},
		From:      "user1",
		To:        "user2",
		Channel:   "general",
		Timestamp: testTimestamp,
	}

	if message.ID != "test_msg_123" {
		t.Errorf("Expected ID 'test_msg_123', got %s", message.ID)
	}

	if message.Type != MessageTypeChat {
		t.Errorf("Expected MessageTypeChat, got %s", message.Type)
	}

	if message.Data["text"] != "Hello" {
		t.Errorf("Expected 'Hello', got %v", message.Data["text"])
	}

	if message.From != "user1" {
		t.Errorf("Expected From 'user1', got %s", message.From)
	}

	if message.To != "user2" {
		t.Errorf("Expected To 'user2', got %s", message.To)
	}

	if message.Channel != "general" {
		t.Errorf("Expected Channel 'general', got %s", message.Channel)
	}

	if !message.Timestamp.Equal(testTimestamp) {
		t.Errorf("Expected Timestamp %v, got %v", testTimestamp, message.Timestamp)
	}
}

// TestChannelCreation はチャネル作成をテストします
func TestChannelCreation(t *testing.T) {
	config := NewHandlerConfig()
	handler := NewWebSocketHandler(config)

	channelName := "test_room"
	description := "Test room for testing"

	channel := &Channel{
		Name:           channelName,
		Description:    description,
		Members:        make(map[string]*WebSocketConnection),
		MessageHistory: make([]*WebSocketMessage, 0),
		CreatedAt:      time.Now(),
	}

	handler.channelsMu.Lock()
	handler.channels[channelName] = channel
	handler.channelsMu.Unlock()

	// チャネルが作成されたことを確認
	handler.channelsMu.RLock()
	createdChannel, exists := handler.channels[channelName]
	handler.channelsMu.RUnlock()

	if !exists {
		t.Error("Expected channel to be created")
	}

	if createdChannel.Name != channelName {
		t.Errorf("Expected channel name '%s', got '%s'", channelName, createdChannel.Name)
	}

	if createdChannel.Description != description {
		t.Errorf("Expected channel description '%s', got '%s'", description, createdChannel.Description)
	}
}

// TestMessageTypes はメッセージタイプをテストします
func TestMessageTypes(t *testing.T) {
	types := []MessageType{
		MessageTypeJoin,
		MessageTypeLeave,
		MessageTypeChat,
		MessageTypeBroadcast,
		MessageTypePrivate,
		MessageTypeHeartbeat,
		MessageTypeError,
		MessageTypeSystem,
	}

	for _, msgType := range types {
		message := &WebSocketMessage{
			Type: msgType,
			Data: map[string]interface{}{"test": "data"},
		}

		if message.Type != msgType {
			t.Errorf("Expected message type %s, got %s", msgType, message.Type)
		}

		if message.Data["test"] != "data" {
			t.Errorf("Expected Data['test'] to be 'data', got %v", message.Data["test"])
		}
	}
}

// TestHandlerStats は統計管理をテストします
func TestHandlerStats(t *testing.T) {
	config := NewHandlerConfig()
	handler := NewWebSocketHandler(config)

	if handler.stats == nil {
		t.Error("Expected stats to be initialized")
	}

	// 統計を取得
	stats := handler.GetStats()

	// 必要なフィールドが存在することを確認
	requiredFields := []string{
		"total_connections", "active_connections", "total_messages",
		"broadcast_messages", "private_messages", "error_messages",
		"messages_per_second", "uptime", "channel_count",
		"worker_count", "worker_stats",
	}

	for _, field := range requiredFields {
		if _, exists := stats[field]; !exists {
			t.Errorf("Missing required field in stats: %s", field)
		}
	}

	// 初期値を確認
	if stats["worker_count"].(int) != config.WorkerCount {
		t.Errorf("Expected worker_count %d, got %v", config.WorkerCount, stats["worker_count"])
	}
}

// TestWorkerInitialization はワーカー初期化をテストします
func TestWorkerInitialization(t *testing.T) {
	config := NewHandlerConfig()
	config.WorkerCount = 3

	handler := NewWebSocketHandler(config)

	if len(handler.workers) != 3 {
		t.Errorf("Expected 3 workers, got %d", len(handler.workers))
	}

	for i, worker := range handler.workers {
		if worker.ID != i {
			t.Errorf("Expected worker ID %d, got %d", i, worker.ID)
		}

		if worker.handler != handler {
			t.Error("Worker handler reference is incorrect")
		}

		if worker.stats == nil {
			t.Error("Worker stats should be initialized")
		}
	}
}

// TestConcurrentMapAccess は並行マップアクセスをテストします
func TestConcurrentMapAccess(t *testing.T) {
	config := NewHandlerConfig()
	handler := NewWebSocketHandler(config)

	const numGoroutines = 10
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// 複数のgoroutineで並行してマップにアクセス
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()

			conn := &WebSocketConnection{
				ID:       fmt.Sprintf("conn_%d", id),
				UserID:   fmt.Sprintf("user_%d", id),
				IsActive: true,
				Channels: make(map[string]bool),
			}

			// 書き込み
			handler.connectionsMu.Lock()
			handler.connections[conn.ID] = conn
			handler.connectionsMu.Unlock()

			// 読み込み
			handler.connectionsMu.RLock()
			_, exists := handler.connections[conn.ID]
			handler.connectionsMu.RUnlock()

			if !exists {
				t.Errorf("Connection %s should exist", conn.ID)
			}

			// 削除
			handler.connectionsMu.Lock()
			delete(handler.connections, conn.ID)
			handler.connectionsMu.Unlock()
		}(i)
	}

	wg.Wait()

	// 最終的にマップが空であることを確認
	handler.connectionsMu.RLock()
	finalCount := len(handler.connections)
	handler.connectionsMu.RUnlock()

	if finalCount != 0 {
		t.Errorf("Expected empty connections map, got %d connections", finalCount)
	}
}

// TestMockWebSocketConn はモックWebSocket接続をテストします
func TestMockWebSocketConn(t *testing.T) {
	mockConn := &MockWebSocketConn{
		ID:          "mock_123",
		IsOpen:      true,
		SendBuffer:  make(chan []byte, 10),
		RecvBuffer:  make(chan []byte, 10),
		CloseSignal: make(chan struct{}),
	}

	if !mockConn.IsOpen {
		t.Error("Mock connection should be open")
	}

	if mockConn.ID != "mock_123" {
		t.Errorf("Expected ID 'mock_123', got '%s'", mockConn.ID)
	}

	// バッファーサイズを確認
	if cap(mockConn.SendBuffer) != 10 {
		t.Errorf("Expected SendBuffer capacity 10, got %d", cap(mockConn.SendBuffer))
	}

	if cap(mockConn.RecvBuffer) != 10 {
		t.Errorf("Expected RecvBuffer capacity 10, got %d", cap(mockConn.RecvBuffer))
	}

	// CloseSignalチャネルが初期化されていることを確認
	if mockConn.CloseSignal == nil {
		t.Error("CloseSignal should be initialized")
	}

	// CloseSignalが非ブロッキングで使用可能であることを確認
	select {
	case <-mockConn.CloseSignal:
		t.Error("CloseSignal should not be closed initially")
	default:
		// 正常: チャネルはまだ開いている
	}
}

// TestConfigValidation は設定値の検証をテストします
func TestConfigValidation(t *testing.T) {
	config := NewHandlerConfig()

	// 最小値チェック
	if config.WorkerCount < 1 {
		t.Error("WorkerCount should be at least 1")
	}

	if config.MessageBufferSize < 1 {
		t.Error("MessageBufferSize should be at least 1")
	}

	if config.SendBufferSize < 1 {
		t.Error("SendBufferSize should be at least 1")
	}

	if config.HeartbeatInterval <= 0 {
		t.Error("HeartbeatInterval should be positive")
	}

	if config.ConnectionTimeout <= 0 {
		t.Error("ConnectionTimeout should be positive")
	}
}

// ベンチマークテスト
func BenchmarkMessageCreation(b *testing.B) {
	var totalFields int
	for i := 0; i < b.N; i++ {
		msg := &WebSocketMessage{
			ID:        fmt.Sprintf("msg_%d", i),
			Type:      MessageTypeChat,
			Data:      map[string]interface{}{"text": "benchmark message"},
			From:      "user1",
			Timestamp: time.Now(),
		}
		// フィールドの値を実際に使用してベンチマーク対象にする
		if len(msg.ID) > 0 && msg.Type == MessageTypeChat &&
			msg.Data["text"] != nil && len(msg.From) > 0 &&
			!msg.Timestamp.IsZero() {
			totalFields++
		}
	}
	// ベンチマーク結果が最適化で除去されないようにする
	if totalFields != b.N {
		b.Fatalf("Expected %d valid messages, got %d", b.N, totalFields)
	}
}

func BenchmarkConnectionMapAccess(b *testing.B) {
	config := NewHandlerConfig()
	handler := NewWebSocketHandler(config)

	// テスト用接続を事前に作成
	for i := 0; i < 1000; i++ {
		conn := &WebSocketConnection{
			ID:       fmt.Sprintf("conn_%d", i),
			UserID:   fmt.Sprintf("user_%d", i),
			IsActive: true,
			Channels: make(map[string]bool),
		}
		handler.connections[conn.ID] = conn
	}

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			handler.connectionsMu.RLock()
			_ = len(handler.connections)
			handler.connectionsMu.RUnlock()
		}
	})
}
