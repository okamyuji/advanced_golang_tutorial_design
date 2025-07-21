package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// WebSocketMessage WebSocketメッセージです
type WebSocketMessage struct {
	ID        string                 `json:"id"`
	Type      MessageType            `json:"type"`
	Data      map[string]interface{} `json:"data"`
	From      string                 `json:"from"`
	To        string                 `json:"to,omitempty"`
	Channel   string                 `json:"channel,omitempty"`
	Timestamp time.Time              `json:"timestamp"`
}

// MessageType メッセージタイプです
type MessageType string

const (
	MessageTypeJoin      MessageType = "join"
	MessageTypeLeave     MessageType = "leave"
	MessageTypeChat      MessageType = "chat"
	MessageTypeBroadcast MessageType = "broadcast"
	MessageTypePrivate   MessageType = "private"
	MessageTypeHeartbeat MessageType = "heartbeat"
	MessageTypeError     MessageType = "error"
	MessageTypeSystem    MessageType = "system"
)

// WebSocketConnection WebSocket接続です
type WebSocketConnection struct {
	ID            string
	UserID        string
	Channels      map[string]bool
	Connection    *MockWebSocketConn
	SendQueue     chan *WebSocketMessage
	IsActive      bool
	LastHeartbeat time.Time
	ConnectedAt   time.Time
	RemoteAddr    string
	UserAgent     string
	mu            sync.RWMutex
}

// MockWebSocketConn WebSocket接続のモックです
type MockWebSocketConn struct {
	ID          string
	IsOpen      bool
	SendBuffer  chan []byte
	RecvBuffer  chan []byte
	CloseSignal chan struct{}
	mu          sync.Mutex
}

// WebSocketHandler WebSocket並行処理ハンドラーです
type WebSocketHandler struct {
	// 接続管理
	connections   map[string]*WebSocketConnection
	connectionsMu sync.RWMutex

	// チャネル管理
	channels   map[string]*Channel
	channelsMu sync.RWMutex

	// メッセージ処理
	messageQueue   chan *WebSocketMessage
	broadcastQueue chan *WebSocketMessage

	// ワーカー管理
	workers     []*MessageWorker
	workerCount int

	// コンテキスト制御
	ctx    context.Context
	cancel context.CancelFunc

	// 同期制御
	wg sync.WaitGroup

	// 統計・監視
	stats *HandlerStats

	// 設定
	config *HandlerConfig

	// 制御フラグ
	isRunning int32
	startTime time.Time
}

// Channel チャネル情報です
type Channel struct {
	Name           string
	Description    string
	Members        map[string]*WebSocketConnection
	MessageHistory []*WebSocketMessage
	CreatedAt      time.Time
	mu             sync.RWMutex
}

// MessageWorker メッセージ処理ワーカーです
type MessageWorker struct {
	ID      int
	handler *WebSocketHandler
	stats   *WorkerStats
}

// HandlerStats ハンドラー統計です
type HandlerStats struct {
	mu                sync.RWMutex
	totalConnections  int64
	activeConnections int64
	totalMessages     int64
	broadcastMessages int64
	privateMessages   int64
	errorMessages     int64
	channelCount      int64
	startTime         time.Time
}

// WorkerStats ワーカー統計です
type WorkerStats struct {
	WorkerID              int
	ProcessedMessages     int64
	SuccessfulMessages    int64
	ErrorMessages         int64
	TotalProcessingTime   time.Duration
	AverageProcessingTime time.Duration
}

// HandlerConfig ハンドラー設定です
type HandlerConfig struct {
	WorkerCount       int
	MessageBufferSize int
	SendBufferSize    int
	HeartbeatInterval time.Duration
	ConnectionTimeout time.Duration
	MaxChannelHistory int
	EnableMetrics     bool
	MetricsInterval   time.Duration
}

// NewHandlerConfig デフォルト設定を作成します
func NewHandlerConfig() *HandlerConfig {
	return &HandlerConfig{
		WorkerCount:       4,
		MessageBufferSize: 10000,
		SendBufferSize:    1000,
		HeartbeatInterval: 30 * time.Second,
		ConnectionTimeout: 60 * time.Second,
		MaxChannelHistory: 100,
		EnableMetrics:     true,
		MetricsInterval:   10 * time.Second,
	}
}

// NewWebSocketHandler 新しいWebSocketハンドラーを作成します
func NewWebSocketHandler(config *HandlerConfig) *WebSocketHandler {
	ctx, cancel := context.WithCancel(context.Background())

	handler := &WebSocketHandler{
		connections:    make(map[string]*WebSocketConnection),
		channels:       make(map[string]*Channel),
		messageQueue:   make(chan *WebSocketMessage, config.MessageBufferSize),
		broadcastQueue: make(chan *WebSocketMessage, config.MessageBufferSize),
		workers:        make([]*MessageWorker, config.WorkerCount),
		workerCount:    config.WorkerCount,
		ctx:            ctx,
		cancel:         cancel,
		config:         config,
		stats: &HandlerStats{
			startTime: time.Now(),
		},
	}

	// ワーカーを初期化
	for i := 0; i < config.WorkerCount; i++ {
		worker := &MessageWorker{
			ID:      i,
			handler: handler,
			stats: &WorkerStats{
				WorkerID: i,
			},
		}
		handler.workers[i] = worker
	}

	// デフォルトチャネルを作成
	handler.createChannel("general", "General discussion channel")

	return handler
}

// Start ハンドラーを開始します
func (wh *WebSocketHandler) Start() error {
	if !atomic.CompareAndSwapInt32(&wh.isRunning, 0, 1) {
		return fmt.Errorf("handler is already running")
	}

	wh.startTime = time.Now()
	wh.stats.startTime = wh.startTime

	log.Printf("Starting WebSocket handler with %d workers", wh.workerCount)

	// メッセージ処理ワーカーを開始
	for _, worker := range wh.workers {
		wh.wg.Add(1)
		go worker.run()
	}

	// ブロードキャスト処理を開始
	wh.wg.Add(1)
	go wh.handleBroadcasts()

	// ハートビート処理を開始
	wh.wg.Add(1)
	go wh.heartbeatLoop()

	// 接続監視を開始
	wh.wg.Add(1)
	go wh.monitorConnections()

	// メトリクス監視を開始
	if wh.config.EnableMetrics {
		wh.wg.Add(1)
		go wh.monitorMetrics()
	}

	log.Printf("WebSocket handler started successfully")
	return nil
}

// HandleConnection 新しいWebSocket接続を処理します
func (wh *WebSocketHandler) HandleConnection(userID, remoteAddr, userAgent string) (*WebSocketConnection, error) {
	if atomic.LoadInt32(&wh.isRunning) == 0 {
		return nil, fmt.Errorf("handler is not running")
	}

	// 接続を作成
	connID := wh.generateConnectionID()
	mockConn := &MockWebSocketConn{
		ID:          connID,
		IsOpen:      true,
		SendBuffer:  make(chan []byte, wh.config.SendBufferSize),
		RecvBuffer:  make(chan []byte, wh.config.SendBufferSize),
		CloseSignal: make(chan struct{}),
	}

	conn := &WebSocketConnection{
		ID:            connID,
		UserID:        userID,
		Channels:      make(map[string]bool),
		Connection:    mockConn,
		SendQueue:     make(chan *WebSocketMessage, wh.config.SendBufferSize),
		IsActive:      true,
		LastHeartbeat: time.Now(),
		ConnectedAt:   time.Now(),
		RemoteAddr:    remoteAddr,
		UserAgent:     userAgent,
	}

	// 接続を登録
	wh.connectionsMu.Lock()
	wh.connections[connID] = conn
	wh.connectionsMu.Unlock()

	// 統計更新
	atomic.AddInt64(&wh.stats.totalConnections, 1)
	atomic.AddInt64(&wh.stats.activeConnections, 1)

	// 接続処理を開始
	wh.wg.Add(2)
	go wh.handleConnectionSend(conn)
	go wh.handleConnectionReceive(conn)

	// 接続通知メッセージを送信
	systemMsg := &WebSocketMessage{
		ID:        wh.generateMessageID(),
		Type:      MessageTypeSystem,
		Data:      map[string]interface{}{"message": "Connected successfully"},
		To:        connID,
		Timestamp: time.Now(),
	}

	select {
	case wh.messageQueue <- systemMsg:
	case <-wh.ctx.Done():
		return nil, fmt.Errorf("handler is shutting down")
	default:
		log.Printf("Message queue is full, dropping system message")
	}

	log.Printf("New WebSocket connection: %s (user=%s, addr=%s)", connID, userID, remoteAddr)
	return conn, nil
}

// handleConnectionSend 接続の送信処理を行います
func (wh *WebSocketHandler) handleConnectionSend(conn *WebSocketConnection) {
	defer wh.wg.Done()

	for {
		select {
		case <-wh.ctx.Done():
			return
		case <-conn.Connection.CloseSignal:
			return
		case message, ok := <-conn.SendQueue:
			if !ok {
				return
			}

			// メッセージをJSON化
			data, err := json.Marshal(message)
			if err != nil {
				log.Printf("Failed to marshal message: %v", err)
				continue
			}

			// モック接続に送信
			select {
			case conn.Connection.SendBuffer <- data:
				// 正常に送信
			case <-time.After(5 * time.Second):
				log.Printf("Send timeout for connection %s", conn.ID)
			case <-wh.ctx.Done():
				return
			}
		}
	}
}

// handleConnectionReceive 接続の受信処理を行います
func (wh *WebSocketHandler) handleConnectionReceive(conn *WebSocketConnection) {
	defer wh.wg.Done()
	defer wh.closeConnection(conn)

	for {
		select {
		case <-wh.ctx.Done():
			return
		case <-conn.Connection.CloseSignal:
			return
		case data, ok := <-conn.Connection.RecvBuffer:
			if !ok {
				return
			}

			// メッセージを解析
			var message WebSocketMessage
			if err := json.Unmarshal(data, &message); err != nil {
				log.Printf("Failed to unmarshal message from %s: %v", conn.ID, err)
				continue
			}

			// メッセージの送信者を設定
			message.From = conn.ID
			message.Timestamp = time.Now()

			// メッセージをキューに追加
			select {
			case wh.messageQueue <- &message:
				atomic.AddInt64(&wh.stats.totalMessages, 1)
			case <-wh.ctx.Done():
				return
			default:
				log.Printf("Message queue is full, dropping message from %s", conn.ID)
			}
		}
	}
}

// closeConnection 接続を閉じます
func (wh *WebSocketHandler) closeConnection(conn *WebSocketConnection) {
	conn.mu.Lock()
	defer conn.mu.Unlock()

	if !conn.IsActive {
		return
	}

	conn.IsActive = false

	// 接続を削除
	wh.connectionsMu.Lock()
	delete(wh.connections, conn.ID)
	wh.connectionsMu.Unlock()

	// チャネルから離脱
	for channelName := range conn.Channels {
		if err := wh.leaveChannel(conn.ID, channelName); err != nil {
			log.Printf("Failed to leave channel %s: %v", channelName, err)
		}
	}

	// 送信キューを閉じる
	close(conn.SendQueue)

	// モック接続を閉じる
	conn.Connection.mu.Lock()
	if conn.Connection.IsOpen {
		conn.Connection.IsOpen = false
		close(conn.Connection.CloseSignal)
	}
	conn.Connection.mu.Unlock()

	// 統計更新
	atomic.AddInt64(&wh.stats.activeConnections, -1)

	log.Printf("WebSocket connection closed: %s (user=%s)", conn.ID, conn.UserID)
}

// run ワーカーのメインループです
func (mw *MessageWorker) run() {
	defer mw.handler.wg.Done()

	log.Printf("Message worker %d started", mw.ID)

	for {
		select {
		case <-mw.handler.ctx.Done():
			log.Printf("Worker %d stopping due to context cancellation", mw.ID)
			return
		case message, ok := <-mw.handler.messageQueue:
			if !ok {
				log.Printf("Worker %d stopping due to message queue closure", mw.ID)
				return
			}

			// メッセージを処理
			mw.processMessage(message)
		}
	}
}

// processMessage メッセージを処理します
func (mw *MessageWorker) processMessage(message *WebSocketMessage) {
	start := time.Now()

	atomic.AddInt64(&mw.stats.ProcessedMessages, 1)

	switch message.Type {
	case MessageTypeJoin:
		mw.handleJoinMessage(message)
	case MessageTypeLeave:
		mw.handleLeaveMessage(message)
	case MessageTypeChat:
		mw.handleChatMessage(message)
	case MessageTypeBroadcast:
		mw.handleBroadcastMessage(message)
	case MessageTypePrivate:
		mw.handlePrivateMessage(message)
	case MessageTypeHeartbeat:
		mw.handleHeartbeatMessage(message)
	default:
		mw.handleUnknownMessage(message)
	}

	// 統計更新
	processingTime := time.Since(start)
	mw.stats.TotalProcessingTime += processingTime

	if mw.stats.ProcessedMessages > 0 {
		mw.stats.AverageProcessingTime = mw.stats.TotalProcessingTime / time.Duration(mw.stats.ProcessedMessages)
	}
}

// handleJoinMessage チャネル参加メッセージを処理します
func (mw *MessageWorker) handleJoinMessage(message *WebSocketMessage) {
	channelName, ok := message.Data["channel"].(string)
	if !ok {
		mw.sendErrorMessage(message.From, "Invalid channel name")
		return
	}

	if err := mw.handler.joinChannel(message.From, channelName); err != nil {
		mw.sendErrorMessage(message.From, fmt.Sprintf("Failed to join channel: %v", err))
		return
	}

	// 成功メッセージを送信
	response := &WebSocketMessage{
		ID:   mw.handler.generateMessageID(),
		Type: MessageTypeSystem,
		Data: map[string]interface{}{
			"message": fmt.Sprintf("Joined channel: %s", channelName),
			"channel": channelName,
		},
		To:        message.From,
		Timestamp: time.Now(),
	}

	mw.sendMessage(response)
	atomic.AddInt64(&mw.stats.SuccessfulMessages, 1)
}

// handleLeaveMessage チャネル離脱メッセージを処理します
func (mw *MessageWorker) handleLeaveMessage(message *WebSocketMessage) {
	channelName, ok := message.Data["channel"].(string)
	if !ok {
		mw.sendErrorMessage(message.From, "Invalid channel name")
		return
	}

	if err := mw.handler.leaveChannel(message.From, channelName); err != nil {
		mw.sendErrorMessage(message.From, fmt.Sprintf("Failed to leave channel: %v", err))
		return
	}

	// 成功メッセージを送信
	response := &WebSocketMessage{
		ID:   mw.handler.generateMessageID(),
		Type: MessageTypeSystem,
		Data: map[string]interface{}{
			"message": fmt.Sprintf("Left channel: %s", channelName),
			"channel": channelName,
		},
		To:        message.From,
		Timestamp: time.Now(),
	}

	mw.sendMessage(response)
	atomic.AddInt64(&mw.stats.SuccessfulMessages, 1)
}

// handleChatMessage チャットメッセージを処理します
func (mw *MessageWorker) handleChatMessage(message *WebSocketMessage) {
	channelName, ok := message.Data["channel"].(string)
	if !ok {
		mw.sendErrorMessage(message.From, "Invalid channel name")
		return
	}

	// チャネルのメンバーにメッセージを送信
	if err := mw.handler.sendToChannel(channelName, message); err != nil {
		mw.sendErrorMessage(message.From, fmt.Sprintf("Failed to send message: %v", err))
		return
	}

	// チャネル履歴に追加
	mw.handler.addToChannelHistory(channelName, message)

	atomic.AddInt64(&mw.stats.SuccessfulMessages, 1)
}

// handleBroadcastMessage ブロードキャストメッセージを処理します
func (mw *MessageWorker) handleBroadcastMessage(message *WebSocketMessage) {
	// ブロードキャストキューに追加
	select {
	case mw.handler.broadcastQueue <- message:
		atomic.AddInt64(&mw.handler.stats.broadcastMessages, 1)
		atomic.AddInt64(&mw.stats.SuccessfulMessages, 1)
	case <-mw.handler.ctx.Done():
		return
	default:
		mw.sendErrorMessage(message.From, "Broadcast queue is full")
		atomic.AddInt64(&mw.stats.ErrorMessages, 1)
	}
}

// handlePrivateMessage プライベートメッセージを処理します
func (mw *MessageWorker) handlePrivateMessage(message *WebSocketMessage) {
	if message.To == "" {
		mw.sendErrorMessage(message.From, "Target connection ID required for private message")
		return
	}

	// 対象接続に送信
	if err := mw.handler.sendToConnection(message.To, message); err != nil {
		mw.sendErrorMessage(message.From, fmt.Sprintf("Failed to send private message: %v", err))
		return
	}

	atomic.AddInt64(&mw.handler.stats.privateMessages, 1)
	atomic.AddInt64(&mw.stats.SuccessfulMessages, 1)
}

// handleHeartbeatMessage ハートビートメッセージを処理します
func (mw *MessageWorker) handleHeartbeatMessage(message *WebSocketMessage) {
	// 接続のハートビート時刻を更新
	mw.handler.connectionsMu.RLock()
	conn, exists := mw.handler.connections[message.From]
	mw.handler.connectionsMu.RUnlock()

	if exists {
		conn.mu.Lock()
		conn.LastHeartbeat = time.Now()
		conn.mu.Unlock()

		// ハートビート応答を送信
		response := &WebSocketMessage{
			ID:        mw.handler.generateMessageID(),
			Type:      MessageTypeHeartbeat,
			Data:      map[string]interface{}{"pong": time.Now().Unix()},
			To:        message.From,
			Timestamp: time.Now(),
		}

		mw.sendMessage(response)
	}

	atomic.AddInt64(&mw.stats.SuccessfulMessages, 1)
}

// handleUnknownMessage 不明なメッセージを処理します
func (mw *MessageWorker) handleUnknownMessage(message *WebSocketMessage) {
	mw.sendErrorMessage(message.From, fmt.Sprintf("Unknown message type: %s", message.Type))
	atomic.AddInt64(&mw.stats.ErrorMessages, 1)
}

// sendMessage メッセージを送信します
func (mw *MessageWorker) sendMessage(message *WebSocketMessage) {
	select {
	case mw.handler.messageQueue <- message:
	case <-mw.handler.ctx.Done():
	default:
		log.Printf("Failed to send message: queue is full")
	}
}

// sendErrorMessage エラーメッセージを送信します
func (mw *MessageWorker) sendErrorMessage(connectionID, errorMsg string) {
	errorMessage := &WebSocketMessage{
		ID:   mw.handler.generateMessageID(),
		Type: MessageTypeError,
		Data: map[string]interface{}{
			"error": errorMsg,
		},
		To:        connectionID,
		Timestamp: time.Now(),
	}

	if err := mw.handler.sendToConnection(connectionID, errorMessage); err != nil {
		log.Printf("Failed to send error message to connection %s: %v", connectionID, err)
	}
	atomic.AddInt64(&mw.handler.stats.errorMessages, 1)
}

// joinChannel チャネルに参加します
func (wh *WebSocketHandler) joinChannel(connectionID, channelName string) error {
	// 接続を取得
	wh.connectionsMu.RLock()
	conn, exists := wh.connections[connectionID]
	wh.connectionsMu.RUnlock()

	if !exists {
		return fmt.Errorf("connection not found: %s", connectionID)
	}

	// チャネルを取得または作成
	wh.channelsMu.Lock()
	channel, exists := wh.channels[channelName]
	if !exists {
		channel = wh.createChannel(channelName, fmt.Sprintf("Channel: %s", channelName))
	}
	wh.channelsMu.Unlock()

	// チャネルにメンバーを追加
	channel.mu.Lock()
	channel.Members[connectionID] = conn
	channel.mu.Unlock()

	// 接続にチャネルを追加
	conn.mu.Lock()
	conn.Channels[channelName] = true
	conn.mu.Unlock()

	log.Printf("Connection %s joined channel %s", connectionID, channelName)
	return nil
}

// leaveChannel チャネルから離脱します
func (wh *WebSocketHandler) leaveChannel(connectionID, channelName string) error {
	// 接続を取得
	wh.connectionsMu.RLock()
	conn, exists := wh.connections[connectionID]
	wh.connectionsMu.RUnlock()

	if !exists {
		return fmt.Errorf("connection not found: %s", connectionID)
	}

	// チャネルを取得
	wh.channelsMu.RLock()
	channel, exists := wh.channels[channelName]
	wh.channelsMu.RUnlock()

	if !exists {
		return fmt.Errorf("channel not found: %s", channelName)
	}

	// チャネルからメンバーを削除
	channel.mu.Lock()
	delete(channel.Members, connectionID)
	channel.mu.Unlock()

	// 接続からチャネルを削除
	conn.mu.Lock()
	delete(conn.Channels, channelName)
	conn.mu.Unlock()

	log.Printf("Connection %s left channel %s", connectionID, channelName)
	return nil
}

// createChannel チャネルを作成します
func (wh *WebSocketHandler) createChannel(name, description string) *Channel {
	channel := &Channel{
		Name:           name,
		Description:    description,
		Members:        make(map[string]*WebSocketConnection),
		MessageHistory: make([]*WebSocketMessage, 0),
		CreatedAt:      time.Now(),
	}

	wh.channels[name] = channel
	atomic.AddInt64(&wh.stats.channelCount, 1)

	log.Printf("Channel created: %s", name)
	return channel
}

// sendToChannel チャネルのメンバーにメッセージを送信します
func (wh *WebSocketHandler) sendToChannel(channelName string, message *WebSocketMessage) error {
	wh.channelsMu.RLock()
	channel, exists := wh.channels[channelName]
	wh.channelsMu.RUnlock()

	if !exists {
		return fmt.Errorf("channel not found: %s", channelName)
	}

	channel.mu.RLock()
	defer channel.mu.RUnlock()

	// チャネルのすべてのメンバーに送信
	for memberID, conn := range channel.Members {
		if memberID != message.From { // 送信者以外に送信
			select {
			case conn.SendQueue <- message:
			case <-time.After(1 * time.Second):
				log.Printf("Send timeout for connection %s in channel %s", memberID, channelName)
			case <-wh.ctx.Done():
				return wh.ctx.Err()
			}
		}
	}

	return nil
}

// sendToConnection 特定の接続にメッセージを送信します
func (wh *WebSocketHandler) sendToConnection(connectionID string, message *WebSocketMessage) error {
	wh.connectionsMu.RLock()
	conn, exists := wh.connections[connectionID]
	wh.connectionsMu.RUnlock()

	if !exists {
		return fmt.Errorf("connection not found: %s", connectionID)
	}

	select {
	case conn.SendQueue <- message:
		return nil
	case <-time.After(1 * time.Second):
		return fmt.Errorf("send timeout for connection %s", connectionID)
	case <-wh.ctx.Done():
		return wh.ctx.Err()
	}
}

// addToChannelHistory チャネル履歴にメッセージを追加します
func (wh *WebSocketHandler) addToChannelHistory(channelName string, message *WebSocketMessage) {
	wh.channelsMu.RLock()
	channel, exists := wh.channels[channelName]
	wh.channelsMu.RUnlock()

	if !exists {
		return
	}

	channel.mu.Lock()
	defer channel.mu.Unlock()

	// 履歴に追加
	channel.MessageHistory = append(channel.MessageHistory, message)

	// 履歴サイズを制限
	if len(channel.MessageHistory) > wh.config.MaxChannelHistory {
		channel.MessageHistory = channel.MessageHistory[1:]
	}
}

// handleBroadcasts ブロードキャストを処理します
func (wh *WebSocketHandler) handleBroadcasts() {
	defer wh.wg.Done()

	log.Println("Broadcast handler started")

	for {
		select {
		case <-wh.ctx.Done():
			log.Println("Broadcast handler stopping due to context cancellation")
			return
		case message, ok := <-wh.broadcastQueue:
			if !ok {
				log.Println("Broadcast handler stopping due to broadcast queue closure")
				return
			}

			// すべての接続にブロードキャスト
			wh.connectionsMu.RLock()
			for connID, conn := range wh.connections {
				if connID != message.From { // 送信者以外に送信
					select {
					case conn.SendQueue <- message:
					case <-time.After(100 * time.Millisecond):
						log.Printf("Broadcast timeout for connection %s", connID)
					case <-wh.ctx.Done():
						wh.connectionsMu.RUnlock()
						return
					}
				}
			}
			wh.connectionsMu.RUnlock()
		}
	}
}

// heartbeatLoop ハートビートループを実行します
func (wh *WebSocketHandler) heartbeatLoop() {
	defer wh.wg.Done()

	ticker := time.NewTicker(wh.config.HeartbeatInterval)
	defer ticker.Stop()

	log.Println("Heartbeat loop started")

	for {
		select {
		case <-wh.ctx.Done():
			log.Println("Heartbeat loop stopping")
			return
		case <-ticker.C:
			wh.checkHeartbeats()
		}
	}
}

// checkHeartbeats ハートビートをチェックします
func (wh *WebSocketHandler) checkHeartbeats() {
	now := time.Now()
	timeout := wh.config.ConnectionTimeout

	var toClose []string

	wh.connectionsMu.RLock()
	for connID, conn := range wh.connections {
		conn.mu.RLock()
		if now.Sub(conn.LastHeartbeat) > timeout {
			toClose = append(toClose, connID)
		}
		conn.mu.RUnlock()
	}
	wh.connectionsMu.RUnlock()

	// タイムアウトした接続を閉じる
	for _, connID := range toClose {
		if conn, exists := wh.connections[connID]; exists {
			log.Printf("Connection %s timed out, closing", connID)
			wh.closeConnection(conn)
		}
	}
}

// monitorConnections 接続を監視します
func (wh *WebSocketHandler) monitorConnections() {
	defer wh.wg.Done()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	log.Println("Connection monitor started")

	for {
		select {
		case <-wh.ctx.Done():
			log.Println("Connection monitor stopping")
			return
		case <-ticker.C:
			wh.reportConnectionStatus()
		}
	}
}

// reportConnectionStatus 接続状況を報告します
func (wh *WebSocketHandler) reportConnectionStatus() {
	activeConnections := atomic.LoadInt64(&wh.stats.activeConnections)
	totalConnections := atomic.LoadInt64(&wh.stats.totalConnections)

	wh.channelsMu.RLock()
	channelCount := len(wh.channels)
	wh.channelsMu.RUnlock()

	log.Printf("Connection Status: Active=%d, Total=%d, Channels=%d",
		activeConnections, totalConnections, channelCount)
}

// monitorMetrics メトリクスを監視します
func (wh *WebSocketHandler) monitorMetrics() {
	defer wh.wg.Done()

	ticker := time.NewTicker(wh.config.MetricsInterval)
	defer ticker.Stop()

	log.Println("Metrics monitor started")

	for {
		select {
		case <-wh.ctx.Done():
			log.Println("Metrics monitor stopping")
			return
		case <-ticker.C:
			wh.reportMetrics()
		}
	}
}

// reportMetrics メトリクスを報告します
func (wh *WebSocketHandler) reportMetrics() {
	wh.stats.mu.RLock()
	defer wh.stats.mu.RUnlock()

	totalMessages := atomic.LoadInt64(&wh.stats.totalMessages)
	broadcastMessages := atomic.LoadInt64(&wh.stats.broadcastMessages)
	privateMessages := atomic.LoadInt64(&wh.stats.privateMessages)
	errorMessages := atomic.LoadInt64(&wh.stats.errorMessages)
	activeConnections := atomic.LoadInt64(&wh.stats.activeConnections)

	uptime := time.Since(wh.stats.startTime)
	var mps float64
	if uptime.Seconds() > 0 {
		mps = float64(totalMessages) / uptime.Seconds()
	}

	log.Printf("Handler Metrics: Messages=%d (Broadcast=%d, Private=%d, Error=%d), MPS=%.2f, Connections=%d",
		totalMessages, broadcastMessages, privateMessages, errorMessages, mps, activeConnections)

	// ワーカー別統計
	for _, worker := range wh.workers {
		processed := atomic.LoadInt64(&worker.stats.ProcessedMessages)
		successful := atomic.LoadInt64(&worker.stats.SuccessfulMessages)
		errors := atomic.LoadInt64(&worker.stats.ErrorMessages)

		if processed > 0 {
			successRate := float64(successful) / float64(processed) * 100
			log.Printf("Worker %d: Processed=%d, Success=%.1f%%, Errors=%d, AvgTime=%v",
				worker.ID, processed, successRate, errors, worker.stats.AverageProcessingTime)
		}
	}
}

// SendMessage 外部からメッセージを送信します
func (wh *WebSocketHandler) SendMessage(message *WebSocketMessage) error {
	if atomic.LoadInt32(&wh.isRunning) == 0 {
		return fmt.Errorf("handler is not running")
	}

	message.ID = wh.generateMessageID()
	message.Timestamp = time.Now()

	select {
	case wh.messageQueue <- message:
		return nil
	case <-wh.ctx.Done():
		return fmt.Errorf("handler is shutting down")
	default:
		return fmt.Errorf("message queue is full")
	}
}

// ユーティリティメソッド

// generateConnectionID 接続IDを生成します
func (wh *WebSocketHandler) generateConnectionID() string {
	return fmt.Sprintf("conn_%d_%d", time.Now().UnixNano(), rand.Int63())
}

// generateMessageID メッセージIDを生成します
func (wh *WebSocketHandler) generateMessageID() string {
	return fmt.Sprintf("msg_%d_%d", time.Now().UnixNano(), rand.Int63())
}

// Shutdown ハンドラーを停止します
func (wh *WebSocketHandler) Shutdown(timeout time.Duration) error {
	if !atomic.CompareAndSwapInt32(&wh.isRunning, 1, 0) {
		return fmt.Errorf("handler is not running")
	}

	log.Println("Shutting down WebSocket handler...")

	// 1. 新しいメッセージの受付を停止
	close(wh.messageQueue)
	close(wh.broadcastQueue)

	// 2. すべての接続を閉じる
	wh.connectionsMu.Lock()
	for _, conn := range wh.connections {
		wh.closeConnection(conn)
	}
	wh.connectionsMu.Unlock()

	// 3. ワーカーの終了を待機
	done := make(chan struct{})
	go func() {
		wh.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("All workers stopped gracefully")
	case <-time.After(timeout):
		log.Println("Timeout reached, forcing shutdown...")
		wh.cancel()

		// 追加の待機時間
		select {
		case <-done:
			log.Println("Workers stopped after cancellation")
		case <-time.After(2 * time.Second):
			log.Println("Some workers may not have stopped properly")
		}
	}

	log.Println("WebSocket handler shutdown completed")
	return nil
}

// GetStats 統計情報を取得します
func (wh *WebSocketHandler) GetStats() map[string]interface{} {
	wh.stats.mu.RLock()
	defer wh.stats.mu.RUnlock()

	totalConnections := atomic.LoadInt64(&wh.stats.totalConnections)
	activeConnections := atomic.LoadInt64(&wh.stats.activeConnections)
	totalMessages := atomic.LoadInt64(&wh.stats.totalMessages)
	broadcastMessages := atomic.LoadInt64(&wh.stats.broadcastMessages)
	privateMessages := atomic.LoadInt64(&wh.stats.privateMessages)
	errorMessages := atomic.LoadInt64(&wh.stats.errorMessages)

	uptime := time.Since(wh.stats.startTime)
	var mps float64
	if uptime.Seconds() > 0 {
		mps = float64(totalMessages) / uptime.Seconds()
	}

	wh.channelsMu.RLock()
	channelCount := len(wh.channels)
	wh.channelsMu.RUnlock()

	workerStats := make(map[string]interface{})
	for i, worker := range wh.workers {
		workerStats[fmt.Sprintf("worker_%d", i)] = map[string]interface{}{
			"processed_messages":      atomic.LoadInt64(&worker.stats.ProcessedMessages),
			"successful_messages":     atomic.LoadInt64(&worker.stats.SuccessfulMessages),
			"error_messages":          atomic.LoadInt64(&worker.stats.ErrorMessages),
			"average_processing_time": worker.stats.AverageProcessingTime,
		}
	}

	return map[string]interface{}{
		"total_connections":   totalConnections,
		"active_connections":  activeConnections,
		"total_messages":      totalMessages,
		"broadcast_messages":  broadcastMessages,
		"private_messages":    privateMessages,
		"error_messages":      errorMessages,
		"messages_per_second": mps,
		"uptime":              uptime,
		"channel_count":       channelCount,
		"worker_count":        len(wh.workers),
		"worker_stats":        workerStats,
	}
}

func main() {
	// ハンドラー設定
	config := NewHandlerConfig()
	config.WorkerCount = 6
	config.MessageBufferSize = 5000

	// ハンドラー作成・開始
	handler := NewWebSocketHandler(config)
	if err := handler.Start(); err != nil {
		log.Fatalf("Failed to start handler: %v", err)
	}

	// テスト用接続をシミュレート
	connections := make([]*WebSocketConnection, 0)

	// 複数のテスト接続を作成
	for i := 0; i < 20; i++ {
		userID := fmt.Sprintf("user_%d", i)
		remoteAddr := fmt.Sprintf("192.168.1.%d:8080", 100+i)
		userAgent := "TestClient/1.0"

		conn, err := handler.HandleConnection(userID, remoteAddr, userAgent)
		if err != nil {
			log.Printf("Failed to create connection %d: %v", i, err)
			continue
		}

		connections = append(connections, conn)

		// チャネルに参加
		channelName := fmt.Sprintf("channel_%d", i%3)
		joinMsg := &WebSocketMessage{
			Type: MessageTypeJoin,
			Data: map[string]interface{}{"channel": channelName},
		}

		// モック受信データとして送信
		data, _ := json.Marshal(joinMsg)
		select {
		case conn.Connection.RecvBuffer <- data:
		default:
		}
	}

	// テストメッセージを送信
	go func() {
		for i := 0; i < 500; i++ {
			if len(connections) == 0 {
				break
			}

			conn := connections[rand.Intn(len(connections))]

			var message *WebSocketMessage

			switch rand.Intn(4) {
			case 0: // チャットメッセージ
				message = &WebSocketMessage{
					Type: MessageTypeChat,
					Data: map[string]interface{}{
						"channel": fmt.Sprintf("channel_%d", rand.Intn(3)),
						"text":    fmt.Sprintf("Test message %d", i),
					},
				}
			case 1: // ブロードキャストメッセージ
				message = &WebSocketMessage{
					Type: MessageTypeBroadcast,
					Data: map[string]interface{}{
						"text": fmt.Sprintf("Broadcast message %d", i),
					},
				}
			case 2: // プライベートメッセージ
				if len(connections) > 1 {
					targetConn := connections[rand.Intn(len(connections))]
					message = &WebSocketMessage{
						Type: MessageTypePrivate,
						To:   targetConn.ID,
						Data: map[string]interface{}{
							"text": fmt.Sprintf("Private message %d", i),
						},
					}
				}
			case 3: // ハートビート
				message = &WebSocketMessage{
					Type: MessageTypeHeartbeat,
					Data: map[string]interface{}{"ping": time.Now().Unix()},
				}
			}

			if message != nil {
				data, _ := json.Marshal(message)
				select {
				case conn.Connection.RecvBuffer <- data:
				default:
				}
			}

			time.Sleep(time.Duration(rand.Intn(200)+50) * time.Millisecond)
		}

		log.Println("Test messages sending completed")
	}()

	// 30秒間実行
	time.Sleep(30 * time.Second)

	// 最終統計表示
	stats := handler.GetStats()
	log.Printf("Final Stats: %+v", stats)

	// グレースフルシャットダウン
	if err := handler.Shutdown(10 * time.Second); err != nil {
		log.Printf("Shutdown error: %v", err)
	}
}
