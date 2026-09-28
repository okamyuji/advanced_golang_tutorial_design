package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// MassiveConnectionServer 大量の同時接続を受けるServer-Sent Eventsサーバー（上限はServerConfig.MaxConnections）
type MassiveConnectionServer struct {
	clients       sync.Map // map[string]*SSEClient - goroutine safe
	channels      sync.Map // map[string]*Channel - goroutine safe
	metrics       *ServerMetrics
	config        *ServerConfig
	hub           *ConnectionHub
	shutdown      chan struct{}
	messageRouter *MessageRouter
	mux           *http.ServeMux
}

// ServerConfig サーバー設定
type ServerConfig struct {
	MaxConnections        int           // 最大100,000接続
	KeepAliveTimeout      time.Duration // 30秒
	WriteTimeout          time.Duration // 10秒
	ReadTimeout           time.Duration // 30秒
	MessageQueueSize      int           // 256
	BroadcastWorkers      int           // 100 ワーカー
	CleanupInterval       time.Duration // 5分
	ResourceCheckInterval time.Duration // 30秒
	MemoryLimitMB         float64       // 8GB
	HeartbeatInterval     time.Duration // 30秒
}

// SSEClient Server-Sent Events接続クライアント
type SSEClient struct {
	ID        string
	writer    http.ResponseWriter
	flusher   http.Flusher
	send      chan []byte
	channels  sync.Map     // map[string]bool - goroutine safe
	lastSeen  atomic.Int64 // atomic access
	bytesSent atomic.Int64 // atomic access
	ctx       context.Context
	cancel    context.CancelFunc
	// mu writeSSEでの書き込み直列化と、closedのRLock/Lock排他の両方に使う。
	// closed sendが閉じられたかどうかを示し、trySendはRLock下で確認してから送る。
	mu     sync.RWMutex
	closed bool
}

// trySend closed状態でなければsendチャネルへ非ブロッキングで送ります。RLock保持中に
// 送るため、close側のLockと排他になりclose済みチャネルへの送信を防ぐ。
func (client *SSEClient) trySend(data []byte) error {
	client.mu.RLock()
	defer client.mu.RUnlock()

	if client.closed {
		return fmt.Errorf("client %s is closed", client.ID)
	}

	select {
	case client.send <- data:
		return nil
	default:
		return fmt.Errorf("send queue full")
	}
}

// Channel チャンネル管理
type Channel struct {
	ID           string
	clients      sync.Map     // map[string]*SSEClient - goroutine safe
	messageCount atomic.Int64 // atomic access
	created      time.Time
	lastActivity atomic.Int64 // atomic access - Unix timestamp
}

// ConnectionHub 接続管理の中央ハブ
type ConnectionHub struct {
	register      chan *SSEClient
	unregister    chan *SSEClient
	broadcast     chan *BroadcastMessage
	activeClients atomic.Int64 // atomic access
}

// BroadcastMessage ブロードキャスト用メッセージ
type BroadcastMessage struct {
	ChannelID     string
	Message       []byte
	ExcludeClient string
}

// ServerMetrics サーバーメトリクス
type ServerMetrics struct {
	totalConnections  atomic.Int64 // atomic access
	activeConnections atomic.Int64 // atomic access
	peakConnections   atomic.Int64 // atomic access
	totalMessages     atomic.Int64 // atomic access
	broadcastMessages atomic.Int64 // atomic access
	errorCount        atomic.Int64 // atomic access
	channelCount      atomic.Int64 // atomic access
	startTime         time.Time
}

// MessageRouter メッセージルーティング
type MessageRouter struct {
	routes map[string]func(*SSEClient, map[string]any)
	mu     sync.RWMutex
}

// NewMassiveConnectionServer 新しい大規模接続サーバーを作成
func NewMassiveConnectionServer(config *ServerConfig) *MassiveConnectionServer {
	hub := &ConnectionHub{
		register:   make(chan *SSEClient, 1000),
		unregister: make(chan *SSEClient, 1000),
		broadcast:  make(chan *BroadcastMessage, 10000),
	}

	server := &MassiveConnectionServer{
		config:   config,
		hub:      hub,
		shutdown: make(chan struct{}),
		metrics: &ServerMetrics{
			startTime: time.Now(),
		},
		messageRouter: &MessageRouter{
			routes: make(map[string]func(*SSEClient, map[string]any)),
		},
	}

	// メッセージルーター設定
	server.setupMessageRoutes()

	return server
}

// setupMessageRoutes メッセージルートを設定
func (mcs *MassiveConnectionServer) setupMessageRoutes() {
	mcs.messageRouter.routes["join_channel"] = mcs.handleJoinChannel
	mcs.messageRouter.routes["leave_channel"] = mcs.handleLeaveChannel
	mcs.messageRouter.routes["broadcast"] = mcs.handleBroadcast
	mcs.messageRouter.routes["direct_message"] = mcs.handleDirectMessage
	mcs.messageRouter.routes["ping"] = mcs.handlePing
}

// Start サーバーを開始
func (mcs *MassiveConnectionServer) Start(addr string) error {
	log.Printf("Starting massive connection server on %s", addr)
	log.Printf("Max connections: %d", mcs.config.MaxConnections)
	log.Printf("Using Server-Sent Events for real-time communication")

	// 接続ハブ開始
	go mcs.hub.run(mcs)

	// リソース監視開始
	go mcs.startResourceMonitoring()

	// メトリクス収集開始
	go mcs.startMetricsCollection()

	// クリーンアップワーカー開始
	go mcs.startCleanupWorker()

	// HTTPハンドラー設定（DefaultServeMuxではなくローカルなmuxに登録する）
	mux := http.NewServeMux()
	mux.HandleFunc("/events", mcs.handleSSEConnection)
	mux.HandleFunc("/send", mcs.handleSendMessage)
	mux.HandleFunc("/metrics", mcs.handleMetrics)
	mux.HandleFunc("/health", mcs.handleHealth)
	mux.HandleFunc("/connections", mcs.handleConnections)
	mux.HandleFunc("/", mcs.handleIndex)
	mcs.mux = mux

	// HTTPサーバー設定
	server := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  mcs.config.ReadTimeout,
		WriteTimeout: mcs.config.WriteTimeout,
		IdleTimeout:  mcs.config.KeepAliveTimeout,
	}

	log.Println("Server endpoints:")
	log.Println("  /events - Server-Sent Events connection")
	log.Println("  /send - Send message (POST)")
	log.Println("  /metrics - Server metrics")
	log.Println("  /health - Health check")
	log.Println("  /connections - Connection statistics")
	log.Println("  / - Test client page")

	return server.ListenAndServe()
}

// handleSSEConnection Server-Sent Events接続をハンドル
func (mcs *MassiveConnectionServer) handleSSEConnection(w http.ResponseWriter, r *http.Request) {
	// 接続数制限チェック
	currentConnections := mcs.metrics.activeConnections.Load()
	if currentConnections >= int64(mcs.config.MaxConnections) {
		http.Error(w, "Connection limit exceeded", http.StatusServiceUnavailable)
		mcs.metrics.errorCount.Add(1)
		return
	}

	// Flusher取得（Server-Sent Eventsに必要）
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "SSE not supported", http.StatusInternalServerError)
		return
	}

	// SSEヘッダー設定
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Cache-Control")

	// クライアント作成
	clientID := fmt.Sprintf("client_%d_%d",
		time.Now().UnixNano(),
		mcs.metrics.totalConnections.Add(1))

	client := mcs.createSSEClient(clientID, w, flusher, r.Context())

	// ハブに登録
	mcs.hub.register <- client

	// 接続確認メッセージ送信
	welcomeMsg := map[string]any{
		"type":      "connection",
		"client_id": clientID,
		"timestamp": time.Now().Unix(),
		"message":   "Connected successfully",
	}
	if err := client.sendJSON(welcomeMsg); err != nil {
		log.Printf("Failed to send welcome message: %v", err)
	}

	// ハートビート開始
	go client.heartbeat(mcs.config.HeartbeatInterval)

	// 接続維持（クライアント切断かサーバー停止まで待機）
	select {
	case <-client.ctx.Done():
	case <-mcs.shutdown:
	}
	mcs.hub.unregister <- client
}

// createSSEClient 新しいSSEクライアントを作成
func (mcs *MassiveConnectionServer) createSSEClient(id string, w http.ResponseWriter, flusher http.Flusher, ctx context.Context) *SSEClient {
	clientCtx, cancel := context.WithCancel(ctx)

	client := &SSEClient{
		ID:      id,
		writer:  w,
		flusher: flusher,
		send:    make(chan []byte, mcs.config.MessageQueueSize),
		ctx:     clientCtx,
		cancel:  cancel,
	}

	client.lastSeen.Store(time.Now().Unix())

	// 送信ループ開始
	go client.sendLoop()

	return client
}

// sendLoop メッセージ送信ループ
func (client *SSEClient) sendLoop() {
	for {
		select {
		case message, ok := <-client.send:
			if !ok {
				return
			}
			if err := client.writeSSE(message); err != nil {
				log.Printf("Failed to send message to client %s: %v", client.ID, err)
				client.cancel()
				return
			}
			client.bytesSent.Add(int64(len(message)))

		case <-client.ctx.Done():
			return
		}
	}
}

// writeSSE SSEメッセージを送信
func (client *SSEClient) writeSSE(data []byte) error {
	client.mu.Lock()
	defer client.mu.Unlock()

	// SSE形式でデータ送信
	_, err := fmt.Fprintf(client.writer, "data: %s\n\n", string(data))
	if err != nil {
		return err
	}

	client.flusher.Flush()
	client.lastSeen.Store(time.Now().Unix())
	return nil
}

// sendJSON JSONメッセージを送信
func (client *SSEClient) sendJSON(data any) error {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}

	return client.trySend(jsonData)
}

// heartbeat ハートビートを送信
func (client *SSEClient) heartbeat(interval time.Duration) {
	tick := time.Tick(interval)

	for {
		select {
		case <-tick:
			heartbeat := map[string]any{
				"type":      "heartbeat",
				"timestamp": time.Now().Unix(),
			}
			if err := client.sendJSON(heartbeat); err != nil {
				client.cancel()
				return
			}

		case <-client.ctx.Done():
			return
		}
	}
}

// run 接続ハブのメインループ
func (ch *ConnectionHub) run(server *MassiveConnectionServer) {
	resourceTick := time.Tick(server.config.ResourceCheckInterval)

	for {
		select {
		case client := <-ch.register:
			ch.registerClient(client, server)

		case client := <-ch.unregister:
			ch.unregisterClient(client, server)

		case broadcast := <-ch.broadcast:
			go ch.handleBroadcast(broadcast, server)

		case <-resourceTick:
			go server.checkResourceLimits()

		case <-server.shutdown:
			return
		}
	}
}

// registerClient クライアントを登録
func (ch *ConnectionHub) registerClient(client *SSEClient, server *MassiveConnectionServer) {
	server.clients.Store(client.ID, client)

	activeCount := ch.activeClients.Add(1)
	server.metrics.activeConnections.Store(activeCount)

	// ピーク接続数更新
	if activeCount > server.metrics.peakConnections.Load() {
		server.metrics.peakConnections.Store(activeCount)
	}

	if activeCount%1000 == 0 {
		log.Printf("Active connections: %d", activeCount)
	}
}

// unregisterClient クライアントを登録解除
func (ch *ConnectionHub) unregisterClient(client *SSEClient, server *MassiveConnectionServer) {
	if _, loaded := server.clients.LoadAndDelete(client.ID); loaded {
		// closedをLock下で立ててからcloseする。trySendはRLock保持中に送信を
		// 完了させるため、以降に呼ばれるtrySendだけが弾かれる。
		client.mu.Lock()
		client.closed = true
		client.mu.Unlock()

		close(client.send)
		client.cancel()

		// チャンネルからクライアントを削除
		client.channels.Range(func(channelID, _ any) bool {
			if chVal, ok := server.channels.Load(channelID); ok {
				channel := chVal.(*Channel)
				channel.clients.Delete(client.ID)
			}
			return true
		})

		activeCount := ch.activeClients.Add(-1)
		server.metrics.activeConnections.Store(activeCount)
	}
}

// handleBroadcast ブロードキャストを処理
func (ch *ConnectionHub) handleBroadcast(broadcast *BroadcastMessage, server *MassiveConnectionServer) {
	server.metrics.broadcastMessages.Add(1)

	// 並行ブロードキャスト用のワーカープール
	semaphore := make(chan struct{}, server.config.BroadcastWorkers)
	var wg sync.WaitGroup

	if broadcast.ChannelID != "" {
		// チャンネル内ブロードキャスト
		if chVal, ok := server.channels.Load(broadcast.ChannelID); ok {
			channel := chVal.(*Channel)
			channel.clients.Range(func(clientID, clientVal any) bool {
				if clientID.(string) == broadcast.ExcludeClient {
					return true
				}

				if client, ok := clientVal.(*SSEClient); ok {
					wg.Go(func() {
						semaphore <- struct{}{}
						defer func() { <-semaphore }()

						// closed済み・キュー満杯はどちらも無視してよい
						_ = client.trySend(broadcast.Message)
					})
				}
				return true
			})
		}
	} else {
		// 全体ブロードキャスト
		server.clients.Range(func(clientID, clientVal any) bool {
			if clientID.(string) == broadcast.ExcludeClient {
				return true
			}

			if client, ok := clientVal.(*SSEClient); ok {
				wg.Go(func() {
					semaphore <- struct{}{}
					defer func() { <-semaphore }()

					// closed済み・キュー満杯はどちらも無視してよい
					_ = client.trySend(broadcast.Message)
				})
			}
			return true
		})
	}

	wg.Wait()
}

// handleSendMessage メッセージ送信をハンドル
func (mcs *MassiveConnectionServer) handleSendMessage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}

	var message map[string]any
	if err := json.Unmarshal(body, &message); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	mcs.metrics.totalMessages.Add(1)

	// メッセージ処理
	messageType, ok := message["type"].(string)
	if !ok {
		http.Error(w, "Missing message type", http.StatusBadRequest)
		return
	}

	mcs.messageRouter.mu.RLock()
	handler, exists := mcs.messageRouter.routes[messageType]
	mcs.messageRouter.mu.RUnlock()

	if exists {
		// ダミークライアント作成（API経由の場合）
		clientID := fmt.Sprintf("api_%d", time.Now().UnixNano())
		dummyClient := &SSEClient{ID: clientID}
		handler(dummyClient, message)
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]string{"status": "sent"}); err != nil {
		log.Printf("Failed to encode broadcast response: %v", err)
	}
}

// handleJoinChannel チャンネル参加を処理
func (mcs *MassiveConnectionServer) handleJoinChannel(client *SSEClient, message map[string]any) {
	channelID, ok := message["channel"].(string)
	if !ok || channelID == "" {
		return
	}

	// チャンネル作成または取得
	channelVal, _ := mcs.channels.LoadOrStore(channelID, &Channel{
		ID:      channelID,
		created: time.Now(),
	})
	channel := channelVal.(*Channel)

	// クライアントをチャンネルに追加
	channel.clients.Store(client.ID, client)
	client.channels.Store(channelID, true)
	channel.lastActivity.Store(time.Now().Unix())

	// チャンネル数更新
	channelCount := int64(0)
	mcs.channels.Range(func(_, _ any) bool {
		channelCount++
		return true
	})
	mcs.metrics.channelCount.Store(channelCount)

	log.Printf("Client %s joined channel %s", client.ID, channelID)
}

// handleLeaveChannel チャンネル退出を処理
func (mcs *MassiveConnectionServer) handleLeaveChannel(client *SSEClient, message map[string]any) {
	channelID, ok := message["channel"].(string)
	if !ok || channelID == "" {
		return
	}

	if channelVal, ok := mcs.channels.Load(channelID); ok {
		channel := channelVal.(*Channel)
		channel.clients.Delete(client.ID)
		client.channels.Delete(channelID)
		channel.lastActivity.Store(time.Now().Unix())
	}

	log.Printf("Client %s left channel %s", client.ID, channelID)
}

// handleBroadcast ブロードキャストメッセージを処理
func (mcs *MassiveConnectionServer) handleBroadcast(client *SSEClient, message map[string]any) {
	channelID, _ := message["channel"].(string)
	content, _ := message["content"].(string)

	broadcastMsg := map[string]any{
		"type":      "message",
		"channel":   channelID,
		"from":      client.ID,
		"content":   content,
		"timestamp": time.Now().Unix(),
	}

	messageBytes, err := json.Marshal(broadcastMsg)
	if err != nil {
		return
	}

	broadcast := &BroadcastMessage{
		ChannelID:     channelID,
		Message:       messageBytes,
		ExcludeClient: client.ID,
	}

	select {
	case mcs.hub.broadcast <- broadcast:
	default:
		log.Printf("Broadcast queue full")
	}
}

// handleDirectMessage ダイレクトメッセージを処理
func (mcs *MassiveConnectionServer) handleDirectMessage(client *SSEClient, message map[string]any) {
	targetID, ok := message["target"].(string)
	if !ok {
		return
	}

	content, _ := message["content"].(string)

	if targetVal, ok := mcs.clients.Load(targetID); ok {
		target := targetVal.(*SSEClient)

		directMsg := map[string]any{
			"type":      "direct_message",
			"from":      client.ID,
			"content":   content,
			"timestamp": time.Now().Unix(),
		}

		if err := target.sendJSON(directMsg); err != nil {
			log.Printf("Failed to send direct message: %v", err)
		}
	}
}

// handlePing Pingメッセージを処理
func (mcs *MassiveConnectionServer) handlePing(client *SSEClient, message map[string]any) {
	pongMsg := map[string]any{
		"type":      "pong",
		"timestamp": time.Now().Unix(),
	}

	if err := client.sendJSON(pongMsg); err != nil {
		log.Printf("Failed to send pong message: %v", err)
	}
}

// startResourceMonitoring リソース監視を開始
func (mcs *MassiveConnectionServer) startResourceMonitoring() {
	tick := time.Tick(mcs.config.ResourceCheckInterval)

	for {
		select {
		case <-tick:
			mcs.monitorResources()
		case <-mcs.shutdown:
			return
		}
	}
}

// monitorResources リソース使用量を監視
func (mcs *MassiveConnectionServer) monitorResources() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	currentMemoryMB := float64(m.Alloc) / 1024 / 1024
	goroutines := runtime.NumGoroutine()
	activeConnections := mcs.metrics.activeConnections.Load()

	// メモリ使用量チェック
	if currentMemoryMB > mcs.config.MemoryLimitMB {
		log.Printf("WARNING: High memory usage: %.2f MB (limit: %.2f MB)",
			currentMemoryMB, mcs.config.MemoryLimitMB)
		runtime.GC()
	}

	// Goroutine数チェック
	expectedGoroutines := activeConnections*3 + 100 // 各接続につき3つのgoroutine + システム
	if int64(goroutines) > expectedGoroutines*2 {
		log.Printf("WARNING: High goroutine count: %d (expected: ~%d)",
			goroutines, expectedGoroutines)
	}

	if activeConnections%10000 == 0 && activeConnections > 0 {
		log.Printf("Resource status - Memory: %.2f MB, Goroutines: %d, Connections: %d",
			currentMemoryMB, goroutines, activeConnections)
	}
}

// checkResourceLimits リソース制限をチェック
func (mcs *MassiveConnectionServer) checkResourceLimits() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	currentMemoryMB := float64(m.Alloc) / 1024 / 1024

	// メモリ使用量が制限を超えた場合の警告
	if currentMemoryMB > mcs.config.MemoryLimitMB*0.9 {
		log.Printf("Resource limit approaching - Memory: %.2f/%.2f MB",
			currentMemoryMB, mcs.config.MemoryLimitMB)
	}
}

// startMetricsCollection メトリクス収集を開始
func (mcs *MassiveConnectionServer) startMetricsCollection() {
	tick := time.Tick(30 * time.Second)

	for {
		select {
		case <-tick:
			mcs.logMetrics()
		case <-mcs.shutdown:
			return
		}
	}
}

// logMetrics メトリクスをログ出力
func (mcs *MassiveConnectionServer) logMetrics() {
	activeConnections := mcs.metrics.activeConnections.Load()
	totalMessages := mcs.metrics.totalMessages.Load()
	broadcastMessages := mcs.metrics.broadcastMessages.Load()

	if activeConnections > 0 {
		uptime := time.Since(mcs.metrics.startTime)
		messagesPerSecond := float64(totalMessages) / uptime.Seconds()

		log.Printf("Metrics - Active: %d, Peak: %d, Messages: %d (%.1f/sec), Broadcasts: %d",
			activeConnections,
			mcs.metrics.peakConnections.Load(),
			totalMessages,
			messagesPerSecond,
			broadcastMessages)
	}
}

// startCleanupWorker クリーンアップワーカーを開始
func (mcs *MassiveConnectionServer) startCleanupWorker() {
	tick := time.Tick(mcs.config.CleanupInterval)

	for {
		select {
		case <-tick:
			mcs.performCleanup()
		case <-mcs.shutdown:
			return
		}
	}
}

// performCleanup クリーンアップを実行
func (mcs *MassiveConnectionServer) performCleanup() {
	now := time.Now().Unix()
	cutoff := now - 300 // 5分前

	// 古い接続をクリーンアップ
	var staleClients []*SSEClient
	mcs.clients.Range(func(clientID, clientVal any) bool {
		client := clientVal.(*SSEClient)
		if client.lastSeen.Load() < cutoff {
			staleClients = append(staleClients, client)
		}
		return true
	})

	for _, client := range staleClients {
		mcs.hub.unregister <- client
	}

	if len(staleClients) > 0 {
		log.Printf("Cleaned up %d stale connections", len(staleClients))
	}

	// 定期的なガベージコレクション
	runtime.GC()
}

// handleMetrics メトリクス情報を返すHTTPハンドラー
func (mcs *MassiveConnectionServer) handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	metrics := map[string]any{
		"total_connections":  mcs.metrics.totalConnections.Load(),
		"active_connections": mcs.metrics.activeConnections.Load(),
		"peak_connections":   mcs.metrics.peakConnections.Load(),
		"total_messages":     mcs.metrics.totalMessages.Load(),
		"broadcast_messages": mcs.metrics.broadcastMessages.Load(),
		"error_count":        mcs.metrics.errorCount.Load(),
		"channel_count":      mcs.metrics.channelCount.Load(),
		"uptime_seconds":     time.Since(mcs.metrics.startTime).Seconds(),
		"memory_mb":          float64(m.Alloc) / 1024 / 1024,
		"memory_sys_mb":      float64(m.Sys) / 1024 / 1024,
		"goroutines":         runtime.NumGoroutine(),
		"gc_runs":            m.NumGC,
	}

	if err := json.NewEncoder(w).Encode(metrics); err != nil {
		log.Printf("Failed to encode metrics: %v", err)
	}
}

// handleHealth ヘルスチェック用HTTPハンドラー
func (mcs *MassiveConnectionServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	memoryMB := float64(m.Alloc) / 1024 / 1024

	status := "healthy"
	if memoryMB > mcs.config.MemoryLimitMB*0.9 {
		status = "warning"
	}
	if memoryMB > mcs.config.MemoryLimitMB {
		status = "critical"
	}

	health := map[string]any{
		"status":             status,
		"timestamp":          time.Now().Unix(),
		"active_connections": mcs.metrics.activeConnections.Load(),
		"memory_mb":          memoryMB,
		"memory_limit_mb":    mcs.config.MemoryLimitMB,
		"goroutines":         runtime.NumGoroutine(),
	}

	statusCode := http.StatusOK
	if status == "critical" {
		statusCode = http.StatusServiceUnavailable
	}

	w.WriteHeader(statusCode)
	if err := json.NewEncoder(w).Encode(health); err != nil {
		log.Printf("Failed to encode health response: %v", err)
	}
}

// handleConnections 接続統計を返すHTTPハンドラー
func (mcs *MassiveConnectionServer) handleConnections(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	// チャンネル別統計
	channelStats := make(map[string]any)
	mcs.channels.Range(func(channelID, channelVal any) bool {
		channel := channelVal.(*Channel)
		clientCount := 0
		channel.clients.Range(func(_, _ any) bool {
			clientCount++
			return true
		})

		channelStats[channelID.(string)] = map[string]any{
			"client_count":  clientCount,
			"message_count": channel.messageCount.Load(),
			"created":       channel.created.Unix(),
			"last_activity": channel.lastActivity.Load(),
		}
		return true
	})

	stats := map[string]any{
		"active_connections": mcs.metrics.activeConnections.Load(),
		"peak_connections":   mcs.metrics.peakConnections.Load(),
		"total_connections":  mcs.metrics.totalConnections.Load(),
		"channel_count":      mcs.metrics.channelCount.Load(),
		"channels":           channelStats,
		"uptime_seconds":     time.Since(mcs.metrics.startTime).Seconds(),
	}

	if err := json.NewEncoder(w).Encode(stats); err != nil {
		log.Printf("Failed to encode stats: %v", err)
	}
}

// handleIndex テスト用HTMLページを提供
func (mcs *MassiveConnectionServer) handleIndex(w http.ResponseWriter, r *http.Request) {
	html := `
<!DOCTYPE html>
<html>
<head>
    <title>Massive Connection Server Test</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        .container { max-width: 800px; margin: 0 auto; }
        .section { margin: 20px 0; padding: 15px; border: 1px solid #ddd; }
        button { padding: 10px 20px; margin: 5px; }
        #messages { height: 300px; overflow-y: scroll; border: 1px solid #ccc; padding: 10px; }
        input[type="text"] { width: 300px; padding: 5px; }
    </style>
</head>
<body>
    <div class="container">
        <h1>Massive Connection Server Test Client</h1>
        
        <div class="section">
            <h3>Connection Status</h3>
            <div id="status">Disconnected</div>
            <button onclick="connect()">Connect</button>
            <button onclick="disconnect()">Disconnect</button>
        </div>
        
        <div class="section">
            <h3>Channel Management</h3>
            <input type="text" id="channelInput" placeholder="Channel name">
            <button onclick="joinChannel()">Join Channel</button>
            <button onclick="leaveChannel()">Leave Channel</button>
        </div>
        
        <div class="section">
            <h3>Send Message</h3>
            <input type="text" id="messageInput" placeholder="Message content">
            <input type="text" id="targetChannel" placeholder="Channel (optional)">
            <button onclick="sendMessage()">Send Broadcast</button>
        </div>
        
        <div class="section">
            <h3>Messages</h3>
            <div id="messages"></div>
            <button onclick="clearMessages()">Clear</button>
        </div>
    </div>
    
    <script>
        let eventSource = null;
        let clientId = null;
        
        function connect() {
            if (eventSource) {
                eventSource.close();
            }
            
            eventSource = new EventSource('/events');
            
            eventSource.onopen = function() {
                document.getElementById('status').textContent = 'Connected';
            };
            
            eventSource.onmessage = function(event) {
                try {
                    const data = JSON.parse(event.data);
                    if (data.type === 'connection') {
                        clientId = data.client_id;
                    }
                    addMessage('Received: ' + JSON.stringify(data));
                } catch (e) {
                    addMessage('Raw: ' + event.data);
                }
            };
            
            eventSource.onerror = function() {
                document.getElementById('status').textContent = 'Connection Error';
            };
        }
        
        function disconnect() {
            if (eventSource) {
                eventSource.close();
                eventSource = null;
            }
            document.getElementById('status').textContent = 'Disconnected';
        }
        
        function joinChannel() {
            const channel = document.getElementById('channelInput').value;
            if (!channel) return;
            
            sendAPIMessage({
                type: 'join_channel',
                channel: channel
            });
        }
        
        function leaveChannel() {
            const channel = document.getElementById('channelInput').value;
            if (!channel) return;
            
            sendAPIMessage({
                type: 'leave_channel',
                channel: channel
            });
        }
        
        function sendMessage() {
            const content = document.getElementById('messageInput').value;
            const channel = document.getElementById('targetChannel').value;
            if (!content) return;
            
            sendAPIMessage({
                type: 'broadcast',
                content: content,
                channel: channel || ''
            });
            
            document.getElementById('messageInput').value = '';
        }
        
        function sendAPIMessage(message) {
            fetch('/send', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(message)
            });
        }
        
        function addMessage(message) {
            const messagesDiv = document.getElementById('messages');
            const time = new Date().toLocaleTimeString();
            messagesDiv.innerHTML += '<div>[' + time + '] ' + message + '</div>';
            messagesDiv.scrollTop = messagesDiv.scrollHeight;
        }
        
        function clearMessages() {
            document.getElementById('messages').innerHTML = '';
        }
    </script>
</body>
</html>
`
	w.Header().Set("Content-Type", "text/html")
	if _, err := w.Write([]byte(html)); err != nil {
		log.Printf("Failed to write HTML response: %v", err)
	}
}

func main() {
	config := &ServerConfig{
		MaxConnections:        100000,           // 10万接続
		KeepAliveTimeout:      60 * time.Second, // 60秒Keep-Alive
		WriteTimeout:          30 * time.Second, // 30秒Write timeout
		ReadTimeout:           30 * time.Second, // 30秒Read timeout
		MessageQueueSize:      256,              // 256メッセージキュー
		BroadcastWorkers:      100,              // 100ブロードキャストワーカー
		CleanupInterval:       5 * time.Minute,  // 5分クリーンアップ
		ResourceCheckInterval: 30 * time.Second, // 30秒リソースチェック
		MemoryLimitMB:         8192,             // 8GBメモリ制限
		HeartbeatInterval:     30 * time.Second, // 30秒ハートビート
	}

	server := NewMassiveConnectionServer(config)

	log.Println("=== Massive Connection Server (SSE) ===")
	log.Printf("Target: %d concurrent connections", config.MaxConnections)
	log.Printf("Memory limit: %.1f GB", config.MemoryLimitMB/1024)
	log.Printf("Using Server-Sent Events for real-time communication")
	log.Printf("Expected memory per connection: ~0.05 MB")
	log.Printf("Total expected memory: ~%.1f GB", float64(config.MaxConnections)*0.05/1024)

	if err := server.Start(":8080"); err != nil {
		log.Fatalf("Server failed to start: %v", err)
	}
}
