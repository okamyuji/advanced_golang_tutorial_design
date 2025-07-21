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

// MassiveConnectionServer 0万並行接続対応サーバー（Server-Sent Events使用）
type MassiveConnectionServer struct {
	clients       sync.Map // map[string]*SSEClient - goroutine safe
	channels      sync.Map // map[string]*Channel - goroutine safe
	metrics       *ServerMetrics
	config        *ServerConfig
	hub           *ConnectionHub
	shutdown      chan struct{}
	messageRouter *MessageRouter
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
	channels  sync.Map // map[string]bool - goroutine safe
	lastSeen  int64    // atomic access
	bytesSent int64    // atomic access
	ctx       context.Context
	cancel    context.CancelFunc
	mu        sync.Mutex
}

// Channel チャンネル管理
type Channel struct {
	ID           string
	clients      sync.Map // map[string]*SSEClient - goroutine safe
	messageCount int64    // atomic access
	created      time.Time
	lastActivity int64 // atomic access - Unix timestamp
}

// ConnectionHub 接続管理の中央ハブ
type ConnectionHub struct {
	register      chan *SSEClient
	unregister    chan *SSEClient
	broadcast     chan *BroadcastMessage
	activeClients int64 // atomic access
}

// BroadcastMessage ブロードキャスト用メッセージ
type BroadcastMessage struct {
	ChannelID     string
	Message       []byte
	ExcludeClient string
}

// ServerMetrics サーバーメトリクス
type ServerMetrics struct {
	totalConnections  int64 // atomic access
	activeConnections int64 // atomic access
	peakConnections   int64 // atomic access
	totalMessages     int64 // atomic access
	broadcastMessages int64 // atomic access
	errorCount        int64 // atomic access
	channelCount      int64 // atomic access
	startTime         time.Time
}

// MessageRouter メッセージルーティング
type MessageRouter struct {
	routes map[string]func(*SSEClient, map[string]interface{})
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
			routes: make(map[string]func(*SSEClient, map[string]interface{})),
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

	// HTTPハンドラー設定
	http.HandleFunc("/events", mcs.handleSSEConnection)
	http.HandleFunc("/send", mcs.handleSendMessage)
	http.HandleFunc("/metrics", mcs.handleMetrics)
	http.HandleFunc("/health", mcs.handleHealth)
	http.HandleFunc("/connections", mcs.handleConnections)
	http.HandleFunc("/", mcs.handleIndex)

	// HTTPサーバー設定
	server := &http.Server{
		Addr:         addr,
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
	currentConnections := atomic.LoadInt64(&mcs.metrics.activeConnections)
	if currentConnections >= int64(mcs.config.MaxConnections) {
		http.Error(w, "Connection limit exceeded", http.StatusServiceUnavailable)
		atomic.AddInt64(&mcs.metrics.errorCount, 1)
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
		atomic.AddInt64(&mcs.metrics.totalConnections, 1))

	client := mcs.createSSEClient(clientID, w, flusher, r.Context())

	// ハブに登録
	mcs.hub.register <- client

	// 接続確認メッセージ送信
	welcomeMsg := map[string]interface{}{
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

	// 接続維持（クライアント切断まで待機）
	<-client.ctx.Done()
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

	atomic.StoreInt64(&client.lastSeen, time.Now().Unix())

	// 送信ループ開始
	go client.sendLoop()

	return client
}

// sendLoop メッセージ送信ループ
func (client *SSEClient) sendLoop() {
	for {
		select {
		case message := <-client.send:
			if err := client.writeSSE(message); err != nil {
				log.Printf("Failed to send message to client %s: %v", client.ID, err)
				client.cancel()
				return
			}
			atomic.AddInt64(&client.bytesSent, int64(len(message)))

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
	atomic.StoreInt64(&client.lastSeen, time.Now().Unix())
	return nil
}

// sendJSON JSONメッセージを送信
func (client *SSEClient) sendJSON(data interface{}) error {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}

	select {
	case client.send <- jsonData:
		return nil
	default:
		return fmt.Errorf("send queue full")
	}
}

// heartbeat ハートビートを送信
func (client *SSEClient) heartbeat(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			heartbeat := map[string]interface{}{
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
	resourceTicker := time.NewTicker(server.config.ResourceCheckInterval)
	defer resourceTicker.Stop()

	for {
		select {
		case client := <-ch.register:
			ch.registerClient(client, server)

		case client := <-ch.unregister:
			ch.unregisterClient(client, server)

		case broadcast := <-ch.broadcast:
			go ch.handleBroadcast(broadcast, server)

		case <-resourceTicker.C:
			go server.checkResourceLimits()

		case <-server.shutdown:
			return
		}
	}
}

// registerClient クライアントを登録
func (ch *ConnectionHub) registerClient(client *SSEClient, server *MassiveConnectionServer) {
	server.clients.Store(client.ID, client)

	activeCount := atomic.AddInt64(&ch.activeClients, 1)
	atomic.StoreInt64(&server.metrics.activeConnections, activeCount)

	// ピーク接続数更新
	if activeCount > atomic.LoadInt64(&server.metrics.peakConnections) {
		atomic.StoreInt64(&server.metrics.peakConnections, activeCount)
	}

	if activeCount%1000 == 0 {
		log.Printf("Active connections: %d", activeCount)
	}
}

// unregisterClient クライアントを登録解除
func (ch *ConnectionHub) unregisterClient(client *SSEClient, server *MassiveConnectionServer) {
	if _, loaded := server.clients.LoadAndDelete(client.ID); loaded {
		close(client.send)
		client.cancel()

		// チャンネルからクライアントを削除
		client.channels.Range(func(channelID, _ interface{}) bool {
			if chVal, ok := server.channels.Load(channelID); ok {
				channel := chVal.(*Channel)
				channel.clients.Delete(client.ID)
			}
			return true
		})

		activeCount := atomic.AddInt64(&ch.activeClients, -1)
		atomic.StoreInt64(&server.metrics.activeConnections, activeCount)
	}
}

// handleBroadcast ブロードキャストを処理
func (ch *ConnectionHub) handleBroadcast(broadcast *BroadcastMessage, server *MassiveConnectionServer) {
	atomic.AddInt64(&server.metrics.broadcastMessages, 1)

	// 並行ブロードキャスト用のワーカープール
	semaphore := make(chan struct{}, server.config.BroadcastWorkers)
	var wg sync.WaitGroup

	if broadcast.ChannelID != "" {
		// チャンネル内ブロードキャスト
		if chVal, ok := server.channels.Load(broadcast.ChannelID); ok {
			channel := chVal.(*Channel)
			channel.clients.Range(func(clientID, clientVal interface{}) bool {
				if clientID.(string) == broadcast.ExcludeClient {
					return true
				}

				if client, ok := clientVal.(*SSEClient); ok {
					wg.Add(1)
					go func(c *SSEClient) {
						defer wg.Done()
						semaphore <- struct{}{}
						defer func() { <-semaphore }()

						select {
						case c.send <- broadcast.Message:
						default:
							// 送信キューが満杯の場合は無視
						}
					}(client)
				}
				return true
			})
		}
	} else {
		// 全体ブロードキャスト
		server.clients.Range(func(clientID, clientVal interface{}) bool {
			if clientID.(string) == broadcast.ExcludeClient {
				return true
			}

			if client, ok := clientVal.(*SSEClient); ok {
				wg.Add(1)
				go func(c *SSEClient) {
					defer wg.Done()
					semaphore <- struct{}{}
					defer func() { <-semaphore }()

					select {
					case c.send <- broadcast.Message:
					default:
						// 送信キューが満杯の場合は無視
					}
				}(client)
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

	var message map[string]interface{}
	if err := json.Unmarshal(body, &message); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	atomic.AddInt64(&mcs.metrics.totalMessages, 1)

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
func (mcs *MassiveConnectionServer) handleJoinChannel(client *SSEClient, message map[string]interface{}) {
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
	atomic.StoreInt64(&channel.lastActivity, time.Now().Unix())

	// チャンネル数更新
	channelCount := int64(0)
	mcs.channels.Range(func(_, _ interface{}) bool {
		channelCount++
		return true
	})
	atomic.StoreInt64(&mcs.metrics.channelCount, channelCount)

	log.Printf("Client %s joined channel %s", client.ID, channelID)
}

// handleLeaveChannel チャンネル退出を処理
func (mcs *MassiveConnectionServer) handleLeaveChannel(client *SSEClient, message map[string]interface{}) {
	channelID, ok := message["channel"].(string)
	if !ok || channelID == "" {
		return
	}

	if channelVal, ok := mcs.channels.Load(channelID); ok {
		channel := channelVal.(*Channel)
		channel.clients.Delete(client.ID)
		client.channels.Delete(channelID)
		atomic.StoreInt64(&channel.lastActivity, time.Now().Unix())
	}

	log.Printf("Client %s left channel %s", client.ID, channelID)
}

// handleBroadcast ブロードキャストメッセージを処理
func (mcs *MassiveConnectionServer) handleBroadcast(client *SSEClient, message map[string]interface{}) {
	channelID, _ := message["channel"].(string)
	content, _ := message["content"].(string)

	broadcastMsg := map[string]interface{}{
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
func (mcs *MassiveConnectionServer) handleDirectMessage(client *SSEClient, message map[string]interface{}) {
	targetID, ok := message["target"].(string)
	if !ok {
		return
	}

	content, _ := message["content"].(string)

	if targetVal, ok := mcs.clients.Load(targetID); ok {
		target := targetVal.(*SSEClient)

		directMsg := map[string]interface{}{
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
func (mcs *MassiveConnectionServer) handlePing(client *SSEClient, message map[string]interface{}) {
	pongMsg := map[string]interface{}{
		"type":      "pong",
		"timestamp": time.Now().Unix(),
	}

	if err := client.sendJSON(pongMsg); err != nil {
		log.Printf("Failed to send pong message: %v", err)
	}
}

// startResourceMonitoring リソース監視を開始
func (mcs *MassiveConnectionServer) startResourceMonitoring() {
	ticker := time.NewTicker(mcs.config.ResourceCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
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
	activeConnections := atomic.LoadInt64(&mcs.metrics.activeConnections)

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
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			mcs.logMetrics()
		case <-mcs.shutdown:
			return
		}
	}
}

// logMetrics メトリクスをログ出力
func (mcs *MassiveConnectionServer) logMetrics() {
	activeConnections := atomic.LoadInt64(&mcs.metrics.activeConnections)
	totalMessages := atomic.LoadInt64(&mcs.metrics.totalMessages)
	broadcastMessages := atomic.LoadInt64(&mcs.metrics.broadcastMessages)

	if activeConnections > 0 {
		uptime := time.Since(mcs.metrics.startTime)
		messagesPerSecond := float64(totalMessages) / uptime.Seconds()

		log.Printf("Metrics - Active: %d, Peak: %d, Messages: %d (%.1f/sec), Broadcasts: %d",
			activeConnections,
			atomic.LoadInt64(&mcs.metrics.peakConnections),
			totalMessages,
			messagesPerSecond,
			broadcastMessages)
	}
}

// startCleanupWorker クリーンアップワーカーを開始
func (mcs *MassiveConnectionServer) startCleanupWorker() {
	ticker := time.NewTicker(mcs.config.CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
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
	mcs.clients.Range(func(clientID, clientVal interface{}) bool {
		client := clientVal.(*SSEClient)
		if atomic.LoadInt64(&client.lastSeen) < cutoff {
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

	metrics := map[string]interface{}{
		"total_connections":  atomic.LoadInt64(&mcs.metrics.totalConnections),
		"active_connections": atomic.LoadInt64(&mcs.metrics.activeConnections),
		"peak_connections":   atomic.LoadInt64(&mcs.metrics.peakConnections),
		"total_messages":     atomic.LoadInt64(&mcs.metrics.totalMessages),
		"broadcast_messages": atomic.LoadInt64(&mcs.metrics.broadcastMessages),
		"error_count":        atomic.LoadInt64(&mcs.metrics.errorCount),
		"channel_count":      atomic.LoadInt64(&mcs.metrics.channelCount),
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

	health := map[string]interface{}{
		"status":             status,
		"timestamp":          time.Now().Unix(),
		"active_connections": atomic.LoadInt64(&mcs.metrics.activeConnections),
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
	channelStats := make(map[string]interface{})
	mcs.channels.Range(func(channelID, channelVal interface{}) bool {
		channel := channelVal.(*Channel)
		clientCount := 0
		channel.clients.Range(func(_, _ interface{}) bool {
			clientCount++
			return true
		})

		channelStats[channelID.(string)] = map[string]interface{}{
			"client_count":  clientCount,
			"message_count": atomic.LoadInt64(&channel.messageCount),
			"created":       channel.created.Unix(),
			"last_activity": atomic.LoadInt64(&channel.lastActivity),
		}
		return true
	})

	stats := map[string]interface{}{
		"active_connections": atomic.LoadInt64(&mcs.metrics.activeConnections),
		"peak_connections":   atomic.LoadInt64(&mcs.metrics.peakConnections),
		"total_connections":  atomic.LoadInt64(&mcs.metrics.totalConnections),
		"channel_count":      atomic.LoadInt64(&mcs.metrics.channelCount),
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
