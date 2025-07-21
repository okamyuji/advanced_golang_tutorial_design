package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// DistributedContextは分散コンテキストです
type DistributedContext struct {
	ID           string
	ParentID     string
	TraceID      string
	SpanID       string
	Values       map[string]interface{}
	Deadline     time.Time
	Metadata     map[string]string
	CreatedAt    time.Time
	UpdatedAt    time.Time
	NodeIDs      []string
	PropagatedTo map[string]bool
}

// ContextEventはコンテキストイベントです
type ContextEvent struct {
	Type      ContextEventType
	ContextID string
	NodeID    string
	Timestamp time.Time
	Data      map[string]interface{}
	Source    string
}

// ContextEventTypeはコンテキストイベントタイプです
type ContextEventType int

const (
	EventContextCreated ContextEventType = iota
	EventContextUpdated
	EventContextPropagated
	EventContextCancelled
	EventContextExpired
	EventNodeJoined
	EventNodeLeft
)

// DistributedContextManagerは分散コンテキスト管理システムです
type DistributedContextManager struct {
	// ノード情報
	nodeID       string
	clusterNodes map[string]*ClusterNode
	nodesMutex   sync.RWMutex

	// コンテキスト管理
	contexts      map[string]*DistributedContext
	contextsMutex sync.RWMutex

	// イベント処理
	eventQueue    chan ContextEvent
	eventHandlers map[ContextEventType][]EventHandler

	// 通信チャネル
	incomingChannel chan *ContextMessage
	outgoingChannel chan *ContextMessage

	// コンテキスト制御
	rootCtx context.Context
	cancel  context.CancelFunc

	// 同期制御
	wg sync.WaitGroup

	// 統計・監視
	stats *ManagerStats

	// 設定
	config *ManagerConfig

	// 制御フラグ
	isRunning int32
	startTime time.Time
}

// ClusterNodeはクラスターノードです
type ClusterNode struct {
	ID           string
	Address      string
	Status       NodeStatus
	LastSeen     time.Time
	Latency      time.Duration
	ContextCount int64
	MessagesSent int64
	MessagesRecv int64
	JoinedAt     time.Time
}

// NodeStatusはノードステータスです
type NodeStatus int

const (
	StatusUnknown NodeStatus = iota
	StatusActive
	StatusInactive
	StatusFailed
)

// ContextMessageはコンテキストメッセージです
type ContextMessage struct {
	Type       MessageType
	SourceNode string
	TargetNode string
	ContextID  string
	Context    *DistributedContext
	Data       map[string]interface{}
	Timestamp  time.Time
	MessageID  string
}

// MessageTypeはメッセージタイプです
type MessageType int

const (
	MessagePropagate MessageType = iota
	MessageUpdate
	MessageCancel
	MessageSync
	MessageHeartbeat
	MessageJoin
	MessageLeave
)

// EventHandlerはイベントハンドラーです
type EventHandler func(event ContextEvent) error

// ManagerStatsはマネージャー統計です
type ManagerStats struct {
	mu                 sync.RWMutex
	contextsCreated    int64
	contextsPropagated int64
	contextsCancelled  int64
	contextsExpired    int64
	messagesSent       int64
	messagesReceived   int64
	startTime          time.Time
}

// ManagerConfigはマネージャー設定です
type ManagerConfig struct {
	NodeID             string
	ClusterSize        int
	EventQueueSize     int
	MessageQueueSize   int
	HeartbeatInterval  time.Duration
	SyncInterval       time.Duration
	ContextTTL         time.Duration
	PropagationTimeout time.Duration
	MaxRetries         int
	EnableMetrics      bool
	MetricsInterval    time.Duration
}

// NewManagerConfigはデフォルト設定を作成します
func NewManagerConfig(nodeID string) *ManagerConfig {
	return &ManagerConfig{
		NodeID:             nodeID,
		ClusterSize:        5,
		EventQueueSize:     10000,
		MessageQueueSize:   5000,
		HeartbeatInterval:  5 * time.Second,
		SyncInterval:       30 * time.Second,
		ContextTTL:         5 * time.Minute,
		PropagationTimeout: 10 * time.Second,
		MaxRetries:         3,
		EnableMetrics:      true,
		MetricsInterval:    15 * time.Second,
	}
}

// NewDistributedContextManagerは新しい分散コンテキストマネージャーを作成します
func NewDistributedContextManager(config *ManagerConfig) *DistributedContextManager {
	ctx, cancel := context.WithCancel(context.Background())

	manager := &DistributedContextManager{
		nodeID:          config.NodeID,
		clusterNodes:    make(map[string]*ClusterNode),
		contexts:        make(map[string]*DistributedContext),
		eventQueue:      make(chan ContextEvent, config.EventQueueSize),
		eventHandlers:   make(map[ContextEventType][]EventHandler),
		incomingChannel: make(chan *ContextMessage, config.MessageQueueSize),
		outgoingChannel: make(chan *ContextMessage, config.MessageQueueSize),
		rootCtx:         ctx,
		cancel:          cancel,
		config:          config,
		stats: &ManagerStats{
			startTime: time.Now(),
		},
	}

	// デフォルトイベントハンドラーを登録
	manager.registerDefaultEventHandlers()

	return manager
}

// registerDefaultEventHandlersはデフォルトイベントハンドラーを登録します
func (dcm *DistributedContextManager) registerDefaultEventHandlers() {
	dcm.RegisterEventHandler(EventContextCreated, dcm.handleContextCreated)
	dcm.RegisterEventHandler(EventContextUpdated, dcm.handleContextUpdated)
	dcm.RegisterEventHandler(EventContextPropagated, dcm.handleContextPropagated)
	dcm.RegisterEventHandler(EventContextCancelled, dcm.handleContextCancelled)
	dcm.RegisterEventHandler(EventContextExpired, dcm.handleContextExpired)
	dcm.RegisterEventHandler(EventNodeJoined, dcm.handleNodeJoined)
	dcm.RegisterEventHandler(EventNodeLeft, dcm.handleNodeLeft)
}

// Startはマネージャーを開始します
func (dcm *DistributedContextManager) Start() error {
	if !atomic.CompareAndSwapInt32(&dcm.isRunning, 0, 1) {
		return fmt.Errorf("manager is already running")
	}

	dcm.startTime = time.Now()
	dcm.stats.startTime = dcm.startTime

	log.Printf("Starting distributed context manager: node=%s", dcm.nodeID)

	// イベント処理を開始
	dcm.wg.Add(1)
	go dcm.processEvents()

	// メッセージ処理を開始
	dcm.wg.Add(1)
	go dcm.handleIncomingMessages()

	dcm.wg.Add(1)
	go dcm.handleOutgoingMessages()

	// ハートビートを開始
	dcm.wg.Add(1)
	go dcm.heartbeatLoop()

	// 同期処理を開始
	dcm.wg.Add(1)
	go dcm.syncLoop()

	// コンテキスト期限管理を開始
	dcm.wg.Add(1)
	go dcm.contextExpirationLoop()

	// メトリクス監視を開始
	if dcm.config.EnableMetrics {
		dcm.wg.Add(1)
		go dcm.monitorMetrics()
	}

	log.Printf("Distributed context manager started successfully")
	return nil
}

// CreateContextは新しい分散コンテキストを作成します
func (dcm *DistributedContextManager) CreateContext(parentID string, values map[string]interface{}, deadline time.Time) (*DistributedContext, error) {
	if atomic.LoadInt32(&dcm.isRunning) == 0 {
		return nil, fmt.Errorf("manager is not running")
	}

	contextID := dcm.generateContextID()
	traceID := dcm.generateTraceID()
	spanID := dcm.generateSpanID()

	if parentID != "" {
		// 親コンテキストのトレースIDを継承
		dcm.contextsMutex.RLock()
		if parent, exists := dcm.contexts[parentID]; exists {
			traceID = parent.TraceID
		}
		dcm.contextsMutex.RUnlock()
	}

	// デフォルトの期限を設定
	if deadline.IsZero() {
		deadline = time.Now().Add(dcm.config.ContextTTL)
	}

	ctx := &DistributedContext{
		ID:           contextID,
		ParentID:     parentID,
		TraceID:      traceID,
		SpanID:       spanID,
		Values:       values,
		Deadline:     deadline,
		Metadata:     make(map[string]string),
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
		NodeIDs:      []string{dcm.nodeID},
		PropagatedTo: make(map[string]bool),
	}

	// コンテキストを保存
	dcm.contextsMutex.Lock()
	dcm.contexts[contextID] = ctx
	dcm.contextsMutex.Unlock()

	// イベントを発行
	event := ContextEvent{
		Type:      EventContextCreated,
		ContextID: contextID,
		NodeID:    dcm.nodeID,
		Timestamp: time.Now(),
		Data: map[string]interface{}{
			"parent_id": parentID,
			"trace_id":  traceID,
			"deadline":  deadline,
		},
		Source: dcm.nodeID,
	}

	select {
	case dcm.eventQueue <- event:
	case <-dcm.rootCtx.Done():
		return nil, fmt.Errorf("manager is shutting down")
	default:
		log.Printf("Event queue is full, dropping context created event")
	}

	// 統計更新
	atomic.AddInt64(&dcm.stats.contextsCreated, 1)

	log.Printf("Context created: %s (parent=%s, trace=%s)", contextID, parentID, traceID)
	return ctx, nil
}

// PropagateContextはコンテキストを他のノードに伝播します
func (dcm *DistributedContextManager) PropagateContext(contextID string, targetNodes []string) error {
	dcm.contextsMutex.RLock()
	ctx, exists := dcm.contexts[contextID]
	dcm.contextsMutex.RUnlock()

	if !exists {
		return fmt.Errorf("context not found: %s", contextID)
	}

	// 指定されたノードまたは全ノードに伝播
	var targets []string
	if len(targetNodes) == 0 {
		dcm.nodesMutex.RLock()
		for nodeID := range dcm.clusterNodes {
			if nodeID != dcm.nodeID {
				targets = append(targets, nodeID)
			}
		}
		dcm.nodesMutex.RUnlock()
	} else {
		targets = targetNodes
	}

	// 各ターゲットノードにメッセージを送信
	for _, targetNode := range targets {
		message := &ContextMessage{
			Type:       MessagePropagate,
			SourceNode: dcm.nodeID,
			TargetNode: targetNode,
			ContextID:  contextID,
			Context:    ctx,
			Timestamp:  time.Now(),
			MessageID:  dcm.generateMessageID(),
		}

		select {
		case dcm.outgoingChannel <- message:
			// 伝播記録を更新
			dcm.contextsMutex.Lock()
			if ctx, exists := dcm.contexts[contextID]; exists {
				ctx.PropagatedTo[targetNode] = true
				ctx.UpdatedAt = time.Now()
			}
			dcm.contextsMutex.Unlock()
		case <-dcm.rootCtx.Done():
			return fmt.Errorf("manager is shutting down")
		default:
			log.Printf("Outgoing message queue is full, failed to propagate to %s", targetNode)
		}
	}

	// イベントを発行
	event := ContextEvent{
		Type:      EventContextPropagated,
		ContextID: contextID,
		NodeID:    dcm.nodeID,
		Timestamp: time.Now(),
		Data: map[string]interface{}{
			"target_nodes": targets,
			"target_count": len(targets),
		},
		Source: dcm.nodeID,
	}

	select {
	case dcm.eventQueue <- event:
	case <-dcm.rootCtx.Done():
		return fmt.Errorf("manager is shutting down")
	default:
		log.Printf("Event queue is full, dropping context propagated event")
	}

	// 統計更新
	atomic.AddInt64(&dcm.stats.contextsPropagated, int64(len(targets)))

	log.Printf("Context %s propagated to %d nodes: %v", contextID, len(targets), targets)
	return nil
}

// UpdateContextはコンテキストを更新します
func (dcm *DistributedContextManager) UpdateContext(contextID string, values map[string]interface{}) error {
	dcm.contextsMutex.Lock()
	defer dcm.contextsMutex.Unlock()

	ctx, exists := dcm.contexts[contextID]
	if !exists {
		return fmt.Errorf("context not found: %s", contextID)
	}

	// 値を更新
	for key, value := range values {
		ctx.Values[key] = value
	}
	ctx.UpdatedAt = time.Now()

	// 更新をすべての伝播先ノードに送信
	for targetNode := range ctx.PropagatedTo {
		message := &ContextMessage{
			Type:       MessageUpdate,
			SourceNode: dcm.nodeID,
			TargetNode: targetNode,
			ContextID:  contextID,
			Context:    ctx,
			Data:       values,
			Timestamp:  time.Now(),
			MessageID:  dcm.generateMessageID(),
		}

		select {
		case dcm.outgoingChannel <- message:
		case <-dcm.rootCtx.Done():
			return fmt.Errorf("manager is shutting down")
		default:
			log.Printf("Failed to send update to node %s", targetNode)
		}
	}

	// イベントを発行
	event := ContextEvent{
		Type:      EventContextUpdated,
		ContextID: contextID,
		NodeID:    dcm.nodeID,
		Timestamp: time.Now(),
		Data: map[string]interface{}{
			"updated_values": values,
			"propagated_to":  len(ctx.PropagatedTo),
		},
		Source: dcm.nodeID,
	}

	select {
	case dcm.eventQueue <- event:
	default:
		log.Printf("Event queue is full, dropping context updated event")
	}

	log.Printf("Context %s updated with %d values", contextID, len(values))
	return nil
}

// CancelContextはコンテキストをキャンセルします
func (dcm *DistributedContextManager) CancelContext(contextID string) error {
	dcm.contextsMutex.Lock()
	ctx, exists := dcm.contexts[contextID]
	if !exists {
		dcm.contextsMutex.Unlock()
		return fmt.Errorf("context not found: %s", contextID)
	}

	// コンテキストを削除
	delete(dcm.contexts, contextID)
	dcm.contextsMutex.Unlock()

	// キャンセルをすべての伝播先ノードに送信
	for targetNode := range ctx.PropagatedTo {
		message := &ContextMessage{
			Type:       MessageCancel,
			SourceNode: dcm.nodeID,
			TargetNode: targetNode,
			ContextID:  contextID,
			Timestamp:  time.Now(),
			MessageID:  dcm.generateMessageID(),
		}

		select {
		case dcm.outgoingChannel <- message:
		case <-dcm.rootCtx.Done():
			return fmt.Errorf("manager is shutting down")
		default:
			log.Printf("Failed to send cancel to node %s", targetNode)
		}
	}

	// イベントを発行
	event := ContextEvent{
		Type:      EventContextCancelled,
		ContextID: contextID,
		NodeID:    dcm.nodeID,
		Timestamp: time.Now(),
		Data: map[string]interface{}{
			"lifetime":      time.Since(ctx.CreatedAt),
			"propagated_to": len(ctx.PropagatedTo),
		},
		Source: dcm.nodeID,
	}

	select {
	case dcm.eventQueue <- event:
	default:
		log.Printf("Event queue is full, dropping context cancelled event")
	}

	// 統計更新
	atomic.AddInt64(&dcm.stats.contextsCancelled, 1)

	log.Printf("Context %s cancelled", contextID)
	return nil
}

// GetContextはコンテキストを取得します
func (dcm *DistributedContextManager) GetContext(contextID string) (*DistributedContext, error) {
	dcm.contextsMutex.RLock()
	defer dcm.contextsMutex.RUnlock()

	ctx, exists := dcm.contexts[contextID]
	if !exists {
		return nil, fmt.Errorf("context not found: %s", contextID)
	}

	// コンテキストのコピーを返す
	return dcm.copyContext(ctx), nil
}

// copyContextはコンテキストのコピーを作成します
func (dcm *DistributedContextManager) copyContext(ctx *DistributedContext) *DistributedContext {
	copy := &DistributedContext{
		ID:           ctx.ID,
		ParentID:     ctx.ParentID,
		TraceID:      ctx.TraceID,
		SpanID:       ctx.SpanID,
		Values:       make(map[string]interface{}),
		Deadline:     ctx.Deadline,
		Metadata:     make(map[string]string),
		CreatedAt:    ctx.CreatedAt,
		UpdatedAt:    ctx.UpdatedAt,
		NodeIDs:      make([]string, len(ctx.NodeIDs)),
		PropagatedTo: make(map[string]bool),
	}

	// 値をコピー
	for k, v := range ctx.Values {
		copy.Values[k] = v
	}
	for k, v := range ctx.Metadata {
		copy.Metadata[k] = v
	}
	copy.NodeIDs = append(copy.NodeIDs, ctx.NodeIDs...)
	for k, v := range ctx.PropagatedTo {
		copy.PropagatedTo[k] = v
	}

	return copy
}

// JoinClusterはクラスターに参加します
func (dcm *DistributedContextManager) JoinCluster(nodeID, address string) error {
	node := &ClusterNode{
		ID:       nodeID,
		Address:  address,
		Status:   StatusActive,
		LastSeen: time.Now(),
		JoinedAt: time.Now(),
	}

	dcm.nodesMutex.Lock()
	dcm.clusterNodes[nodeID] = node
	dcm.nodesMutex.Unlock()

	// ジョインイベントを発行
	event := ContextEvent{
		Type:      EventNodeJoined,
		NodeID:    nodeID,
		Timestamp: time.Now(),
		Data: map[string]interface{}{
			"address": address,
		},
		Source: dcm.nodeID,
	}

	select {
	case dcm.eventQueue <- event:
	default:
		log.Printf("Event queue is full, dropping node joined event")
	}

	log.Printf("Node %s joined cluster at %s", nodeID, address)
	return nil
}

// LeaveClusterはクラスターから離脱します
func (dcm *DistributedContextManager) LeaveCluster(nodeID string) error {
	dcm.nodesMutex.Lock()
	delete(dcm.clusterNodes, nodeID)
	dcm.nodesMutex.Unlock()

	// 離脱イベントを発行
	event := ContextEvent{
		Type:      EventNodeLeft,
		NodeID:    nodeID,
		Timestamp: time.Now(),
		Source:    dcm.nodeID,
	}

	select {
	case dcm.eventQueue <- event:
	default:
		log.Printf("Event queue is full, dropping node left event")
	}

	log.Printf("Node %s left cluster", nodeID)
	return nil
}

// RegisterEventHandlerはイベントハンドラーを登録します
func (dcm *DistributedContextManager) RegisterEventHandler(eventType ContextEventType, handler EventHandler) {
	if dcm.eventHandlers[eventType] == nil {
		dcm.eventHandlers[eventType] = make([]EventHandler, 0)
	}
	dcm.eventHandlers[eventType] = append(dcm.eventHandlers[eventType], handler)
}

// processEventsはイベントを処理します
func (dcm *DistributedContextManager) processEvents() {
	defer dcm.wg.Done()

	log.Println("Event processor started")

	for {
		select {
		case <-dcm.rootCtx.Done():
			log.Println("Event processor stopping due to context cancellation")
			return
		case event, ok := <-dcm.eventQueue:
			if !ok {
				log.Println("Event processor stopping due to event queue closure")
				return
			}

			// 対応するハンドラーを呼び出し
			handlers, exists := dcm.eventHandlers[event.Type]
			if exists {
				for _, handler := range handlers {
					if err := handler(event); err != nil {
						log.Printf("Event handler error: %v", err)
					}
				}
			}
		}
	}
}

// handleIncomingMessagesは受信メッセージを処理します
func (dcm *DistributedContextManager) handleIncomingMessages() {
	defer dcm.wg.Done()

	log.Println("Incoming message handler started")

	for {
		select {
		case <-dcm.rootCtx.Done():
			log.Println("Incoming message handler stopping due to context cancellation")
			return
		case message, ok := <-dcm.incomingChannel:
			if !ok {
				log.Println("Incoming message handler stopping due to channel closure")
				return
			}

			dcm.processIncomingMessage(message)
			atomic.AddInt64(&dcm.stats.messagesReceived, 1)
		}
	}
}

// processIncomingMessageは受信メッセージを処理します
func (dcm *DistributedContextManager) processIncomingMessage(message *ContextMessage) {
	switch message.Type {
	case MessagePropagate:
		dcm.handlePropagateMessage(message)
	case MessageUpdate:
		dcm.handleUpdateMessage(message)
	case MessageCancel:
		dcm.handleCancelMessage(message)
	case MessageSync:
		dcm.handleSyncMessage(message)
	case MessageHeartbeat:
		dcm.handleHeartbeatMessage(message)
	default:
		log.Printf("Unknown message type: %v", message.Type)
	}
}

// handlePropagateMessageは伝播メッセージを処理します
func (dcm *DistributedContextManager) handlePropagateMessage(message *ContextMessage) {
	if message.Context == nil {
		log.Printf("Propagate message without context: %s", message.MessageID)
		return
	}

	// コンテキストを保存
	dcm.contextsMutex.Lock()
	dcm.contexts[message.ContextID] = message.Context
	dcm.contextsMutex.Unlock()

	log.Printf("Context %s propagated from node %s", message.ContextID, message.SourceNode)
}

// handleUpdateMessageは更新メッセージを処理します
func (dcm *DistributedContextManager) handleUpdateMessage(message *ContextMessage) {
	dcm.contextsMutex.Lock()
	defer dcm.contextsMutex.Unlock()

	ctx, exists := dcm.contexts[message.ContextID]
	if !exists {
		log.Printf("Update message for unknown context: %s", message.ContextID)
		return
	}

	// 値を更新
	for key, value := range message.Data {
		ctx.Values[key] = value
	}
	ctx.UpdatedAt = time.Now()

	log.Printf("Context %s updated from node %s", message.ContextID, message.SourceNode)
}

// handleCancelMessageはキャンセルメッセージを処理します
func (dcm *DistributedContextManager) handleCancelMessage(message *ContextMessage) {
	dcm.contextsMutex.Lock()
	defer dcm.contextsMutex.Unlock()

	delete(dcm.contexts, message.ContextID)

	log.Printf("Context %s cancelled by node %s", message.ContextID, message.SourceNode)
}

// handleSyncMessageは同期メッセージを処理します
func (dcm *DistributedContextManager) handleSyncMessage(message *ContextMessage) {
	// 同期処理の実装
	log.Printf("Sync message from node %s", message.SourceNode)
}

// handleHeartbeatMessageはハートビートメッセージを処理します
func (dcm *DistributedContextManager) handleHeartbeatMessage(message *ContextMessage) {
	dcm.nodesMutex.Lock()
	defer dcm.nodesMutex.Unlock()

	if node, exists := dcm.clusterNodes[message.SourceNode]; exists {
		node.LastSeen = time.Now()
		node.Status = StatusActive
	}
}

// handleOutgoingMessagesは送信メッセージを処理します
func (dcm *DistributedContextManager) handleOutgoingMessages() {
	defer dcm.wg.Done()

	log.Println("Outgoing message handler started")

	for {
		select {
		case <-dcm.rootCtx.Done():
			log.Println("Outgoing message handler stopping due to context cancellation")
			return
		case message, ok := <-dcm.outgoingChannel:
			if !ok {
				log.Println("Outgoing message handler stopping due to channel closure")
				return
			}

			// 実際のネットワーク送信をシミュレート
			dcm.simulateMessageSend(message)
			atomic.AddInt64(&dcm.stats.messagesSent, 1)
		}
	}
}

// simulateMessageSendはメッセージ送信をシミュレートします
func (dcm *DistributedContextManager) simulateMessageSend(message *ContextMessage) {
	// 実際の実装では、HTTP、gRPC、TCP等でメッセージを送信

	// ネットワーク遅延をシミュレート
	time.Sleep(time.Duration(rand.Intn(100)+10) * time.Millisecond)

	// 送信エラーをランダムに発生（5%の確率）
	if rand.Float64() < 0.05 {
		log.Printf("Failed to send message %s to node %s", message.MessageID, message.TargetNode)
		return
	}

	log.Printf("Message %s sent to node %s (type=%v)", message.MessageID, message.TargetNode, message.Type)
}

// heartbeatLoopはハートビートループを実行します
func (dcm *DistributedContextManager) heartbeatLoop() {
	defer dcm.wg.Done()

	ticker := time.NewTicker(dcm.config.HeartbeatInterval)
	defer ticker.Stop()

	log.Println("Heartbeat loop started")

	for {
		select {
		case <-dcm.rootCtx.Done():
			log.Println("Heartbeat loop stopping due to context cancellation")
			return
		case <-ticker.C:
			dcm.sendHeartbeats()
		}
	}
}

// sendHeartbeatsはハートビートを送信します
func (dcm *DistributedContextManager) sendHeartbeats() {
	dcm.nodesMutex.RLock()
	defer dcm.nodesMutex.RUnlock()

	for nodeID := range dcm.clusterNodes {
		if nodeID == dcm.nodeID {
			continue
		}

		message := &ContextMessage{
			Type:       MessageHeartbeat,
			SourceNode: dcm.nodeID,
			TargetNode: nodeID,
			Timestamp:  time.Now(),
			MessageID:  dcm.generateMessageID(),
		}

		select {
		case dcm.outgoingChannel <- message:
		case <-dcm.rootCtx.Done():
			return
		default:
			log.Printf("Failed to send heartbeat to node %s", nodeID)
		}
	}
}

// syncLoopは同期ループを実行します
func (dcm *DistributedContextManager) syncLoop() {
	defer dcm.wg.Done()

	ticker := time.NewTicker(dcm.config.SyncInterval)
	defer ticker.Stop()

	log.Println("Sync loop started")

	for {
		select {
		case <-dcm.rootCtx.Done():
			log.Println("Sync loop stopping due to context cancellation")
			return
		case <-ticker.C:
			dcm.performSync()
		}
	}
}

// performSyncは同期処理を実行します
func (dcm *DistributedContextManager) performSync() {
	// 非アクティブなノードを検出
	dcm.detectInactiveNodes()

	// コンテキスト状態の同期
	dcm.syncContextStates()
}

// detectInactiveNodesは非アクティブなノードを検出します
func (dcm *DistributedContextManager) detectInactiveNodes() {
	dcm.nodesMutex.Lock()
	defer dcm.nodesMutex.Unlock()

	threshold := time.Now().Add(-dcm.config.HeartbeatInterval * 3)

	for nodeID, node := range dcm.clusterNodes {
		if node.LastSeen.Before(threshold) && node.Status == StatusActive {
			node.Status = StatusInactive
			log.Printf("Node %s detected as inactive", nodeID)
		}
	}
}

// syncContextStatesはコンテキスト状態を同期します
func (dcm *DistributedContextManager) syncContextStates() {
	// 実際の実装では、他のノードとコンテキストリストを比較・同期
	dcm.contextsMutex.RLock()
	contextCount := len(dcm.contexts)
	dcm.contextsMutex.RUnlock()

	log.Printf("Context sync: managing %d contexts", contextCount)
}

// contextExpirationLoopはコンテキスト期限管理ループを実行します
func (dcm *DistributedContextManager) contextExpirationLoop() {
	defer dcm.wg.Done()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	log.Println("Context expiration loop started")

	for {
		select {
		case <-dcm.rootCtx.Done():
			log.Println("Context expiration loop stopping due to context cancellation")
			return
		case <-ticker.C:
			dcm.expireContexts()
		}
	}
}

// expireContextsは期限切れコンテキストを削除します
func (dcm *DistributedContextManager) expireContexts() {
	now := time.Now()
	var expired []string

	dcm.contextsMutex.Lock()
	for contextID, ctx := range dcm.contexts {
		if now.After(ctx.Deadline) {
			expired = append(expired, contextID)
			delete(dcm.contexts, contextID)
		}
	}
	dcm.contextsMutex.Unlock()

	// 期限切れイベントを発行
	for _, contextID := range expired {
		event := ContextEvent{
			Type:      EventContextExpired,
			ContextID: contextID,
			NodeID:    dcm.nodeID,
			Timestamp: now,
			Source:    dcm.nodeID,
		}

		select {
		case dcm.eventQueue <- event:
		default:
			log.Printf("Event queue is full, dropping context expired event")
		}

		atomic.AddInt64(&dcm.stats.contextsExpired, 1)
	}

	if len(expired) > 0 {
		log.Printf("Expired %d contexts", len(expired))
	}
}

// monitorMetricsはメトリクスを監視します
func (dcm *DistributedContextManager) monitorMetrics() {
	defer dcm.wg.Done()

	ticker := time.NewTicker(dcm.config.MetricsInterval)
	defer ticker.Stop()

	log.Println("Metrics monitor started")

	for {
		select {
		case <-dcm.rootCtx.Done():
			log.Println("Metrics monitor stopping")
			return
		case <-ticker.C:
			dcm.reportMetrics()
		}
	}
}

// reportMetricsはメトリクスを報告します
func (dcm *DistributedContextManager) reportMetrics() {
	dcm.stats.mu.RLock()
	defer dcm.stats.mu.RUnlock()

	created := atomic.LoadInt64(&dcm.stats.contextsCreated)
	propagated := atomic.LoadInt64(&dcm.stats.contextsPropagated)
	cancelled := atomic.LoadInt64(&dcm.stats.contextsCancelled)
	expired := atomic.LoadInt64(&dcm.stats.contextsExpired)
	sentMsgs := atomic.LoadInt64(&dcm.stats.messagesSent)
	recvMsgs := atomic.LoadInt64(&dcm.stats.messagesReceived)

	dcm.contextsMutex.RLock()
	activeContexts := len(dcm.contexts)
	dcm.contextsMutex.RUnlock()

	dcm.nodesMutex.RLock()
	activeNodes := 0
	for _, node := range dcm.clusterNodes {
		if node.Status == StatusActive {
			activeNodes++
		}
	}
	dcm.nodesMutex.RUnlock()

	uptime := time.Since(dcm.stats.startTime)

	log.Printf("Manager Metrics: Created=%d, Propagated=%d, Cancelled=%d, Expired=%d, Active=%d",
		created, propagated, cancelled, expired, activeContexts)
	log.Printf("Communication: Sent=%d, Received=%d, ActiveNodes=%d, Uptime=%v",
		sentMsgs, recvMsgs, activeNodes, uptime)
}

// デフォルトイベントハンドラー実装

func (dcm *DistributedContextManager) handleContextCreated(event ContextEvent) error {
	log.Printf("Context created event: %s on node %s", event.ContextID, event.NodeID)
	return nil
}

func (dcm *DistributedContextManager) handleContextUpdated(event ContextEvent) error {
	log.Printf("Context updated event: %s on node %s", event.ContextID, event.NodeID)
	return nil
}

func (dcm *DistributedContextManager) handleContextPropagated(event ContextEvent) error {
	log.Printf("Context propagated event: %s on node %s", event.ContextID, event.NodeID)
	return nil
}

func (dcm *DistributedContextManager) handleContextCancelled(event ContextEvent) error {
	log.Printf("Context cancelled event: %s on node %s", event.ContextID, event.NodeID)
	return nil
}

func (dcm *DistributedContextManager) handleContextExpired(event ContextEvent) error {
	log.Printf("Context expired event: %s on node %s", event.ContextID, event.NodeID)
	return nil
}

func (dcm *DistributedContextManager) handleNodeJoined(event ContextEvent) error {
	log.Printf("Node joined event: %s", event.NodeID)
	return nil
}

func (dcm *DistributedContextManager) handleNodeLeft(event ContextEvent) error {
	log.Printf("Node left event: %s", event.NodeID)
	return nil
}

// ユーティリティメソッド

// generateContextIDはコンテキストIDを生成します
func (dcm *DistributedContextManager) generateContextID() string {
	return fmt.Sprintf("ctx_%s_%d_%d", dcm.nodeID, time.Now().UnixNano(), rand.Int63())
}

// generateTraceIDはトレースIDを生成します
func (dcm *DistributedContextManager) generateTraceID() string {
	return fmt.Sprintf("trace_%d_%d", time.Now().UnixNano(), rand.Int63())
}

// generateSpanIDはスパンIDを生成します
func (dcm *DistributedContextManager) generateSpanID() string {
	return fmt.Sprintf("span_%d_%d", time.Now().UnixNano(), rand.Int63())
}

// generateMessageIDはメッセージIDを生成します
func (dcm *DistributedContextManager) generateMessageID() string {
	return fmt.Sprintf("msg_%s_%d_%d", dcm.nodeID, time.Now().UnixNano(), rand.Int63())
}

// Shutdownはマネージャーを停止します
func (dcm *DistributedContextManager) Shutdown(timeout time.Duration) error {
	if !atomic.CompareAndSwapInt32(&dcm.isRunning, 1, 0) {
		return fmt.Errorf("manager is not running")
	}

	log.Println("Shutting down distributed context manager...")

	// 1. 新しいイベントの受付を停止
	close(dcm.eventQueue)

	// 2. ワーカーの終了を待機
	done := make(chan struct{})
	go func() {
		dcm.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("All workers stopped gracefully")
	case <-time.After(timeout):
		log.Println("Timeout reached, forcing shutdown...")
		dcm.cancel()

		// 追加の待機時間
		select {
		case <-done:
			log.Println("Workers stopped after cancellation")
		case <-time.After(2 * time.Second):
			log.Println("Some workers may not have stopped properly")
		}
	}

	// 3. チャネルを閉じる
	close(dcm.incomingChannel)
	close(dcm.outgoingChannel)

	log.Println("Distributed context manager shutdown completed")
	return nil
}

// GetStatsは統計情報を取得します
func (dcm *DistributedContextManager) GetStats() map[string]interface{} {
	dcm.stats.mu.RLock()
	defer dcm.stats.mu.RUnlock()

	created := atomic.LoadInt64(&dcm.stats.contextsCreated)
	propagated := atomic.LoadInt64(&dcm.stats.contextsPropagated)
	cancelled := atomic.LoadInt64(&dcm.stats.contextsCancelled)
	expired := atomic.LoadInt64(&dcm.stats.contextsExpired)
	sentMsgs := atomic.LoadInt64(&dcm.stats.messagesSent)
	recvMsgs := atomic.LoadInt64(&dcm.stats.messagesReceived)

	dcm.contextsMutex.RLock()
	activeContexts := len(dcm.contexts)
	dcm.contextsMutex.RUnlock()

	dcm.nodesMutex.RLock()
	nodeStats := make(map[string]interface{})
	for nodeID, node := range dcm.clusterNodes {
		nodeStats[nodeID] = map[string]interface{}{
			"status":        node.Status,
			"last_seen":     node.LastSeen,
			"latency":       node.Latency,
			"context_count": node.ContextCount,
			"messages_sent": node.MessagesSent,
			"messages_recv": node.MessagesRecv,
		}
	}
	dcm.nodesMutex.RUnlock()

	uptime := time.Since(dcm.stats.startTime)

	return map[string]interface{}{
		"node_id":             dcm.nodeID,
		"contexts_created":    created,
		"contexts_propagated": propagated,
		"contexts_cancelled":  cancelled,
		"contexts_expired":    expired,
		"contexts_active":     activeContexts,
		"messages_sent":       sentMsgs,
		"messages_received":   recvMsgs,
		"uptime":              uptime,
		"cluster_size":        len(dcm.clusterNodes),
		"node_stats":          nodeStats,
	}
}

func main() {
	// 分散コンテキストマネージャー設定
	config := NewManagerConfig("node-1")
	config.ClusterSize = 3
	config.ContextTTL = 2 * time.Minute
	config.HeartbeatInterval = 3 * time.Second

	// マネージャー作成・開始
	manager := NewDistributedContextManager(config)

	// テスト用ノードをクラスターに追加
	if err := manager.JoinCluster("node-2", "192.168.1.11:8080"); err != nil {
		log.Printf("Failed to join node-2: %v", err)
	}
	if err := manager.JoinCluster("node-3", "192.168.1.12:8080"); err != nil {
		log.Printf("Failed to join node-3: %v", err)
	}

	if err := manager.Start(); err != nil {
		log.Fatalf("Failed to start manager: %v", err)
	}

	// テストコンテキストを作成・操作
	go func() {
		for i := 0; i < 100; i++ {
			// ルートコンテキストを作成
			values := map[string]interface{}{
				"user_id":    fmt.Sprintf("user_%d", rand.Intn(20)),
				"session_id": fmt.Sprintf("session_%d", rand.Intn(50)),
				"request_id": fmt.Sprintf("req_%d", i),
				"timestamp":  time.Now().Unix(),
			}

			deadline := time.Now().Add(time.Duration(rand.Intn(120)+60) * time.Second)
			ctx, err := manager.CreateContext("", values, deadline)
			if err != nil {
				log.Printf("Failed to create context: %v", err)
				continue
			}

			// コンテキストを他のノードに伝播
			if err := manager.PropagateContext(ctx.ID, nil); err != nil {
				log.Printf("Failed to propagate context %s: %v", ctx.ID, err)
			}

			// 子コンテキストを作成
			if i%3 == 0 {
				childValues := map[string]interface{}{
					"child_data": fmt.Sprintf("child_%d", i),
					"operation":  "sub_task",
				}

				childCtx, err := manager.CreateContext(ctx.ID, childValues, deadline)
				if err != nil {
					log.Printf("Failed to create child context: %v", err)
				} else {
					if err := manager.PropagateContext(childCtx.ID, []string{"node-2"}); err != nil {
						log.Printf("Failed to propagate context: %v", err)
					}
				}
			}

			// 一部のコンテキストを更新
			if i%5 == 0 {
				updateValues := map[string]interface{}{
					"updated_at": time.Now().Unix(),
					"status":     "processing",
				}
				if err := manager.UpdateContext(ctx.ID, updateValues); err != nil {
					log.Printf("Failed to update context: %v", err)
				}
			}

			// 一部のコンテキストをキャンセル
			if i%10 == 0 {
				go func(contextID string) {
					time.Sleep(time.Duration(rand.Intn(30)+10) * time.Second)
					if err := manager.CancelContext(contextID); err != nil {
						log.Printf("Failed to cancel context: %v", err)
					}
				}(ctx.ID)
			}

			time.Sleep(time.Duration(rand.Intn(500)+200) * time.Millisecond)
		}

		log.Println("All test contexts created")
	}()

	// 受信メッセージをシミュレート
	go func() {
		for i := 0; i < 50; i++ {
			// 他のノードからのメッセージをシミュレート
			message := &ContextMessage{
				Type:       MessageHeartbeat,
				SourceNode: fmt.Sprintf("node-%d", rand.Intn(3)+2),
				TargetNode: "node-1",
				Timestamp:  time.Now(),
				MessageID:  fmt.Sprintf("sim_msg_%d", i),
			}

			select {
			case manager.incomingChannel <- message:
			case <-time.After(1 * time.Second):
				log.Printf("Failed to send simulated message")
			}

			time.Sleep(time.Duration(rand.Intn(5)+2) * time.Second)
		}
	}()

	// 60秒間実行
	time.Sleep(60 * time.Second)

	// 最終統計表示
	stats := manager.GetStats()
	log.Printf("Final Stats: %+v", stats)

	// グレースフルシャットダウン
	if err := manager.Shutdown(10 * time.Second); err != nil {
		log.Printf("Shutdown error: %v", err)
	}
}
