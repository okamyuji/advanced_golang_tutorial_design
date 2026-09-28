package main

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

// ErrStopped 停止後のシステムに対する操作で返します
var ErrStopped = errors.New("pubsub system is stopped")

// Message パブリッシュ・サブスクライブシステムのメッセージです
type Message struct {
	ID        string    `json:"id"`
	Topic     string    `json:"topic"`
	Payload   any       `json:"payload"`
	Timestamp time.Time `json:"timestamp"`
	Sequence  uint64    `json:"sequence"`
}

// MessageHandler メッセージハンドラーのインターフェースです
type MessageHandler interface {
	Handle(msg Message) error
	Name() string
}

// PubSubConfig パブリッシュ・サブスクライブシステムの設定です
type PubSubConfig struct {
	BufferSize              int
	MaxRetries              int
	RetryDelay              time.Duration
	EnableDuplication       bool // 重複排除を有効にするか
	EnableOrdering          bool // 順序保証を有効にするか
	EnableDeliveryGuarantee bool // 配信保証を有効にするか
}

// PubSubSystem パブリッシュ・サブスクライブシステムです
type PubSubSystem struct {
	config      PubSubConfig
	topics      map[string]*Topic
	subscribers map[string]map[string]*Subscriber
	messageSeq  atomic.Uint64
	mutex       sync.RWMutex
	isRunning   atomic.Bool
	ctx         context.Context
	cancel      context.CancelFunc
	// wg トピック処理と配信の goroutine を数え、Stop がその終了を待てるようにします
	wg sync.WaitGroup
}

// Topic トピックを表します
type Topic struct {
	name        string
	messageChan chan Message
	subscribers map[string]*Subscriber
	// mutex subscribers を守ります。Publish は採番とチャネル投入をこのロック内で行い、チャネル内の順序をシーケンス順にそろえます
	mutex sync.RWMutex
}

// Subscriber サブスクライバーを表します
type Subscriber struct {
	id              string
	topic           string
	handler         MessageHandler
	messageChan     chan Message
	processedMsgs   map[string]bool    // 重複排除用
	ackChan         chan string        // 配信確認用
	pendingMessages map[string]Message // 配信保証用
	maxRetries      int
	retryDelay      time.Duration
	isRunning       atomic.Bool
	ctx             context.Context
	cancel          context.CancelFunc
	mutex           sync.RWMutex
	wg              sync.WaitGroup
}

// NewPubSubSystem 新しいパブリッシュ・サブスクライブシステムを作成します
func NewPubSubSystem(config PubSubConfig) *PubSubSystem {
	config.BufferSize = cmp.Or(config.BufferSize, 1000)
	config.MaxRetries = cmp.Or(config.MaxRetries, 3)
	config.RetryDelay = cmp.Or(config.RetryDelay, 100*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())

	return &PubSubSystem{
		config:      config,
		topics:      make(map[string]*Topic),
		subscribers: make(map[string]map[string]*Subscriber),
		ctx:         ctx,
		cancel:      cancel,
	}
}

// Start システムを開始します
func (ps *PubSubSystem) Start() error {
	if ps.ctx.Err() != nil {
		return ErrStopped
	}
	if !ps.isRunning.CompareAndSwap(false, true) {
		return errors.New("pubsub system is already running")
	}

	log.Println("Starting PubSub System...")
	return nil
}

// Stop システムを停止します
func (ps *PubSubSystem) Stop() error {
	if !ps.isRunning.CompareAndSwap(true, false) {
		return errors.New("pubsub system is not running")
	}

	log.Println("Stopping PubSub System...")

	// キャンセルと購読者の収集を ps.mutex 内で行う。CreateTopic と Subscribe は同じロック内で ctx を確かめるので、
	// ここより後に goroutine が増えることはない
	ps.mutex.Lock()
	ps.cancel()
	var subs []*Subscriber
	for _, topicSubs := range ps.subscribers {
		subs = slices.AppendSeq(subs, maps.Values(topicSubs))
	}
	ps.mutex.Unlock()

	ps.wg.Wait()

	// 全サブスクライバーを停止
	for _, sub := range subs {
		if err := sub.Stop(); err != nil {
			log.Printf("Failed to stop subscriber: %v", err)
		}
	}

	return nil
}

// CreateTopic トピックを作成します
func (ps *PubSubSystem) CreateTopic(topicName string) error {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()

	if ps.ctx.Err() != nil {
		return ErrStopped
	}

	if _, exists := ps.topics[topicName]; exists {
		return fmt.Errorf("topic %s already exists", topicName)
	}

	topic := &Topic{
		name:        topicName,
		messageChan: make(chan Message, ps.config.BufferSize),
		subscribers: make(map[string]*Subscriber),
	}

	ps.topics[topicName] = topic
	ps.subscribers[topicName] = make(map[string]*Subscriber)

	// トピック処理ゴルーチンを開始
	ps.wg.Go(func() { ps.processTopicMessages(topic) })

	return nil
}

// Subscribe トピックをサブスクライブします
func (ps *PubSubSystem) Subscribe(topicName, subscriberID string, handler MessageHandler) (*Subscriber, error) {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()

	if ps.ctx.Err() != nil {
		return nil, ErrStopped
	}

	topic, exists := ps.topics[topicName]
	if !exists {
		return nil, fmt.Errorf("topic %s does not exist", topicName)
	}

	if _, exists := ps.subscribers[topicName][subscriberID]; exists {
		return nil, fmt.Errorf("subscriber %s already exists for topic %s", subscriberID, topicName)
	}

	ctx, cancel := context.WithCancel(ps.ctx)

	subscriber := &Subscriber{
		id:              subscriberID,
		topic:           topicName,
		handler:         handler,
		messageChan:     make(chan Message, ps.config.BufferSize),
		processedMsgs:   make(map[string]bool),
		ackChan:         make(chan string, ps.config.BufferSize),
		pendingMessages: make(map[string]Message),
		maxRetries:      ps.config.MaxRetries,
		retryDelay:      ps.config.RetryDelay,
		ctx:             ctx,
		cancel:          cancel,
	}

	// topic.subscribers はトピック処理 goroutine が topic.mutex の下で読む
	topic.mutex.Lock()
	topic.subscribers[subscriberID] = subscriber
	topic.mutex.Unlock()
	ps.subscribers[topicName][subscriberID] = subscriber

	// サブスクライバー処理ゴルーチンを開始
	if err := subscriber.Start(); err != nil {
		return nil, err
	}

	return subscriber, nil
}

// Publish メッセージをパブリッシュします
func (ps *PubSubSystem) Publish(topicName string, payload any) error {
	if ps.ctx.Err() != nil {
		return ErrStopped
	}

	ps.mutex.RLock()
	topic, exists := ps.topics[topicName]
	ps.mutex.RUnlock()

	if !exists {
		return fmt.Errorf("topic %s does not exist", topicName)
	}

	_, err := topic.enqueue(func() Message {
		sequence := ps.messageSeq.Add(1)
		return Message{
			ID:        fmt.Sprintf("msg-%d", sequence),
			Topic:     topicName,
			Payload:   payload,
			Timestamp: time.Now(),
			Sequence:  sequence,
		}
	})
	return err
}

// enqueue 採番とチャネルへの投入を同じロック内で行い、並行に Publish されてもチャネル内がシーケンス順になるようにします
func (t *Topic) enqueue(newMessage func() Message) (Message, error) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	message := newMessage()
	select {
	case t.messageChan <- message:
		return message, nil
	default:
		return Message{}, fmt.Errorf("topic %s message channel is full", t.name)
	}
}

// processTopicMessages トピックメッセージを処理します
func (ps *PubSubSystem) processTopicMessages(topic *Topic) {
	for {
		select {
		case <-ps.ctx.Done():
			return
		case message := <-topic.messageChan:
			ps.distributeMessage(topic, message)
		}
	}
}

// distributeMessage メッセージを配信します
func (ps *PubSubSystem) distributeMessage(topic *Topic, message Message) {
	topic.mutex.RLock()
	subscribers := slices.Collect(maps.Values(topic.subscribers))
	topic.mutex.RUnlock()

	if ps.config.EnableOrdering {
		// 順序保証：トピックのチャネルはシーケンス順なので、この goroutine から順に各サブスクライバーのキューへ積む。
		// サブスクライバーは自分のキューを1件ずつ処理するので、受け取る順序もシーケンス順になる
		for _, subscriber := range subscribers {
			ps.deliverToSubscriber(subscriber, message)
		}
		return
	}

	// 並列配信
	for _, subscriber := range subscribers {
		ps.wg.Go(func() { ps.deliverToSubscriber(subscriber, message) })
	}
}

// deliverToSubscriber サブスクライバーにメッセージを配信します
func (ps *PubSubSystem) deliverToSubscriber(subscriber *Subscriber, message Message) {
	// 重複排除チェック
	if ps.config.EnableDuplication {
		subscriber.mutex.RLock()
		if subscriber.processedMsgs[message.ID] {
			subscriber.mutex.RUnlock()
			return // 既に処理済み
		}
		subscriber.mutex.RUnlock()
	}

	// 配信保証：送信より先にペンディングへ追加する。送信後に追加すると、先に届いた ACK の削除と入れ違いになり、
	// 処理済みのメッセージがペンディングに残る
	if ps.config.EnableDeliveryGuarantee {
		subscriber.mutex.Lock()
		subscriber.pendingMessages[message.ID] = message
		subscriber.mutex.Unlock()
	}

	select {
	case subscriber.messageChan <- message:
	default:
		log.Printf("Subscriber %s message channel is full", subscriber.id)
		if ps.config.EnableDeliveryGuarantee {
			subscriber.mutex.Lock()
			delete(subscriber.pendingMessages, message.ID)
			subscriber.mutex.Unlock()
		}
	}
}

// Start サブスクライバーを開始します
func (s *Subscriber) Start() error {
	if !s.isRunning.CompareAndSwap(false, true) {
		return fmt.Errorf("subscriber %s is already running", s.id)
	}

	s.wg.Go(s.processMessages)
	s.wg.Go(s.processAcknowledgments)

	return nil
}

// Stop サブスクライバーを停止し、処理中の goroutine が終わるまで待ちます
func (s *Subscriber) Stop() error {
	if !s.isRunning.CompareAndSwap(true, false) {
		return fmt.Errorf("subscriber %s is not running", s.id)
	}

	s.cancel()
	s.wg.Wait()
	return nil
}

// processMessages メッセージを処理します
func (s *Subscriber) processMessages() {
	for {
		select {
		case <-s.ctx.Done():
			return
		case message := <-s.messageChan:
			s.handleMessage(message)
		}
	}
}

// handleMessage メッセージを処理します
func (s *Subscriber) handleMessage(message Message) {
	// 重複排除
	s.mutex.Lock()
	if s.processedMsgs[message.ID] {
		s.mutex.Unlock()
		return
	}
	s.processedMsgs[message.ID] = true
	s.mutex.Unlock()

	// ハンドラー実行。失敗したら retryDelay 空けて maxRetries 回まで再試行する
	var err error
	for attempt := range s.maxRetries + 1 {
		if attempt > 0 {
			select {
			case <-s.ctx.Done():
				return
			case <-time.After(s.retryDelay):
			}
		}
		if err = s.handler.Handle(message); err == nil {
			break
		}
		log.Printf("Error handling message %s (attempt %d/%d): %v", message.ID, attempt+1, s.maxRetries+1, err)
	}
	if err != nil {
		// ACK を返さないので、メッセージはペンディングに残る
		return
	}

	// 配信確認
	select {
	case s.ackChan <- message.ID:
	default:
		log.Printf("ACK channel is full for subscriber %s", s.id)
	}
}

// processAcknowledgments 配信確認を処理します
func (s *Subscriber) processAcknowledgments() {
	for {
		select {
		case <-s.ctx.Done():
			return
		case messageID := <-s.ackChan:
			s.mutex.Lock()
			delete(s.pendingMessages, messageID)
			s.mutex.Unlock()
		}
	}
}

// GetProcessedMessages 処理済みメッセージIDリストを取得します
func (s *Subscriber) GetProcessedMessages() []string {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	return slices.Collect(maps.Keys(s.processedMsgs))
}

// GetPendingMessages ペンディングメッセージを取得します
func (s *Subscriber) GetPendingMessages() []Message {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	return slices.Collect(maps.Values(s.pendingMessages))
}

// TestMessageHandler テスト用のメッセージハンドラーです
type TestMessageHandler struct {
	name            string
	processedMsgs   []Message
	processingDelay time.Duration
	shouldFail      atomic.Bool
	mutex           sync.Mutex
}

func NewTestMessageHandler(name string, delay time.Duration) *TestMessageHandler {
	return &TestMessageHandler{
		name:            name,
		processedMsgs:   make([]Message, 0),
		processingDelay: delay,
	}
}

func (tmh *TestMessageHandler) Name() string {
	return tmh.name
}

func (tmh *TestMessageHandler) Handle(msg Message) error {
	// 処理遅延をシミュレート。ロックを持ったまま眠ると、synctest ではロック待ちの goroutine が
	// 「durably blocked」とみなされず、偽の時計が進まなくなる
	if tmh.processingDelay > 0 {
		time.Sleep(tmh.processingDelay)
	}

	// 失敗をシミュレート
	if tmh.shouldFail.Load() {
		return errors.New("simulated failure")
	}

	tmh.mutex.Lock()
	defer tmh.mutex.Unlock()
	tmh.processedMsgs = append(tmh.processedMsgs, msg)
	return nil
}

func (tmh *TestMessageHandler) GetProcessedMessages() []Message {
	tmh.mutex.Lock()
	defer tmh.mutex.Unlock()

	return slices.Clone(tmh.processedMsgs)
}

func (tmh *TestMessageHandler) SetShouldFail(shouldFail bool) {
	tmh.shouldFail.Store(shouldFail)
}

// main関数（テスト実行用）
func main() {
	// 通常のGo testコマンドでテストを実行
	log.Println("=== PubSub System with testing/synctest Demo ===")
	log.Println("Run tests with: go test -v")
	log.Println("Tests include:")
	log.Println("- Basic functionality")
	log.Println("- Message ordering")
	log.Println("- Duplicate elimination")
	log.Println("- Delivery guarantee")
	log.Println("- Concurrent publish/subscribe")

	// 簡単なデモ実行
	config := PubSubConfig{
		BufferSize:              100,
		EnableDuplication:       true,
		EnableOrdering:          true,
		EnableDeliveryGuarantee: true,
	}

	pubsub := NewPubSubSystem(config)
	defer func() {
		if err := pubsub.Stop(); err != nil {
			log.Printf("Failed to stop pubsub: %v", err)
		}
	}()

	if err := pubsub.Start(); err != nil {
		log.Fatalf("Failed to start pubsub system: %v", err)
	}

	topicName := "demo-topic"
	if err := pubsub.CreateTopic(topicName); err != nil {
		log.Fatalf("Failed to create topic: %v", err)
	}

	handler := NewTestMessageHandler("demo-handler", 10*time.Millisecond)
	_, err := pubsub.Subscribe(topicName, "demo-subscriber", handler)
	if err != nil {
		log.Fatalf("Failed to subscribe: %v", err)
	}

	// デモメッセージ送信
	for i := range 10 {
		payload := fmt.Sprintf("demo-message-%d", i)
		if err := pubsub.Publish(topicName, payload); err != nil {
			log.Printf("Failed to publish message %d: %v", i, err)
		}
		time.Sleep(50 * time.Millisecond)
	}

	// 処理完了待機
	time.Sleep(200 * time.Millisecond)

	// 結果表示
	processed := handler.GetProcessedMessages()
	log.Printf("Demo completed: processed %d messages", len(processed))
	for i, msg := range processed {
		log.Printf("Message %d: ID=%s, Seq=%d, Payload=%v",
			i, msg.ID, msg.Sequence, msg.Payload)
	}
}
