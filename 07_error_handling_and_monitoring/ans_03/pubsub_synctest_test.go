package main

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

// newStartedPubSub バブル内でシステムを作って開始し、テスト終了時に停止します
// synctest では ctx のチャネルもバブル内で作る必要があるので、NewPubSubSystem は必ず synctest.Test の中で呼びます。
func newStartedPubSub(t *testing.T, config PubSubConfig) *PubSubSystem {
	t.Helper()
	pubsub := NewPubSubSystem(config)
	if err := pubsub.Start(); err != nil {
		t.Fatalf("Failed to start pubsub system: %v", err)
	}
	t.Cleanup(func() {
		if err := pubsub.Stop(); err != nil {
			t.Errorf("Failed to stop pubsub: %v", err)
		}
	})
	return pubsub
}

func TestPubSubSystem_BasicFunctionality(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pubsub := newStartedPubSub(t, PubSubConfig{
			BufferSize:              100,
			MaxRetries:              3,
			RetryDelay:              10 * time.Millisecond,
			EnableDuplication:       true,
			EnableOrdering:          false,
			EnableDeliveryGuarantee: true,
		})

		// トピック作成
		topicName := "test-topic"
		if err := pubsub.CreateTopic(topicName); err != nil {
			t.Fatalf("Failed to create topic: %v", err)
		}

		// サブスクライバー作成
		handler1 := NewTestMessageHandler("handler-1", 1*time.Millisecond)
		handler2 := NewTestMessageHandler("handler-2", 1*time.Millisecond)

		sub1, err := pubsub.Subscribe(topicName, "subscriber-1", handler1)
		if err != nil {
			t.Fatalf("Failed to subscribe: %v", err)
		}

		sub2, err := pubsub.Subscribe(topicName, "subscriber-2", handler2)
		if err != nil {
			t.Fatalf("Failed to subscribe: %v", err)
		}

		// メッセージ送信
		const numMessages = 10
		for i := range numMessages {
			payload := fmt.Sprintf("message-%d", i)
			if err := pubsub.Publish(topicName, payload); err != nil {
				t.Errorf("Failed to publish message %d: %v", i, err)
			}
		}

		// メッセージ処理を待機
		synctest.Sleep(100 * time.Millisecond)

		// 結果検証
		if got := len(handler1.GetProcessedMessages()); got != numMessages {
			t.Errorf("Handler1 expected %d messages, got %d", numMessages, got)
		}

		if got := len(handler2.GetProcessedMessages()); got != numMessages {
			t.Errorf("Handler2 expected %d messages, got %d", numMessages, got)
		}

		// 重複チェック
		if got := len(sub1.GetProcessedMessages()); got != numMessages {
			t.Errorf("Subscriber1 expected %d processed messages, got %d", numMessages, got)
		}

		// ペンディングメッセージチェック（配信保証）
		if got := len(sub1.GetPendingMessages()); got > 0 {
			t.Errorf("Subscriber1 should have no pending messages, got %d", got)
		}

		if got := len(sub2.GetPendingMessages()); got > 0 {
			t.Errorf("Subscriber2 should have no pending messages, got %d", got)
		}
	})
}

func TestPubSubSystem_MessageOrdering(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pubsub := newStartedPubSub(t, PubSubConfig{
			BufferSize:              100,
			EnableDuplication:       true,
			EnableOrdering:          true, // 順序保証を有効
			EnableDeliveryGuarantee: true,
		})

		topicName := "ordered-topic"
		if err := pubsub.CreateTopic(topicName); err != nil {
			t.Fatalf("Failed to create topic: %v", err)
		}

		handler := NewTestMessageHandler("ordered-handler", 2*time.Millisecond)
		if _, err := pubsub.Subscribe(topicName, "ordered-subscriber", handler); err != nil {
			t.Fatalf("Failed to subscribe: %v", err)
		}

		// 並行でメッセージを送信
		const numMessages = 20
		var wg sync.WaitGroup

		for i := range numMessages {
			wg.Go(func() {
				payload := fmt.Sprintf("ordered-message-%d", i)
				if err := pubsub.Publish(topicName, payload); err != nil {
					t.Errorf("Failed to publish message: %v", err)
				}
			})
		}

		wg.Wait()

		// メッセージ処理を待機
		synctest.Sleep(200 * time.Millisecond)

		// 順序確認
		processedMsgs := handler.GetProcessedMessages()
		if len(processedMsgs) != numMessages {
			t.Fatalf("Expected %d messages, got %d", numMessages, len(processedMsgs))
		}

		// シーケンス番号が連続しているかチェック
		for i := range len(processedMsgs) - 1 {
			if processedMsgs[i+1].Sequence != processedMsgs[i].Sequence+1 {
				t.Errorf("Message order violation: seq %d followed by seq %d",
					processedMsgs[i].Sequence, processedMsgs[i+1].Sequence)
			}
		}
	})
}

func TestPubSubSystem_DuplicateElimination(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pubsub := newStartedPubSub(t, PubSubConfig{
			BufferSize:              100,
			EnableDuplication:       true, // 重複排除を有効
			EnableOrdering:          false,
			EnableDeliveryGuarantee: true,
		})

		topicName := "dedup-topic"
		if err := pubsub.CreateTopic(topicName); err != nil {
			t.Fatalf("Failed to create topic: %v", err)
		}

		handler := NewTestMessageHandler("dedup-handler", 1*time.Millisecond)
		subscriber, err := pubsub.Subscribe(topicName, "dedup-subscriber", handler)
		if err != nil {
			t.Fatalf("Failed to subscribe: %v", err)
		}

		// 同じメッセージ（同じ ID）を複数回配信して、再送による重複をシミュレートする
		msg := Message{ID: "dup-1", Topic: topicName, Payload: "duplicate-message", Sequence: 1}
		const duplicates = 5
		for range duplicates {
			pubsub.deliverToSubscriber(subscriber, msg)
		}

		// 処理待機
		synctest.Sleep(50 * time.Millisecond)

		// 同じ ID は1回だけ処理される
		if got := len(handler.GetProcessedMessages()); got != 1 {
			t.Errorf("Expected duplicate message to be handled once, got %d", got)
		}

		if got := len(subscriber.GetProcessedMessages()); got != 1 {
			t.Errorf("Expected 1 processed message ID, got %d", got)
		}

		// 処理済みなのでペンディングにも残らない
		if got := len(subscriber.GetPendingMessages()); got != 0 {
			t.Errorf("Expected no pending messages, got %d", got)
		}
	})
}

func TestPubSubSystem_DeliveryGuarantee(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pubsub := newStartedPubSub(t, PubSubConfig{
			BufferSize:              100,
			MaxRetries:              3,
			RetryDelay:              10 * time.Millisecond,
			EnableDuplication:       true,
			EnableOrdering:          false,
			EnableDeliveryGuarantee: true, // 配信保証を有効
		})

		topicName := "delivery-topic"
		if err := pubsub.CreateTopic(topicName); err != nil {
			t.Fatalf("Failed to create topic: %v", err)
		}

		// 失敗するハンドラーを作成
		failingHandler := NewTestMessageHandler("failing-handler", 1*time.Millisecond)
		failingHandler.SetShouldFail(true)

		subscriber, err := pubsub.Subscribe(topicName, "delivery-subscriber", failingHandler)
		if err != nil {
			t.Fatalf("Failed to subscribe: %v", err)
		}

		// メッセージ送信
		const numMessages = 5
		for i := range numMessages {
			payload := fmt.Sprintf("delivery-message-%d", i)
			if err := pubsub.Publish(topicName, payload); err != nil {
				t.Errorf("Failed to publish message %d: %v", i, err)
			}
		}

		// 最初のメッセージの処理が始まるところまで進める
		synctest.Wait()

		// ペンディングメッセージの確認（処理に成功していないので確認されていない）
		if got := len(subscriber.GetPendingMessages()); got != numMessages {
			t.Errorf("Expected %d pending messages, got %d", numMessages, got)
		}

		// 再試行の途中でハンドラーが回復すれば、全メッセージが処理されてペンディングが空になる
		failingHandler.SetShouldFail(false)
		synctest.Sleep(1 * time.Second)

		if got := len(failingHandler.GetProcessedMessages()); got != numMessages {
			t.Errorf("Expected %d messages processed after recovery, got %d", numMessages, got)
		}
		if got := len(subscriber.GetPendingMessages()); got != 0 {
			t.Errorf("Expected no pending messages after recovery, got %d", got)
		}
	})
}

func TestPubSubSystem_RetryExhaustedKeepsPending(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pubsub := newStartedPubSub(t, PubSubConfig{
			BufferSize:              100,
			MaxRetries:              2,
			RetryDelay:              10 * time.Millisecond,
			EnableDeliveryGuarantee: true,
		})

		topicName := "exhausted-topic"
		if err := pubsub.CreateTopic(topicName); err != nil {
			t.Fatalf("Failed to create topic: %v", err)
		}

		handler := NewTestMessageHandler("always-failing", 1*time.Millisecond)
		handler.SetShouldFail(true)
		subscriber, err := pubsub.Subscribe(topicName, "exhausted-subscriber", handler)
		if err != nil {
			t.Fatalf("Failed to subscribe: %v", err)
		}

		const numMessages = 3
		for i := range numMessages {
			if err := pubsub.Publish(topicName, i); err != nil {
				t.Fatalf("Failed to publish message %d: %v", i, err)
			}
		}

		// 再試行をすべて使い切るまで進める
		synctest.Sleep(1 * time.Second)

		if got := len(handler.GetProcessedMessages()); got != 0 {
			t.Errorf("Expected no processed messages, got %d", got)
		}
		if got := len(subscriber.GetPendingMessages()); got != numMessages {
			t.Errorf("Expected %d pending messages after retries are exhausted, got %d", numMessages, got)
		}
	})
}

func TestPubSubSystem_ConcurrentPublishSubscribe(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pubsub := newStartedPubSub(t, PubSubConfig{
			BufferSize:              1000,
			EnableDuplication:       true,
			EnableOrdering:          false,
			EnableDeliveryGuarantee: true,
		})

		topicName := "concurrent-topic"
		if err := pubsub.CreateTopic(topicName); err != nil {
			t.Fatalf("Failed to create topic: %v", err)
		}

		// 複数のサブスクライバーを作成
		const numSubscribers = 5
		const numMessages = 100

		handlers := make([]*TestMessageHandler, numSubscribers)
		subscribers := make([]*Subscriber, numSubscribers)

		for i := range numSubscribers {
			handler := NewTestMessageHandler(fmt.Sprintf("handler-%d", i), 1*time.Millisecond)
			subscriber, err := pubsub.Subscribe(topicName, fmt.Sprintf("subscriber-%d", i), handler)
			if err != nil {
				t.Fatalf("Failed to create subscriber %d: %v", i, err)
			}
			handlers[i] = handler
			subscribers[i] = subscriber
		}

		// 並行パブリッシュ
		var publishWg sync.WaitGroup
		for i := range numMessages {
			publishWg.Go(func() {
				payload := fmt.Sprintf("concurrent-message-%d", i)
				if err := pubsub.Publish(topicName, payload); err != nil {
					t.Errorf("Failed to publish message %d: %v", i, err)
				}
			})
		}

		publishWg.Wait()

		// 処理完了を待機
		synctest.Sleep(500 * time.Millisecond)

		// 全サブスクライバーが全メッセージを受信したことを確認
		for i, handler := range handlers {
			if got := len(handler.GetProcessedMessages()); got != numMessages {
				t.Errorf("Subscriber %d expected %d messages, got %d", i, numMessages, got)
			}

			// 重複チェック
			uniqueIDs := make(map[string]bool)
			for _, id := range subscribers[i].GetProcessedMessages() {
				if uniqueIDs[id] {
					t.Errorf("Subscriber %d found duplicate message ID: %s", i, id)
				}
				uniqueIDs[id] = true
			}
		}
	})
}

func TestPubSubSystem_PublishDuringStop(t *testing.T) {
	for range 50 {
		pubsub := NewPubSubSystem(PubSubConfig{BufferSize: 10, EnableDeliveryGuarantee: true})
		if err := pubsub.Start(); err != nil {
			t.Fatalf("Failed to start pubsub system: %v", err)
		}
		if err := pubsub.CreateTopic("topic"); err != nil {
			t.Fatalf("Failed to create topic: %v", err)
		}
		if _, err := pubsub.Subscribe("topic", "sub", NewTestMessageHandler("h", 0)); err != nil {
			t.Fatalf("Failed to subscribe: %v", err)
		}

		var wg sync.WaitGroup
		wg.Go(func() {
			for i := range 20 {
				_ = pubsub.Publish("topic", i)
			}
		})
		wg.Go(func() {
			if err := pubsub.Stop(); err != nil {
				t.Errorf("Failed to stop pubsub: %v", err)
			}
		})
		wg.Wait()

		if err := pubsub.Publish("topic", "late"); !errors.Is(err, ErrStopped) {
			t.Fatalf("Expected ErrStopped from Publish after stop, got %v", err)
		}
		if err := pubsub.CreateTopic("late-topic"); !errors.Is(err, ErrStopped) {
			t.Fatalf("Expected ErrStopped from CreateTopic after stop, got %v", err)
		}
		if _, err := pubsub.Subscribe("topic", "late-sub", NewTestMessageHandler("h", 0)); !errors.Is(err, ErrStopped) {
			t.Fatalf("Expected ErrStopped from Subscribe after stop, got %v", err)
		}
		if err := pubsub.Start(); !errors.Is(err, ErrStopped) {
			t.Fatalf("Expected ErrStopped from Start after stop, got %v", err)
		}
	}
}
