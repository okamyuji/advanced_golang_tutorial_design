package main

import (
	"context"
	"testing"
	"time"
)

func TestOrderProcessor_SubmitOrder(t *testing.T) {
	processor := NewOrderProcessor(2, 10)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	processor.Start(ctx)

	// 正常系: 注文の送信
	order := Order{
		ID:         1,
		CustomerID: "test-customer",
		Amount:     100.0,
		CreatedAt:  time.Now(),
	}

	err := processor.SubmitOrder(order)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// バッファが満杯になるまで注文を送信
	for i := 2; i <= 11; i++ {
		order.ID = i
		if err := processor.SubmitOrder(order); err != nil {
			t.Logf("Failed to submit order %d: %v", i, err)
		}
	}

	// 異常系: バッファ満杯時の送信
	order.ID = 12
	err = processor.SubmitOrder(order)
	if err == nil {
		t.Error("Expected error for full buffer, got nil")
	}

	processor.Shutdown()
}

func TestOrderProcessor_GetStats(t *testing.T) {
	processor := NewOrderProcessor(1, 5)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	processor.Start(ctx)

	// 初期状態の確認
	processed, errors := processor.GetStats()
	if processed != 0 || errors != 0 {
		t.Errorf("Expected initial stats (0, 0), got (%d, %d)", processed, errors)
	}

	// 注文を送信
	for i := 1; i <= 5; i++ {
		order := Order{
			ID:         i,
			CustomerID: "test-customer",
			Amount:     100.0,
			CreatedAt:  time.Now(),
		}
		if err := processor.SubmitOrder(order); err != nil {
			t.Logf("Failed to submit order: %v", err)
		}
	}

	// 処理完了を待つ
	time.Sleep(1 * time.Second)

	// 統計の確認
	processed, errors = processor.GetStats()
	if processed+errors != 5 {
		t.Errorf("Expected total processed+errors = 5, got %d", processed+errors)
	}

	processor.Shutdown()
}
