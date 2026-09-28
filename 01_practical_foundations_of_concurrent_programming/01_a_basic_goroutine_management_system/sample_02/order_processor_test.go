package main

import (
	"testing"
	"testing/synctest"
	"time"
)

func newTestOrder(id int) Order {
	return Order{
		ID:         id,
		CustomerID: "test-customer",
		Amount:     100.0,
		CreatedAt:  time.Now(),
	}
}

func TestOrderProcessor_SubmitOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// ワーカーを起動する前に投入し、消費と競合しない状態でバッファ上限を確かめる
		processor := NewOrderProcessor(2, 10)

		for i := range 10 {
			if err := processor.SubmitOrder(newTestOrder(i + 1)); err != nil {
				t.Fatalf("order %d: expected no error, got %v", i+1, err)
			}
		}

		// 異常系: バッファ満杯時の送信は待たずにエラーを返す
		if err := processor.SubmitOrder(newTestOrder(11)); err == nil {
			t.Error("Expected error for full buffer, got nil")
		}

		// 正常系: 起動後はキューに溜まった注文がすべて処理される
		processor.Start(t.Context())
		processor.Shutdown()

		processed, errors := processor.GetStats()
		if processed+errors != 10 {
			t.Errorf("Expected total processed+errors = 10, got %d", processed+errors)
		}
	})
}

func TestOrderProcessor_GetStats(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		processor := NewOrderProcessor(1, 5)
		processor.Start(t.Context())

		// 初期状態の確認
		processed, errors := processor.GetStats()
		if processed != 0 || errors != 0 {
			t.Errorf("Expected initial stats (0, 0), got (%d, %d)", processed, errors)
		}

		for i := range 5 {
			if err := processor.SubmitOrder(newTestOrder(i + 1)); err != nil {
				t.Fatalf("Failed to submit order: %v", err)
			}
		}

		// 仮想時計を1秒進め、全goroutineが待機状態に入るまで待つ（実時間は経過しない）
		synctest.Sleep(time.Second)

		processed, errors = processor.GetStats()
		if processed+errors != 5 {
			t.Errorf("Expected total processed+errors = 5, got %d", processed+errors)
		}

		processor.Shutdown()
	})
}
