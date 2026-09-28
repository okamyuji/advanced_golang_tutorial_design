// order_processor.go
package main

import (
	"context"
	"fmt"
	"log"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"
)

// Order 注文情報を表現します
type Order struct {
	ID         int
	CustomerID string
	Amount     float64
	CreatedAt  time.Time
}

// OrderProcessor 注文処理システムです
type OrderProcessor struct {
	orderChan      chan Order
	resultChan     chan ProcessResult
	workerCount    int
	processedCount atomic.Int64
	errorCount     atomic.Int64
	// resultChanを閉じる時点を決めるため、ワーカーと結果ハンドラを別々に待つ
	workers sync.WaitGroup
	handler sync.WaitGroup
}

// ProcessResult 処理結果を表現します
type ProcessResult struct {
	OrderID     int
	Success     bool
	Error       error
	ProcessedAt time.Time
}

// NewOrderProcessor 新しい注文処理システムを作成します
func NewOrderProcessor(workerCount int, bufferSize int) *OrderProcessor {
	return &OrderProcessor{
		orderChan:   make(chan Order, bufferSize),
		resultChan:  make(chan ProcessResult, bufferSize),
		workerCount: workerCount,
	}
}

// Start 注文処理システムを開始します
func (op *OrderProcessor) Start(ctx context.Context) {
	// 複数のワーカーGoroutineを開始
	for i := range op.workerCount {
		op.workers.Go(func() { op.worker(ctx, i) })
	}

	// 結果処理用のGoroutineを開始
	op.handler.Go(func() { op.resultHandler(ctx) })
}

// worker 注文を処理するワーカーです
func (op *OrderProcessor) worker(ctx context.Context, workerID int) {
	for {
		select {
		case <-ctx.Done():
			fmt.Printf("Worker %d stopping\n", workerID)
			return
		case order, ok := <-op.orderChan:
			if !ok {
				fmt.Printf("Worker %d: order channel closed\n", workerID)
				return
			}

			// 注文を処理
			result := op.processOrder(order, workerID)

			// 結果を送信する。結果チャネルが満杯なら捨てて次の注文へ進む
			select {
			case op.resultChan <- result:
			case <-ctx.Done():
				return
			default:
				fmt.Printf("Warning: result channel full, dropping result for order %d\n", order.ID)
			}
		}
	}
}

// processOrder 個別の注文を処理します
func (op *OrderProcessor) processOrder(order Order, workerID int) ProcessResult {
	// シミュレーション用の処理時間
	time.Sleep(rand.N(100 * time.Millisecond))

	// ランダムにエラーを発生させる（10%の確率）
	var err error
	success := true
	if rand.Float32() < 0.1 {
		err = fmt.Errorf("processing failed for order %d", order.ID)
		success = false
	}

	fmt.Printf("Worker %d processed order %d (success: %v)\n", workerID, order.ID, success)

	return ProcessResult{
		OrderID:     order.ID,
		Success:     success,
		Error:       err,
		ProcessedAt: time.Now(),
	}
}

// resultHandler 処理結果を処理します
func (op *OrderProcessor) resultHandler(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			fmt.Println("Result handler stopping")
			return
		case result, ok := <-op.resultChan:
			if !ok {
				fmt.Println("Result channel closed")
				return
			}

			if result.Success {
				op.processedCount.Add(1)
			} else {
				op.errorCount.Add(1)
				log.Printf("Order processing error: %v", result.Error)
			}
		}
	}
}

// SubmitOrder 新しい注文を送信します
func (op *OrderProcessor) SubmitOrder(order Order) error {
	select {
	case op.orderChan <- order:
		return nil
	default:
		return fmt.Errorf("order queue is full")
	}
}

// GetStats 処理統計を取得します
func (op *OrderProcessor) GetStats() (int64, int64) {
	return op.processedCount.Load(), op.errorCount.Load()
}

// Shutdown システムを適切に停止します
func (op *OrderProcessor) Shutdown() {
	// 1. 新しい注文の受付を停止し、ワーカーがキューを処理し終えるのを待つ
	close(op.orderChan)
	op.workers.Wait()

	// 2. 送信側のワーカーがいなくなってから結果チャネルを閉じる
	close(op.resultChan)
	op.handler.Wait()
}

func main() {
	// 注文処理システムを作成（3ワーカー、バッファサイズ100）
	processor := NewOrderProcessor(3, 100)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// システム開始
	processor.Start(ctx)

	// 注文を生成して送信（Shutdownで閉じたチャネルへ送らないよう、終了を待てる形で起動する）
	var producer sync.WaitGroup
	producer.Go(func() {
		for i := range 50 {
			order := Order{
				ID:         i + 1,
				CustomerID: fmt.Sprintf("customer-%d", rand.IntN(10)+1),
				Amount:     float64(rand.IntN(1000) + 100),
				CreatedAt:  time.Now(),
			}

			if err := processor.SubmitOrder(order); err != nil {
				log.Printf("Failed to submit order %d: %v", order.ID, err)
			}

			select {
			case <-ctx.Done():
				return
			case <-time.After(50 * time.Millisecond):
			}
		}
	})

	// 統計情報を定期的に出力
	go func() {
		tick := time.Tick(2 * time.Second)

		for {
			select {
			case <-ctx.Done():
				return
			case <-tick:
				processed, errors := processor.GetStats()
				fmt.Printf("Stats: Processed=%d, Errors=%d\n", processed, errors)
			}
		}
	}()

	// コンテキストの完了と注文生成の終了を待機
	<-ctx.Done()
	producer.Wait()

	// システムを停止
	processor.Shutdown()
	fmt.Println("Order processing system stopped")
}
