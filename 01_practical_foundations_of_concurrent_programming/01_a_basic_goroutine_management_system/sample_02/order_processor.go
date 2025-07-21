// order_processor.go
package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
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
	processedCount int64
	errorCount     int64
	mu             sync.RWMutex
	wg             sync.WaitGroup // ワーカー管理用
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
	// 複数のワーカーGorutineを開始
	for i := 0; i < op.workerCount; i++ {
		op.wg.Add(1)
		go op.worker(ctx, i)
	}

	// 結果処理用のGoroutineを開始
	op.wg.Add(1)
	go op.resultHandler(ctx)
}

// worker 注文を処理するワーカーです
func (op *OrderProcessor) worker(ctx context.Context, workerID int) {
	defer op.wg.Done()
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

			// 結果を送信（チャネル閉鎖チェック追加）
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
	processingTime := time.Duration(rand.Intn(100)) * time.Millisecond
	time.Sleep(processingTime)

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
	defer op.wg.Done()
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

			op.mu.Lock()
			if result.Success {
				op.processedCount++
			} else {
				op.errorCount++
				log.Printf("Order processing error: %v", result.Error)
			}
			op.mu.Unlock()
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
	op.mu.RLock()
	defer op.mu.RUnlock()
	return op.processedCount, op.errorCount
}

// Shutdown システムを適切に停止します
func (op *OrderProcessor) Shutdown() {
	// 1. 新しい注文の受付を停止
	close(op.orderChan)

	// 2. ワーカーの終了を待つ（結果チャネルを閉じる前に）
	done := make(chan struct{})
	go func() {
		op.wg.Wait()
		close(done)
	}()

	// 3. タイムアウト付きでワーカー終了を待つ
	select {
	case <-done:
		// 全ワーカー終了後に結果チャネルを閉じる
		close(op.resultChan)
	case <-time.After(5 * time.Second):
		// タイムアウト時も結果チャネルを閉じる
		close(op.resultChan)
		fmt.Println("Warning: Shutdown timeout, forcing close")
	}
}

func main() {
	// 注文処理システムを作成（3ワーカー、バッファサイズ100）
	processor := NewOrderProcessor(3, 100)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// システム開始
	processor.Start(ctx)

	// 注文を生成して送信
	go func() {
		for i := 1; i <= 50; i++ {
			order := Order{
				ID:         i,
				CustomerID: fmt.Sprintf("customer-%d", rand.Intn(10)+1),
				Amount:     float64(rand.Intn(1000) + 100),
				CreatedAt:  time.Now(),
			}

			if err := processor.SubmitOrder(order); err != nil {
				log.Printf("Failed to submit order %d: %v", order.ID, err)
			}

			time.Sleep(50 * time.Millisecond)
		}
	}()

	// 統計情報を定期的に出力
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				processed, errors := processor.GetStats()
				fmt.Printf("Stats: Processed=%d, Errors=%d\n", processed, errors)
			}
		}
	}()

	// コンテキストの完了を待機
	<-ctx.Done()

	// システムを停止
	processor.Shutdown()
	fmt.Println("Order processing system stopped")
}
