package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	_ "github.com/lib/pq"
)

// 楽観的ロック付き在庫管理システム
// 在庫の整合性を保ちながら並行処理を実現

// InventoryItem 在庫アイテムを表現
type InventoryItem struct {
	ID        int64     `json:"id"`
	Name      string    `json:"name"`
	Quantity  int32     `json:"quantity"`
	Version   int32     `json:"version"` // 楽観的ロック用バージョン
	UpdatedAt time.Time `json:"updated_at"`
}

// OrderRequest 注文リクエストを表現
type OrderRequest struct {
	ItemID   int64  `json:"item_id"`
	Quantity int32  `json:"quantity"`
	OrderID  string `json:"order_id"`
}

// InventoryManager 在庫管理システム
type InventoryManager struct {
	db         *sql.DB
	maxRetries int
	retryDelay time.Duration
	metrics    *InventoryMetrics
}

// InventoryMetrics 在庫システムのメトリクス
type InventoryMetrics struct {
	mu                    sync.RWMutex
	successfulOrders      int64
	failedOrders          int64
	stockOutErrors        int64
	optimisticLockRetries int64
	deadlockRetries       int64
}

// InventoryMetricsSnapshot メトリクスのスナップショット（mutex無し）
type InventoryMetricsSnapshot struct {
	SuccessfulOrders      int64 `json:"successful_orders"`
	FailedOrders          int64 `json:"failed_orders"`
	StockOutErrors        int64 `json:"stock_out_errors"`
	OptimisticLockRetries int64 `json:"optimistic_lock_retries"`
	DeadlockRetries       int64 `json:"deadlock_retries"`
}

// カスタムエラー定義
var (
	ErrInsufficientStock    = errors.New("insufficient stock")
	ErrOptimisticLockFailed = errors.New("optimistic lock failed")
	ErrMaxRetriesExceeded   = errors.New("maximum retries exceeded")
	ErrDeadlock             = errors.New("deadlock detected")
)

// NewInventoryManager 新しい在庫管理システムを作成
func NewInventoryManager(db *sql.DB) *InventoryManager {
	return &InventoryManager{
		db:         db,
		maxRetries: 5,
		retryDelay: 10 * time.Millisecond,
		metrics:    &InventoryMetrics{},
	}
}

// ProcessOrder 注文を処理（楽観的ロック付き）
func (im *InventoryManager) ProcessOrder(ctx context.Context, req *OrderRequest) error {
	for attempt := 0; attempt < im.maxRetries; attempt++ {
		err := im.processOrderAttempt(ctx, req)

		if err == nil {
			im.metrics.incrementSuccessfulOrders()
			return nil
		}

		// エラー種別による処理分岐
		switch {
		case errors.Is(err, ErrOptimisticLockFailed):
			im.metrics.incrementOptimisticLockRetries()
			// 楽観的ロック失敗時は短時間待機してリトライ
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(im.calculateBackoffDelay(attempt)):
				continue
			}
		case isDeadlockError(err):
			im.metrics.incrementDeadlockRetries()
			// デッドロック時は少し長めに待機
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(im.calculateBackoffDelay(attempt) * 2):
				continue
			}
		case errors.Is(err, ErrInsufficientStock):
			im.metrics.incrementStockOutErrors()
			im.metrics.incrementFailedOrders()
			return err // 在庫不足はリトライしない
		default:
			im.metrics.incrementFailedOrders()
			return err // その他のエラーもリトライしない
		}
	}

	im.metrics.incrementFailedOrders()
	return ErrMaxRetriesExceeded
}

// processOrderAttempt 単一の注文処理を試行
func (im *InventoryManager) processOrderAttempt(ctx context.Context, req *OrderRequest) error {
	tx, err := im.db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
	})
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil {
			log.Printf("Failed to rollback transaction: %v", err)
		}
	}()

	// 1. 現在の在庫情報を取得（FOR UPDATE）
	item, err := im.getInventoryForUpdate(ctx, tx, req.ItemID)
	if err != nil {
		return err
	}

	// 2. 在庫量チェック
	if item.Quantity < req.Quantity {
		return ErrInsufficientStock
	}

	// 3. 在庫を減算（楽観的ロック付き）
	newQuantity := item.Quantity - req.Quantity
	updated, err := im.updateInventoryWithOptimisticLock(ctx, tx, item.ID,
		newQuantity, item.Version)
	if err != nil {
		return err
	}

	if !updated {
		return ErrOptimisticLockFailed
	}

	// 4. 注文履歴を記録
	err = im.recordOrderHistory(ctx, tx, req, item.Quantity, newQuantity)
	if err != nil {
		return fmt.Errorf("failed to record order history: %w", err)
	}

	// 5. トランザクションコミット
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	log.Printf("Order processed: ItemID=%d, Quantity=%d, RemainingStock=%d",
		req.ItemID, req.Quantity, newQuantity)

	return nil
}

// getInventoryForUpdate 更新用に在庫情報を取得
func (im *InventoryManager) getInventoryForUpdate(ctx context.Context, tx *sql.Tx, itemID int64) (*InventoryItem, error) {
	query := `
		SELECT id, name, quantity, version, updated_at 
		FROM inventory 
		WHERE id = $1 
		FOR UPDATE`

	var item InventoryItem
	err := tx.QueryRowContext(ctx, query, itemID).Scan(
		&item.ID, &item.Name, &item.Quantity, &item.Version, &item.UpdatedAt)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("inventory item not found: id=%d", itemID)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get inventory: %w", err)
	}

	return &item, nil
}

// updateInventoryWithOptimisticLock 楽観的ロック付きで在庫を更新
func (im *InventoryManager) updateInventoryWithOptimisticLock(ctx context.Context,
	tx *sql.Tx, itemID int64, newQuantity int32, expectedVersion int32) (bool, error) {

	query := `
		UPDATE inventory 
		SET quantity = $1, version = version + 1, updated_at = CURRENT_TIMESTAMP 
		WHERE id = $2 AND version = $3`

	result, err := tx.ExecContext(ctx, query, newQuantity, itemID, expectedVersion)
	if err != nil {
		return false, fmt.Errorf("failed to update inventory: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("failed to get rows affected: %w", err)
	}

	return rowsAffected > 0, nil
}

// recordOrderHistory 注文履歴を記録
func (im *InventoryManager) recordOrderHistory(ctx context.Context, tx *sql.Tx,
	req *OrderRequest, beforeQuantity, afterQuantity int32) error {

	query := `
		INSERT INTO order_history (order_id, item_id, quantity, before_stock, after_stock, created_at)
		VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP)`

	_, err := tx.ExecContext(ctx, query, req.OrderID, req.ItemID, req.Quantity,
		beforeQuantity, afterQuantity)

	return err
}

// calculateBackoffDelay 指数バックオフ遅延を計算
func (im *InventoryManager) calculateBackoffDelay(attempt int) time.Duration {
	// 指数バックオフ: 基本遅延 * 2^attempt + ジッター
	backoff := im.retryDelay * time.Duration(1<<uint(attempt))
	jitter := time.Duration(attempt) * time.Millisecond
	return backoff + jitter
}

// isDeadlockError デッドロックエラーかどうかを判定
func isDeadlockError(err error) bool {
	if err == nil {
		return false
	}
	// PostgreSQLのデッドロックエラーコード: 40P01
	return fmt.Sprintf("%v", err) == "pq: deadlock detected"
}

// GetInventoryStatus 在庫状況を取得
func (im *InventoryManager) GetInventoryStatus(ctx context.Context, itemID int64) (*InventoryItem, error) {
	query := `
		SELECT id, name, quantity, version, updated_at 
		FROM inventory 
		WHERE id = $1`

	var item InventoryItem
	err := im.db.QueryRowContext(ctx, query, itemID).Scan(
		&item.ID, &item.Name, &item.Quantity, &item.Version, &item.UpdatedAt)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("inventory item not found: id=%d", itemID)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get inventory status: %w", err)
	}

	return &item, nil
}

// GetMetrics システムメトリクスを取得
func (im *InventoryManager) GetMetrics() InventoryMetricsSnapshot {
	im.metrics.mu.RLock()
	defer im.metrics.mu.RUnlock()

	return InventoryMetricsSnapshot{
		SuccessfulOrders:      im.metrics.successfulOrders,
		FailedOrders:          im.metrics.failedOrders,
		StockOutErrors:        im.metrics.stockOutErrors,
		OptimisticLockRetries: im.metrics.optimisticLockRetries,
		DeadlockRetries:       im.metrics.deadlockRetries,
	}
}

// メトリクス更新メソッド群
func (m *InventoryMetrics) incrementSuccessfulOrders() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.successfulOrders++
}

func (m *InventoryMetrics) incrementFailedOrders() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failedOrders++
}

func (m *InventoryMetrics) incrementStockOutErrors() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stockOutErrors++
}

func (m *InventoryMetrics) incrementOptimisticLockRetries() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.optimisticLockRetries++
}

func (m *InventoryMetrics) incrementDeadlockRetries() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deadlockRetries++
}

// 使用例とデモンストレーション
func main() {
	// データベース接続
	db, err := sql.Open("postgres",
		"host=localhost port=5432 user=tutorial_user password=tutorial_pass dbname=tutorial_db sslmode=disable")
	if err != nil {
		log.Fatal("Failed to connect to database:", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Printf("Failed to close database connection: %v", err)
		}
	}()

	// 在庫管理システム初期化
	inventoryManager := NewInventoryManager(db)

	// 並行注文処理のデモンストレーション
	ctx := context.Background()

	// 同じアイテムに対する複数の並行注文
	var wg sync.WaitGroup
	orderCount := 10
	itemID := int64(1)

	for i := 0; i < orderCount; i++ {
		wg.Add(1)
		go func(orderNum int) {
			defer wg.Done()

			req := &OrderRequest{
				ItemID:   itemID,
				Quantity: 1,
				OrderID:  fmt.Sprintf("ORDER-%d", orderNum),
			}

			err := inventoryManager.ProcessOrder(ctx, req)
			if err != nil {
				log.Printf("Order %d failed: %v", orderNum, err)
			} else {
				log.Printf("Order %d succeeded", orderNum)
			}
		}(i)
	}

	wg.Wait()

	// 最終的な在庫状況を確認
	finalInventory, err := inventoryManager.GetInventoryStatus(ctx, itemID)
	if err != nil {
		log.Printf("Failed to get final inventory: %v", err)
	} else {
		log.Printf("Final inventory: %+v", finalInventory)
	}

	// メトリクス表示
	metrics := inventoryManager.GetMetrics()
	log.Printf("System metrics: %+v", metrics)
}
