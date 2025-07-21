// 02_transaction_control.go
// トランザクション並行制御システム
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/lib/pq"
)

// TxConfig はトランザクション設定です
type TxConfig struct {
	IsolationLevel sql.IsolationLevel
	ReadOnly       bool
	Timeout        time.Duration
	RetryPolicy    *RetryConfig
}

// RetryConfig はリトライ設定です
type RetryConfig struct {
	MaxRetries      int
	InitialDelay    time.Duration
	MaxDelay        time.Duration
	BackoffFactor   float64
	RetryableErrors []string
}

// TransactionManager はトランザクション管理を行います
type TransactionManager struct {
	db    *sql.DB
	stats *TransactionStats
}

// TransactionStats はトランザクション統計です
type TransactionStats struct {
	mu                sync.RWMutex
	totalTransactions int64
	committedTx       int64
	rolledBackTx      int64
	deadlockCount     int64
	retryCount        int64
	avgTxDuration     time.Duration
}

// NewTransactionManager は新しいトランザクションマネージャーを作成します
func NewTransactionManager(db *sql.DB) *TransactionManager {
	return &TransactionManager{
		db:    db,
		stats: &TransactionStats{},
	}
}

// ExecuteTransaction はトランザクションを実行します
func (tm *TransactionManager) ExecuteTransaction(ctx context.Context, config *TxConfig, fn func(*sql.Tx) error) error {
	start := time.Now()
	atomic.AddInt64(&tm.stats.totalTransactions, 1)

	defer func() {
		elapsed := time.Since(start)
		tm.updateTxDuration(elapsed)
	}()

	// タイムアウト設定
	if config.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, config.Timeout)
		defer cancel()
	}

	// リトライ付きでトランザクション実行
	if config.RetryPolicy != nil {
		return tm.executeWithRetry(ctx, config, fn)
	}

	return tm.executeSingleTransaction(ctx, config, fn)
}

// executeWithRetry はリトライ付きでトランザクションを実行します
func (tm *TransactionManager) executeWithRetry(ctx context.Context, config *TxConfig, fn func(*sql.Tx) error) error {
	var lastErr error
	retryDelay := config.RetryPolicy.InitialDelay

	for attempt := 0; attempt <= config.RetryPolicy.MaxRetries; attempt++ {
		if attempt > 0 {
			// リトライ前の待機
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(retryDelay):
				atomic.AddInt64(&tm.stats.retryCount, 1)
			}

			// 指数バックオフ
			retryDelay = time.Duration(float64(retryDelay) * config.RetryPolicy.BackoffFactor)
			if retryDelay > config.RetryPolicy.MaxDelay {
				retryDelay = config.RetryPolicy.MaxDelay
			}
		}

		lastErr = tm.executeSingleTransaction(ctx, config, fn)
		if lastErr == nil {
			return nil // 成功
		}

		// リトライ可能なエラーかチェック
		if !tm.isRetryableError(lastErr, config.RetryPolicy) {
			break
		}

		if tm.isDeadlockError(lastErr) {
			atomic.AddInt64(&tm.stats.deadlockCount, 1)
		}

		log.Printf("Transaction failed (attempt %d/%d): %v", attempt+1, config.RetryPolicy.MaxRetries+1, lastErr)
	}

	return lastErr
}

// executeSingleTransaction は単一のトランザクションを実行します
func (tm *TransactionManager) executeSingleTransaction(ctx context.Context, config *TxConfig, fn func(*sql.Tx) error) error {
	tx, err := tm.db.BeginTx(ctx, &sql.TxOptions{
		Isolation: config.IsolationLevel,
		ReadOnly:  config.ReadOnly,
	})
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %v", err)
	}

	// パニック回復とロールバック
	defer func() {
		if r := recover(); r != nil {
			if err := tx.Rollback(); err != nil {
				log.Printf("Failed to rollback transaction during panic: %v", err)
			}
			atomic.AddInt64(&tm.stats.rolledBackTx, 1)
			panic(r) // パニックを再発生
		}
	}()

	// トランザクション処理実行
	if err := fn(tx); err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			log.Printf("Rollback failed: %v", rollbackErr)
		}
		atomic.AddInt64(&tm.stats.rolledBackTx, 1)
		return err
	}

	// コミット
	if err := tx.Commit(); err != nil {
		atomic.AddInt64(&tm.stats.rolledBackTx, 1)
		return fmt.Errorf("failed to commit transaction: %v", err)
	}

	atomic.AddInt64(&tm.stats.committedTx, 1)
	return nil
}

// isRetryableError はリトライ可能なエラーかチェックします
func (tm *TransactionManager) isRetryableError(err error, retryConfig *RetryConfig) bool {
	if err == nil {
		return false
	}

	errStr := strings.ToLower(err.Error())
	for _, retryableErr := range retryConfig.RetryableErrors {
		if strings.Contains(errStr, strings.ToLower(retryableErr)) {
			return true
		}
	}

	return false
}

// isDeadlockError はデッドロックエラーかチェックします
func (tm *TransactionManager) isDeadlockError(err error) bool {
	if err == nil {
		return false
	}
	errStr := strings.ToLower(err.Error())
	return strings.Contains(errStr, "deadlock") || strings.Contains(errStr, "40P01")
}

// updateTxDuration はトランザクション実行時間を更新します
func (tm *TransactionManager) updateTxDuration(elapsed time.Duration) {
	tm.stats.mu.Lock()
	defer tm.stats.mu.Unlock()

	totalTx := atomic.LoadInt64(&tm.stats.totalTransactions)
	if totalTx == 1 {
		tm.stats.avgTxDuration = elapsed
	} else {
		// 移動平均の計算
		currentAvg := int64(tm.stats.avgTxDuration)
		newAvg := (currentAvg*(totalTx-1) + int64(elapsed)) / totalTx
		tm.stats.avgTxDuration = time.Duration(newAvg)
	}
}

// GetStats は統計情報を取得します
func (tm *TransactionManager) GetStats() TransactionStatsSnapshot {
	tm.stats.mu.RLock()
	defer tm.stats.mu.RUnlock()

	return TransactionStatsSnapshot{
		TotalTransactions: atomic.LoadInt64(&tm.stats.totalTransactions),
		CommittedTx:       atomic.LoadInt64(&tm.stats.committedTx),
		RolledBackTx:      atomic.LoadInt64(&tm.stats.rolledBackTx),
		DeadlockCount:     atomic.LoadInt64(&tm.stats.deadlockCount),
		RetryCount:        atomic.LoadInt64(&tm.stats.retryCount),
		AvgTxDuration:     tm.stats.avgTxDuration,
	}
}

// TransactionStatsSnapshot は統計のスナップショットです
type TransactionStatsSnapshot struct {
	TotalTransactions int64
	CommittedTx       int64
	RolledBackTx      int64
	DeadlockCount     int64
	RetryCount        int64
	AvgTxDuration     time.Duration
}

// OptimisticLockManager は楽観的ロック管理を行います
type OptimisticLockManager struct {
	tm *TransactionManager
}

// NewOptimisticLockManager は楽観的ロックマネージャーを作成します
func NewOptimisticLockManager(tm *TransactionManager) *OptimisticLockManager {
	return &OptimisticLockManager{tm: tm}
}

// UpdateWithOptimisticLock は楽観的ロック付きで更新します
func (olm *OptimisticLockManager) UpdateWithOptimisticLock(ctx context.Context, tableName string, id int64,
	updateFields map[string]interface{}) error {

	config := &TxConfig{
		IsolationLevel: sql.LevelReadCommitted,
		Timeout:        30 * time.Second,
		RetryPolicy: &RetryConfig{
			MaxRetries:    5,
			InitialDelay:  10 * time.Millisecond,
			MaxDelay:      1 * time.Second,
			BackoffFactor: 2.0,
			RetryableErrors: []string{
				"optimistic lock",
				"concurrent update",
				"version mismatch",
			},
		},
	}

	return olm.tm.ExecuteTransaction(ctx, config, func(tx *sql.Tx) error {
		// 現在のバージョンを取得
		var currentVersion int64
		query := fmt.Sprintf("SELECT version FROM %s WHERE id = $1", tableName)
		err := tx.QueryRowContext(ctx, query, id).Scan(&currentVersion)
		if err != nil {
			return fmt.Errorf("failed to get current version: %v", err)
		}

		// 更新実行
		setParts := make([]string, 0, len(updateFields))
		args := make([]interface{}, 0, len(updateFields)+2)
		argIndex := 1

		for field, value := range updateFields {
			setParts = append(setParts, fmt.Sprintf("%s = $%d", field, argIndex))
			args = append(args, value)
			argIndex++
		}

		// バージョンも更新
		setParts = append(setParts, fmt.Sprintf("version = $%d", argIndex))
		args = append(args, currentVersion+1)
		argIndex++

		args = append(args, id, currentVersion)

		updateQuery := fmt.Sprintf(
			"UPDATE %s SET %s WHERE id = $%d AND version = $%d",
			tableName, strings.Join(setParts, ", "), argIndex, argIndex+1)

		result, err := tx.ExecContext(ctx, updateQuery, args...)
		if err != nil {
			return fmt.Errorf("failed to execute update: %v", err)
		}

		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("failed to get rows affected: %v", err)
		}

		if rowsAffected == 0 {
			return fmt.Errorf("optimistic lock error: record was modified by another transaction")
		}

		return nil
	})
}

// InventoryManager は在庫管理を行います
type InventoryManager struct {
	tm  *TransactionManager
	olm *OptimisticLockManager
}

// NewInventoryManager は在庫マネージャーを作成します
func NewInventoryManager(tm *TransactionManager) *InventoryManager {
	return &InventoryManager{
		tm:  tm,
		olm: NewOptimisticLockManager(tm),
	}
}

// ReserveInventory は在庫を予約します
func (im *InventoryManager) ReserveInventory(ctx context.Context, productID int64, warehouseID int64, quantity int) error {
	config := &TxConfig{
		IsolationLevel: sql.LevelReadCommitted,
		Timeout:        30 * time.Second,
		RetryPolicy: &RetryConfig{
			MaxRetries:      3,
			InitialDelay:    50 * time.Millisecond,
			MaxDelay:        500 * time.Millisecond,
			BackoffFactor:   2.0,
			RetryableErrors: []string{"deadlock", "40P01"},
		},
	}

	return im.tm.ExecuteTransaction(ctx, config, func(tx *sql.Tx) error {
		// 在庫確認と予約
		var currentQty, reservedQty int
		var version int64

		err := tx.QueryRowContext(ctx,
			"SELECT quantity, reserved_quantity, version FROM inventory WHERE product_id = $1 AND warehouse_id = $2",
			productID, warehouseID).Scan(&currentQty, &reservedQty, &version)
		if err != nil {
			return fmt.Errorf("failed to get inventory: %v", err)
		}

		availableQty := currentQty - reservedQty
		if availableQty < quantity {
			return fmt.Errorf("insufficient inventory: available=%d, requested=%d", availableQty, quantity)
		}

		// 予約数を更新
		result, err := tx.ExecContext(ctx,
			"UPDATE inventory SET reserved_quantity = reserved_quantity + $1, version = version + 1 WHERE product_id = $2 AND warehouse_id = $3 AND version = $4",
			quantity, productID, warehouseID, version)
		if err != nil {
			return fmt.Errorf("failed to update inventory: %v", err)
		}

		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("failed to get rows affected: %v", err)
		}

		if rowsAffected == 0 {
			return fmt.Errorf("inventory was modified by another transaction")
		}

		log.Printf("Reserved %d units of product %d in warehouse %d", quantity, productID, warehouseID)
		return nil
	})
}

// ProcessConcurrentOrders は並行注文処理をシミュレートします
func (im *InventoryManager) ProcessConcurrentOrders(ctx context.Context, productID int64, warehouseID int64, orderCount int) {
	var wg sync.WaitGroup
	successCount := int64(0)
	failureCount := int64(0)

	for i := 0; i < orderCount; i++ {
		wg.Add(1)
		go func(orderID int) {
			defer wg.Done()

			// ランダムな数量で注文
			quantity := rand.Intn(5) + 1

			err := im.ReserveInventory(ctx, productID, warehouseID, quantity)
			if err != nil {
				atomic.AddInt64(&failureCount, 1)
				log.Printf("Order %d failed: %v", orderID, err)
			} else {
				atomic.AddInt64(&successCount, 1)
				log.Printf("Order %d succeeded: reserved %d units", orderID, quantity)
			}
		}(i + 1)
	}

	wg.Wait()

	log.Printf("Concurrent order processing completed: Success=%d, Failure=%d",
		atomic.LoadInt64(&successCount), atomic.LoadInt64(&failureCount))
}

// 使用例とテスト用のmain関数
func main() {
	// データベース接続
	db, err := sql.Open("postgres",
		"postgres://test_user:test_password@localhost:5432/advanced_golang_tutorial?sslmode=disable")
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Printf("Failed to close database: %v", err)
		}
	}()

	// トランザクションマネージャー作成
	tm := NewTransactionManager(db)
	im := NewInventoryManager(tm)

	ctx := context.Background()

	// 楽観的ロックのテスト
	log.Println("Testing optimistic lock...")
	testOptimisticLock(tm)

	// 並行在庫処理のテスト
	log.Println("Testing concurrent inventory management...")
	im.ProcessConcurrentOrders(ctx, 1, 1, 10)

	// 統計表示
	stats := tm.GetStats()
	fmt.Printf("\nTransaction Statistics:\n")
	fmt.Printf("  Total Transactions: %d\n", stats.TotalTransactions)
	fmt.Printf("  Committed: %d\n", stats.CommittedTx)
	fmt.Printf("  Rolled Back: %d\n", stats.RolledBackTx)
	fmt.Printf("  Deadlocks: %d\n", stats.DeadlockCount)
	fmt.Printf("  Retries: %d\n", stats.RetryCount)
	fmt.Printf("  Average Duration: %v\n", stats.AvgTxDuration)
}

// testOptimisticLock は楽観的ロックのテストを実行します
func testOptimisticLock(tm *TransactionManager) {
	ctx := context.Background()
	olm := NewOptimisticLockManager(tm)

	var wg sync.WaitGroup
	productID := int64(1)

	// 並行で同じ商品を更新
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			updateFields := map[string]interface{}{
				"price": 1000.0 + float64(workerID*100),
			}

			err := olm.UpdateWithOptimisticLock(ctx, "products", productID, updateFields)
			if err != nil {
				log.Printf("Worker %d: Update failed: %v", workerID, err)
			} else {
				log.Printf("Worker %d: Update succeeded", workerID)
			}
		}(i)
	}

	wg.Wait()
	log.Println("Optimistic lock test completed")
}
