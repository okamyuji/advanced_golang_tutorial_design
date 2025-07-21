package main

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// setupTestDB テスト用のPostgreSQLコンテナを起動し、初期化されたデータベースを返す
func setupTestDB(t *testing.T) (*sql.DB, func()) {
	ctx := context.Background()

	// PostgreSQLコンテナを起動
	pgContainer, err := postgres.RunContainer(ctx,
		testcontainers.WithImage("postgres:15-alpine"),
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("testuser"),
		postgres.WithPassword("testpass"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(30*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("Failed to start PostgreSQL container: %v", err)
	}

	// コンテナ接続情報取得
	host, err := pgContainer.Host(ctx)
	if err != nil {
		t.Fatalf("Failed to get container host: %v", err)
	}

	port, err := pgContainer.MappedPort(ctx, "5432")
	if err != nil {
		t.Fatalf("Failed to get container port: %v", err)
	}

	// データベース接続
	dsn := fmt.Sprintf("postgres://testuser:testpass@%s:%s/testdb?sslmode=disable", host, port.Port())
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		if termErr := pgContainer.Terminate(ctx); termErr != nil {
			t.Logf("Failed to terminate container: %v", termErr)
		}
		t.Fatalf("Failed to connect to database: %v", err)
	}

	// テスト用テーブル作成
	err = createTestTables(db)
	if err != nil {
		if closeErr := db.Close(); closeErr != nil {
			t.Logf("Failed to close database: %v", closeErr)
		}
		if termErr := pgContainer.Terminate(ctx); termErr != nil {
			t.Logf("Failed to terminate container: %v", termErr)
		}
		t.Fatalf("Failed to create test tables: %v", err)
	}

	// テストデータ挿入
	err = insertTestData(db)
	if err != nil {
		if closeErr := db.Close(); closeErr != nil {
			t.Logf("Failed to close database: %v", closeErr)
		}
		if termErr := pgContainer.Terminate(ctx); termErr != nil {
			t.Logf("Failed to terminate container: %v", termErr)
		}
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// クリーンアップ関数
	cleanup := func() {
		if err := db.Close(); err != nil {
			t.Logf("Failed to close database: %v", err)
		}
		if err := pgContainer.Terminate(ctx); err != nil {
			t.Logf("Failed to terminate container: %v", err)
		}
	}

	return db, cleanup
}

// createTestTables テスト用テーブルを作成
func createTestTables(db *sql.DB) error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS products (
			id SERIAL PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			price DECIMAL(10,2) NOT NULL,
			version BIGINT DEFAULT 1,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE TABLE IF NOT EXISTS inventory (
			id SERIAL PRIMARY KEY,
			product_id BIGINT NOT NULL,
			warehouse_id BIGINT NOT NULL,
			quantity INTEGER NOT NULL DEFAULT 0,
			reserved_quantity INTEGER NOT NULL DEFAULT 0,
			version BIGINT DEFAULT 1,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE TABLE IF NOT EXISTS test_locks (
			id SERIAL PRIMARY KEY,
			data TEXT,
			version BIGINT DEFAULT 1,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`,
	}

	for _, query := range queries {
		_, err := db.Exec(query)
		if err != nil {
			return fmt.Errorf("failed to create table: %v", err)
		}
	}

	return nil
}

// insertTestData テストデータを挿入
func insertTestData(db *sql.DB) error {
	// テスト用商品
	_, err := db.Exec(`
		INSERT INTO products (name, price) VALUES 
		('Test Product 1', 1000.00),
		('Test Product 2', 2000.00),
		('Test Product 3', 3000.00)
	`)
	if err != nil {
		return fmt.Errorf("failed to insert test products: %v", err)
	}

	// テスト用在庫
	_, err = db.Exec(`
		INSERT INTO inventory (product_id, warehouse_id, quantity) VALUES 
		(1, 1, 100),
		(2, 1, 50),
		(3, 1, 200)
	`)
	if err != nil {
		return fmt.Errorf("failed to insert test inventory: %v", err)
	}

	// テスト用ロックデータ
	_, err = db.Exec(`
		INSERT INTO test_locks (data) VALUES 
		('initial data 1'),
		('initial data 2'),
		('initial data 3')
	`)
	if err != nil {
		return fmt.Errorf("failed to insert test lock data: %v", err)
	}

	return nil
}

func TestTransactionManager_BasicTransaction(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	tm := NewTransactionManager(db)
	ctx := context.Background()

	config := &TxConfig{
		IsolationLevel: sql.LevelReadCommitted,
		Timeout:        10 * time.Second,
	}

	// 正常なトランザクション
	err := tm.ExecuteTransaction(ctx, config, func(tx *sql.Tx) error {
		_, err := tx.Exec("UPDATE products SET price = price * 1.1 WHERE id = $1", 1)
		return err
	})

	if err != nil {
		t.Fatalf("Transaction should succeed: %v", err)
	}

	// 価格が更新されていることを確認
	var price float64
	err = db.QueryRow("SELECT price FROM products WHERE id = $1", 1).Scan(&price)
	if err != nil {
		t.Fatalf("Failed to query updated price: %v", err)
	}

	expectedPrice := 1100.0
	if price != expectedPrice {
		t.Errorf("Expected price %f, got %f", expectedPrice, price)
	}

	// 統計確認
	stats := tm.GetStats()
	if stats.TotalTransactions != 1 {
		t.Errorf("Expected 1 transaction, got %d", stats.TotalTransactions)
	}
	if stats.CommittedTx != 1 {
		t.Errorf("Expected 1 committed transaction, got %d", stats.CommittedTx)
	}
}

func TestTransactionManager_WithRetry(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	tm := NewTransactionManager(db)
	ctx := context.Background()

	config := &TxConfig{
		IsolationLevel: sql.LevelReadCommitted,
		Timeout:        10 * time.Second,
		RetryPolicy: &RetryConfig{
			MaxRetries:      3,
			InitialDelay:    10 * time.Millisecond,
			MaxDelay:        100 * time.Millisecond,
			BackoffFactor:   2.0,
			RetryableErrors: []string{"optimistic", "concurrent"},
		},
	}

	attemptCount := 0
	err := tm.ExecuteTransaction(ctx, config, func(tx *sql.Tx) error {
		attemptCount++
		if attemptCount < 3 {
			return fmt.Errorf("optimistic lock error simulation")
		}
		_, err := tx.Exec("UPDATE products SET price = $1 WHERE id = $2", 5000.0, 2)
		return err
	})

	if err != nil {
		t.Fatalf("Transaction should succeed after retries: %v", err)
	}

	if attemptCount != 3 {
		t.Errorf("Expected 3 attempts, got %d", attemptCount)
	}

	// 統計でリトライがカウントされていることを確認
	stats := tm.GetStats()
	if stats.RetryCount < 2 {
		t.Errorf("Expected at least 2 retries, got %d", stats.RetryCount)
	}
}

func TestOptimisticLockManager_ConcurrentUpdates(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	tm := NewTransactionManager(db)
	olm := NewOptimisticLockManager(tm)
	ctx := context.Background()

	const numWorkers = 5
	var wg sync.WaitGroup
	var successCount, failureCount int64
	var mu sync.Mutex

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			updateFields := map[string]interface{}{
				"data": fmt.Sprintf("updated by worker %d at %s", workerID, time.Now().Format("15:04:05.000")),
			}

			err := olm.UpdateWithOptimisticLock(ctx, "test_locks", 1, updateFields)

			mu.Lock()
			if err != nil {
				failureCount++
				t.Logf("Worker %d failed: %v", workerID, err)
			} else {
				successCount++
				t.Logf("Worker %d succeeded", workerID)
			}
			mu.Unlock()
		}(i)
	}

	wg.Wait()

	// 少なくとも1つは成功することを確認
	if successCount == 0 {
		t.Error("At least one update should succeed")
	}
	// リトライ機能により最終的にすべて成功する場合もある
	if successCount+failureCount != numWorkers {
		t.Errorf("Total updates should equal worker count: success=%d, failure=%d, workers=%d", successCount, failureCount, numWorkers)
	}

	t.Logf("Results: Success=%d, Failure=%d", successCount, failureCount)

	// 最終的な値を確認
	var finalData string
	var finalVersion int64
	err := db.QueryRow("SELECT data, version FROM test_locks WHERE id = $1", 1).Scan(&finalData, &finalVersion)
	if err != nil {
		t.Fatalf("Failed to query final state: %v", err)
	}

	t.Logf("Final state: data='%s', version=%d", finalData, finalVersion)

	// バージョンが初期値から更新されていることを確認
	if finalVersion <= 1 {
		t.Error("Version should be incremented after updates")
	}
}

func TestInventoryManager_ConcurrentReservations(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	tm := NewTransactionManager(db)
	im := NewInventoryManager(tm)
	ctx := context.Background()

	const numOrders = 10
	const productID = 1
	const warehouseID = 1

	// 初期在庫確認
	var initialQty, initialReserved int
	err := db.QueryRow("SELECT quantity, reserved_quantity FROM inventory WHERE product_id = $1 AND warehouse_id = $2",
		productID, warehouseID).Scan(&initialQty, &initialReserved)
	if err != nil {
		t.Fatalf("Failed to get initial inventory: %v", err)
	}

	t.Logf("Initial inventory: quantity=%d, reserved=%d", initialQty, initialReserved)

	var wg sync.WaitGroup
	successCount := int64(0)
	failureCount := int64(0)

	for i := 0; i < numOrders; i++ {
		wg.Add(1)
		go func(orderID int) {
			defer wg.Done()

			quantity := 5 // 各注文で5個予約
			err := im.ReserveInventory(ctx, productID, warehouseID, quantity)
			if err != nil {
				atomic.AddInt64(&failureCount, 1)
				t.Logf("Order %d failed: %v", orderID, err)
			} else {
				atomic.AddInt64(&successCount, 1)
				t.Logf("Order %d succeeded: reserved %d units", orderID, quantity)
			}
		}(i + 1)
	}

	wg.Wait()

	// 結果確認
	var finalQty, finalReserved int
	err = db.QueryRow("SELECT quantity, reserved_quantity FROM inventory WHERE product_id = $1 AND warehouse_id = $2",
		productID, warehouseID).Scan(&finalQty, &finalReserved)
	if err != nil {
		t.Fatalf("Failed to get final inventory: %v", err)
	}

	// 予約数の増加が正しいことを確認
	successCountValue := atomic.LoadInt64(&successCount)
	failureCountValue := atomic.LoadInt64(&failureCount)

	t.Logf("Final inventory: quantity=%d, reserved=%d", finalQty, finalReserved)
	t.Logf("Order results: Success=%d, Failure=%d", successCountValue, failureCountValue)
	expectedReservedIncrease := int(successCountValue) * 5
	actualReservedIncrease := finalReserved - initialReserved

	if actualReservedIncrease != expectedReservedIncrease {
		t.Errorf("Expected reserved increase %d, got %d", expectedReservedIncrease, actualReservedIncrease)
	}

	// 在庫不足で失敗するケースがあることを確認
	if failureCountValue == 0 && successCountValue*5 > int64(initialQty) {
		t.Error("Some orders should fail due to insufficient inventory")
	}

	// 統計確認
	stats := tm.GetStats()
	if stats.TotalTransactions == 0 {
		t.Error("Transaction statistics should show activity")
	}
}

func TestTransactionManager_RollbackOnError(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	tm := NewTransactionManager(db)
	ctx := context.Background()

	config := &TxConfig{
		IsolationLevel: sql.LevelReadCommitted,
		Timeout:        10 * time.Second,
	}

	// 初期価格確認
	var initialPrice float64
	err := db.QueryRow("SELECT price FROM products WHERE id = $1", 3).Scan(&initialPrice)
	if err != nil {
		t.Fatalf("Failed to get initial price: %v", err)
	}

	// エラーを発生させるトランザクション
	err = tm.ExecuteTransaction(ctx, config, func(tx *sql.Tx) error {
		// 最初の更新は成功
		_, err := tx.Exec("UPDATE products SET price = $1 WHERE id = $2", 9999.0, 3)
		if err != nil {
			return err
		}

		// 意図的にエラーを発生
		return fmt.Errorf("intentional error for rollback test")
	})

	if err == nil {
		t.Fatal("Transaction should fail")
	}

	// ロールバックにより価格が変更されていないことを確認
	var finalPrice float64
	err = db.QueryRow("SELECT price FROM products WHERE id = $1", 3).Scan(&finalPrice)
	if err != nil {
		t.Fatalf("Failed to get final price: %v", err)
	}

	if finalPrice != initialPrice {
		t.Errorf("Price should not change due to rollback: initial=%f, final=%f", initialPrice, finalPrice)
	}

	// 統計でロールバックがカウントされていることを確認
	stats := tm.GetStats()
	if stats.RolledBackTx == 0 {
		t.Error("Rollback should be counted in statistics")
	}
}

func TestTransactionManager_Timeout(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	tm := NewTransactionManager(db)
	ctx := context.Background()

	config := &TxConfig{
		IsolationLevel: sql.LevelReadCommitted,
		Timeout:        100 * time.Millisecond, // 短いタイムアウト
	}

	start := time.Now()
	err := tm.ExecuteTransaction(ctx, config, func(tx *sql.Tx) error {
		// 長時間処理をシミュレート
		time.Sleep(200 * time.Millisecond)
		return nil
	})
	elapsed := time.Since(start)

	if err == nil {
		t.Error("Transaction should timeout")
	}

	// タイムアウト時間内で終了していることを確認
	if elapsed > 250*time.Millisecond {
		t.Errorf("Transaction should timeout quickly, took %v", elapsed)
	}

	t.Logf("Transaction correctly timed out after %v", elapsed)
}

func TestTransactionManager_IsolationLevels(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	tm := NewTransactionManager(db)
	ctx := context.Background()

	isolationLevels := []sql.IsolationLevel{
		sql.LevelDefault,
		sql.LevelReadUncommitted,
		sql.LevelReadCommitted,
		sql.LevelRepeatableRead,
		sql.LevelSerializable,
	}

	for _, level := range isolationLevels {
		t.Run(fmt.Sprintf("IsolationLevel_%d", level), func(t *testing.T) {
			config := &TxConfig{
				IsolationLevel: level,
				Timeout:        10 * time.Second,
			}

			err := tm.ExecuteTransaction(ctx, config, func(tx *sql.Tx) error {
				_, err := tx.Exec("SELECT 1")
				return err
			})

			if err != nil {
				t.Errorf("Transaction with isolation level %d should succeed: %v", level, err)
			}
		})
	}
}

func TestTransactionManager_Statistics(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	tm := NewTransactionManager(db)
	ctx := context.Background()

	config := &TxConfig{
		IsolationLevel: sql.LevelReadCommitted,
		Timeout:        10 * time.Second,
	}

	// 複数のトランザクションを実行
	const numTransactions = 5
	successfulTx := 0

	for i := 0; i < numTransactions; i++ {
		err := tm.ExecuteTransaction(ctx, config, func(tx *sql.Tx) error {
			if i%2 == 0 {
				// 成功するトランザクション
				_, err := tx.Exec("SELECT 1")
				return err
			} else {
				// 失敗するトランザクション
				return fmt.Errorf("intentional failure for test")
			}
		})

		if err == nil {
			successfulTx++
		}
	}

	// 統計確認
	stats := tm.GetStats()

	if stats.TotalTransactions != numTransactions {
		t.Errorf("Expected %d total transactions, got %d", numTransactions, stats.TotalTransactions)
	}

	if stats.CommittedTx != int64(successfulTx) {
		t.Errorf("Expected %d committed transactions, got %d", successfulTx, stats.CommittedTx)
	}

	expectedRollbacks := numTransactions - successfulTx
	if stats.RolledBackTx != int64(expectedRollbacks) {
		t.Errorf("Expected %d rolled back transactions, got %d", expectedRollbacks, stats.RolledBackTx)
	}

	if stats.AvgTxDuration <= 0 {
		t.Error("Average transaction duration should be positive")
	}

	t.Logf("Transaction statistics: %+v", stats)
}
