package main

import (
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

// setupTestDB 2フェーズコミットの参加者が使うテーブルを作り、接続を返す
func setupTestDB(t *testing.T, stock, balance int) *sql.DB {
	t.Helper()
	ctx := t.Context()

	pgContainer, err := postgres.Run(ctx, "postgres:15-alpine",
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("testuser"),
		postgres.WithPassword("testpass"),
		postgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("PostgreSQL コンテナの起動に失敗した: %v", err)
	}
	t.Cleanup(func() {
		if err := pgContainer.Terminate(ctx); err != nil {
			t.Logf("コンテナの停止に失敗した: %v", err)
		}
	})

	dsn, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("接続文字列の取得に失敗した: %v", err)
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("接続に失敗した: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Logf("接続のクローズに失敗した: %v", err)
		}
	})

	_, err = db.ExecContext(ctx, `
		CREATE TABLE customers (id BIGINT PRIMARY KEY);
		CREATE TABLE customer_accounts (
			customer_id BIGINT PRIMARY KEY, balance BIGINT NOT NULL, updated_at TIMESTAMP);
		CREATE TABLE inventory (id BIGINT PRIMARY KEY, quantity INTEGER NOT NULL, updated_at TIMESTAMP);
		CREATE TABLE inventory_reservations (
			item_id BIGINT, order_id TEXT, quantity INTEGER, created_at TIMESTAMP);
		CREATE TABLE payment_reservations (
			customer_id BIGINT, order_id TEXT, amount BIGINT, created_at TIMESTAMP);
		CREATE TABLE payments (
			order_id TEXT, customer_id BIGINT, amount BIGINT, status TEXT, created_at TIMESTAMP);
		CREATE TABLE orders (
			order_id TEXT PRIMARY KEY, customer_id BIGINT, item_id BIGINT, quantity INTEGER,
			total_amount BIGINT, status TEXT, created_at TIMESTAMP);
		CREATE TABLE distributed_transactions (
			tx_id TEXT PRIMARY KEY, order_id TEXT, status TEXT, created_at TIMESTAMP, updated_at TIMESTAMP);
		INSERT INTO customers (id) VALUES (1);`)
	if err != nil {
		t.Fatalf("スキーマ作成に失敗した: %v", err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO customer_accounts (customer_id, balance) VALUES (1, $1)", balance); err != nil {
		t.Fatalf("口座の作成に失敗した: %v", err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO inventory (id, quantity) VALUES (1, $1)", stock); err != nil {
		t.Fatalf("在庫の作成に失敗した: %v", err)
	}
	return db
}

// queryInt 1 行 1 列の整数を返す
func queryInt(t *testing.T, db *sql.DB, query string) int {
	t.Helper()
	var n int
	if err := db.QueryRowContext(t.Context(), query).Scan(&n); err != nil {
		t.Fatalf("%s の実行に失敗した: %v", query, err)
	}
	return n
}

func TestExecuteDistributedTransaction_CommitsAllParticipants(t *testing.T) {
	db := setupTestDB(t, 10, 5000)
	tc := NewTransactionCoordinator(db)

	err := tc.ExecuteDistributedTransaction(t.Context(), &OrderData{
		OrderID: "ORDER-1", CustomerID: 1, ItemID: 1, Quantity: 2, TotalAmount: 2000, CreatedAt: time.Now(),
	})
	if err != nil {
		t.Fatalf("分散トランザクションが失敗した: %v", err)
	}

	if got := queryInt(t, db, "SELECT quantity FROM inventory WHERE id = 1"); got != 8 {
		t.Errorf("在庫 = %d, 期待値 8", got)
	}
	if got := queryInt(t, db, "SELECT balance FROM customer_accounts WHERE customer_id = 1"); got != 3000 {
		t.Errorf("残高 = %d, 期待値 3000", got)
	}
	if got := queryInt(t, db, "SELECT COUNT(*) FROM orders"); got != 1 {
		t.Errorf("注文 = %d 件, 期待値 1 件", got)
	}
	if got := queryInt(t, db, "SELECT COUNT(*) FROM inventory_reservations") +
		queryInt(t, db, "SELECT COUNT(*) FROM payment_reservations"); got != 0 {
		t.Errorf("残った予約 = %d 件, 期待値 0 件", got)
	}
	if m := tc.GetMetrics(); m.CommittedTx != 1 || m.AbortedTx != 0 {
		t.Errorf("メトリクス = %+v, 期待値 コミット 1・中止 0", m)
	}
}

func TestExecuteDistributedTransaction_InsufficientFundsAbortsEverything(t *testing.T) {
	db := setupTestDB(t, 10, 1000)
	tc := NewTransactionCoordinator(db)

	err := tc.ExecuteDistributedTransaction(t.Context(), &OrderData{
		OrderID: "ORDER-1", CustomerID: 1, ItemID: 1, Quantity: 2, TotalAmount: 2000, CreatedAt: time.Now(),
	})
	if !errors.Is(err, ErrPreparePhaseFailure) {
		t.Fatalf("エラー = %v, 期待値 ErrPreparePhaseFailure", err)
	}

	if got := queryInt(t, db, "SELECT quantity FROM inventory WHERE id = 1"); got != 10 {
		t.Errorf("在庫 = %d, 期待値 10（変化なし）", got)
	}
	if got := queryInt(t, db, "SELECT balance FROM customer_accounts WHERE customer_id = 1"); got != 1000 {
		t.Errorf("残高 = %d, 期待値 1000（変化なし）", got)
	}
	if got := queryInt(t, db, "SELECT COUNT(*) FROM orders") +
		queryInt(t, db, "SELECT COUNT(*) FROM inventory_reservations"); got != 0 {
		t.Errorf("注文と在庫予約 = %d 件, 期待値 0 件", got)
	}
	var status string
	if err := db.QueryRowContext(t.Context(), "SELECT status FROM distributed_transactions").Scan(&status); err != nil {
		t.Fatalf("状態の取得に失敗した: %v", err)
	}
	if status != string(StatusAborted) {
		t.Errorf("状態 = %s, 期待値 %s", status, StatusAborted)
	}
}

func TestExecuteDistributedTransaction_ConcurrentOrdersNeverOversell(t *testing.T) {
	const stock = 3
	db := setupTestDB(t, stock, 1_000_000)
	tc := NewTransactionCoordinator(db)

	var wg sync.WaitGroup
	for n := range 6 {
		wg.Go(func() {
			// SERIALIZABLE なので直列化エラーで中止される注文もある。件数ではなく在庫の下限だけを確かめる
			if err := tc.ExecuteDistributedTransaction(t.Context(), &OrderData{
				OrderID: fmt.Sprintf("ORDER-%d", n), CustomerID: 1, ItemID: 1,
				Quantity: 1, TotalAmount: 100, CreatedAt: time.Now(),
			}); err != nil {
				t.Logf("注文 %d は中止された: %v", n, err)
			}
		})
	}
	wg.Wait()

	if got := queryInt(t, db, "SELECT quantity FROM inventory WHERE id = 1"); got < 0 {
		t.Errorf("在庫 = %d, 負になってはいけない", got)
	}
	if m := tc.GetMetrics(); m.TotalTransactions != 6 || m.CommittedTx+m.AbortedTx != 6 {
		t.Errorf("メトリクス = %+v, 期待値 合計 6 件がコミットか中止のどちらか", m)
	}
}
