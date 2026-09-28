package main

import (
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/lib/pq"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

// setupTestDB PostgreSQL コンテナに在庫と注文履歴のテーブルを作り、接続を返す
func setupTestDB(t *testing.T) *sql.DB {
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
		CREATE TABLE inventory (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL,
			quantity INTEGER NOT NULL,
			version INTEGER NOT NULL DEFAULT 0,
			updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
		);
		CREATE TABLE order_history (
			order_id TEXT NOT NULL,
			item_id BIGINT NOT NULL,
			quantity INTEGER NOT NULL,
			before_stock INTEGER NOT NULL,
			after_stock INTEGER NOT NULL,
			created_at TIMESTAMP NOT NULL
		);
		INSERT INTO inventory (name, quantity) VALUES ('widget', 5);`)
	if err != nil {
		t.Fatalf("スキーマ作成に失敗した: %v", err)
	}
	return db
}

func TestIsDeadlockError_MatchesSQLStateThroughWrapping(t *testing.T) {
	deadlock := &pq.Error{Code: "40P01", Message: "deadlock detected"}

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"pq のデッドロック", deadlock, true},
		{"%w で包んだデッドロック", fmt.Errorf("failed to update inventory: %w", deadlock), true},
		{"別の SQLSTATE", &pq.Error{Code: "40001"}, false},
		{"pq 以外のエラー", errors.New("pq: deadlock detected"), false},
		{"nil", nil, false},
	}
	for _, tt := range tests {
		if got := isDeadlockError(tt.err); got != tt.want {
			t.Errorf("%s: isDeadlockError() = %v, 期待値 %v", tt.name, got, tt.want)
		}
	}
}

func TestProcessOrder_ConcurrentOrdersNeverOversell(t *testing.T) {
	db := setupTestDB(t)
	im := NewInventoryManager(db)
	ctx := t.Context()

	const orders = 10 // 在庫 5 に対して 1 個ずつ 10 件
	var wg sync.WaitGroup
	errs := make(chan error, orders)
	for n := range orders {
		wg.Go(func() {
			errs <- im.ProcessOrder(ctx, &OrderRequest{ItemID: 1, Quantity: 1, OrderID: fmt.Sprintf("ORDER-%d", n)})
		})
	}
	wg.Wait()
	close(errs)

	succeeded, outOfStock := 0, 0
	for err := range errs {
		switch {
		case err == nil:
			succeeded++
		case errors.Is(err, ErrInsufficientStock):
			outOfStock++
		default:
			t.Errorf("想定外のエラー: %v", err)
		}
	}
	if succeeded != 5 || outOfStock != 5 {
		t.Errorf("成功 %d 件・在庫切れ %d 件, 期待値 5 件・5 件", succeeded, outOfStock)
	}

	item, err := im.GetInventoryStatus(ctx, 1)
	if err != nil {
		t.Fatalf("在庫の取得に失敗した: %v", err)
	}
	if item.Quantity != 0 {
		t.Errorf("最終在庫 = %d, 期待値 0", item.Quantity)
	}

	var history int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM order_history").Scan(&history); err != nil {
		t.Fatalf("注文履歴の件数取得に失敗した: %v", err)
	}
	if history != 5 {
		t.Errorf("注文履歴 = %d 件, 期待値 5 件", history)
	}

	m := im.GetMetrics()
	if m.SuccessfulOrders != 5 || m.StockOutErrors != 5 {
		t.Errorf("メトリクス = %+v, 期待値 成功 5・在庫切れ 5", m)
	}
}
