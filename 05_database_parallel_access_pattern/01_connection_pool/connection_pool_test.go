package main

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// setupTestDB テスト用のPostgreSQLコンテナを起動し、初期化されたDatabaseを返す
func setupTestDB(t *testing.T) (*Database, func()) {
	ctx := t.Context()

	// PostgreSQLコンテナを起動
	pgContainer, err := postgres.Run(ctx,
		"postgres:15-alpine",
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

	// データベース設定
	config := &DBConfig{
		DSN:             fmt.Sprintf("postgres://testuser:testpass@%s:%s/testdb?sslmode=disable", host, port.Port()),
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: 30 * time.Minute,
		ConnMaxIdleTime: 5 * time.Minute,
		PingInterval:    10 * time.Second,
	}

	// Database インスタンス作成
	db, err := NewDatabase(config)
	if err != nil {
		if termErr := pgContainer.Terminate(ctx); termErr != nil {
			t.Logf("Failed to terminate container: %v", termErr)
		}
		t.Fatalf("Failed to create database: %v", err)
	}

	// テスト用テーブル作成
	_, err = db.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS products (
			id SERIAL PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			price DECIMAL(10,2) NOT NULL,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)
	`)
	if err != nil {
		if closeErr := db.Close(); closeErr != nil {
			t.Logf("Failed to close database: %v", closeErr)
		}
		if termErr := pgContainer.Terminate(ctx); termErr != nil {
			t.Logf("Failed to terminate container: %v", termErr)
		}
		t.Fatalf("Failed to create test table: %v", err)
	}

	// テストデータ挿入（ID 1〜10）
	for i := range 10 {
		n := i + 1
		_, err = db.Exec(ctx,
			"INSERT INTO products (name, price) VALUES ($1, $2)",
			fmt.Sprintf("Product %d", n), float64(n)*10.5)
		if err != nil {
			if closeErr := db.Close(); closeErr != nil {
				t.Logf("Failed to close database: %v", closeErr)
			}
			if termErr := pgContainer.Terminate(ctx); termErr != nil {
				t.Logf("Failed to terminate container: %v", termErr)
			}
			t.Fatalf("Failed to insert test data: %v", err)
		}
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

func TestNewDatabase_WithRealPostgreSQL(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// 接続テスト
	err := db.ping()
	if err != nil {
		t.Fatalf("Failed to ping database: %v", err)
	}

	// 統計の初期化確認
	stats := db.GetStats()
	if stats.QueryCount < 0 {
		t.Error("QueryCount should be >= 0")
	}
}

func TestDatabase_Query_WithRealData(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := t.Context()

	// クエリ実行
	rows, err := db.Query(ctx, "SELECT id, name, price FROM products WHERE price > $1 ORDER BY id LIMIT 3", 50.0)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			t.Logf("Failed to close rows: %v", err)
		}
	}()

	count := 0
	var products []struct {
		ID    int
		Name  string
		Price float64
	}

	for rows.Next() {
		var p struct {
			ID    int
			Name  string
			Price float64
		}
		err := rows.Scan(&p.ID, &p.Name, &p.Price)
		if err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		products = append(products, p)
		count++
	}

	if count != 3 {
		t.Errorf("Expected 3 rows, got %d", count)
	}

	// 価格が期待値以上であることを確認
	for _, p := range products {
		if p.Price <= 50.0 {
			t.Errorf("Product %d price %f should be > 50.0", p.ID, p.Price)
		}
	}

	// 統計確認
	stats := db.GetStats()
	if stats.QueryCount == 0 {
		t.Error("QueryCount should be > 0 after query")
	}
}

func TestDatabase_Transaction_WithRealPostgreSQL(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := t.Context()

	// トランザクション開始
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx failed: %v", err)
	}

	// トランザクション内で価格更新
	_, err = tx.ExecContext(ctx, "UPDATE products SET price = price * 1.1 WHERE id = $1", 1)
	if err != nil {
		t.Fatalf("Transaction update failed: %v", err)
	}

	// まだコミットしていないので、別の接続からは元の値が見える
	var originalPrice float64
	row := db.QueryRow(ctx, "SELECT price FROM products WHERE id = $1", 1)
	err = row.Scan(&originalPrice)
	if err != nil {
		t.Fatalf("Query original price failed: %v", err)
	}

	// コミット
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// コミット後は更新された値が見える
	var updatedPrice float64
	row = db.QueryRow(ctx, "SELECT price FROM products WHERE id = $1", 1)
	err = row.Scan(&updatedPrice)
	if err != nil {
		t.Fatalf("Query updated price failed: %v", err)
	}

	expectedPrice := originalPrice * 1.1
	if updatedPrice != expectedPrice {
		t.Errorf("Expected price %f, got %f", expectedPrice, updatedPrice)
	}
}

func TestDatabase_ConcurrentQueries(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := t.Context()
	const numGoroutines = 10
	const queriesPerGoroutine = 5

	var wg sync.WaitGroup
	errCh := make(chan error, numGoroutines)

	for goroutineID := range numGoroutines {
		wg.Go(func() {
			for j := range queriesPerGoroutine {
				// 異なるクエリパターンを実行
				switch j % 3 {
				case 0:
					// SELECT クエリ
					rows, err := db.Query(ctx, "SELECT COUNT(*) FROM products WHERE price > $1", float64(goroutineID*10))
					if err != nil {
						errCh <- fmt.Errorf("goroutine %d query failed: %w", goroutineID, err)
						return
					}
					var count int
					if rows.Next() {
						if err := rows.Scan(&count); err != nil {
							t.Logf("Failed to scan row: %v", err)
						}
					}
					if err := rows.Close(); err != nil {
						t.Logf("Failed to close rows: %v", err)
					}

				case 1:
					// UPDATE クエリ
					_, err := db.Exec(ctx, "UPDATE products SET name = $1 WHERE id = $2",
						fmt.Sprintf("Updated by goroutine %d", goroutineID), (goroutineID%10)+1)
					if err != nil {
						errCh <- fmt.Errorf("goroutine %d update failed: %w", goroutineID, err)
						return
					}

				case 2:
					// INSERT クエリ（一時的な行）
					_, err := db.Exec(ctx, "INSERT INTO products (name, price) VALUES ($1, $2)",
						fmt.Sprintf("Temp product %d-%d", goroutineID, j), float64(goroutineID+j))
					if err != nil {
						errCh <- fmt.Errorf("goroutine %d insert failed: %w", goroutineID, err)
						return
					}
				}

				// 短時間待機
				time.Sleep(10 * time.Millisecond)
			}
		})
	}

	wg.Wait()
	close(errCh)

	// エラーがないことを確認
	for err := range errCh {
		t.Errorf("Concurrent query error: %v", err)
	}

	// 統計確認
	stats := db.GetStats()
	expectedMinQueries := int64(numGoroutines * queriesPerGoroutine)
	if stats.QueryCount < expectedMinQueries {
		t.Errorf("Expected at least %d queries, got %d", expectedMinQueries, stats.QueryCount)
	}

	// 平均応答時間が設定されていることを確認
	if stats.AvgResponseTime <= 0 {
		t.Error("Average response time should be positive")
	}
}

func TestDatabase_ConnectionPoolStats(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := t.Context()

	// 複数の並行接続を作成
	const numConnections = 8
	var wg sync.WaitGroup

	for id := range numConnections {
		wg.Go(func() {
			// 長時間実行クエリ
			rows, err := db.Query(ctx, "SELECT pg_sleep(0.1), id FROM products WHERE id = $1", id%10+1)
			if err != nil {
				t.Errorf("Long query failed: %v", err)
				return
			}

			for rows.Next() {
				var sleepResult any
				var id int
				if err := rows.Scan(&sleepResult, &id); err != nil {
					t.Logf("Failed to scan row: %v", err)
				}
			}
			if err := rows.Close(); err != nil {
				t.Logf("Failed to close rows: %v", err)
			}
		})
	}

	wg.Wait()

	// 統計確認
	stats := db.GetStats()
	if stats.QueryCount < int64(numConnections) {
		t.Errorf("Expected at least %d queries, got %d", numConnections, stats.QueryCount)
	}

	if stats.TotalQueryTime <= 0 {
		t.Error("Total query time should be positive")
	}
}

func TestDatabase_ErrorHandling(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := t.Context()

	// 意図的にエラーを発生させる
	_, err := db.Query(ctx, "SELECT * FROM nonexistent_table")
	if err == nil {
		t.Error("Expected error for nonexistent table")
	}

	// 統計でエラーがカウントされていることを確認
	stats := db.GetStats()
	if stats.ErrorCount == 0 {
		t.Error("Error count should be > 0 after failed query")
	}

	// 正常なクエリも実行して、エラー後でも動作することを確認
	rows, err := db.Query(ctx, "SELECT COUNT(*) FROM products")
	if err != nil {
		t.Fatalf("Normal query after error failed: %v", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			t.Logf("Failed to close rows: %v", err)
		}
	}()

	var count int
	if rows.Next() {
		err = rows.Scan(&count)
		if err != nil {
			t.Fatalf("Scan after error recovery failed: %v", err)
		}
	}

	if count != 10 {
		t.Errorf("Expected 10 products, got %d", count)
	}
}

func TestDatabase_CloseConcurrentWithQueries(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := t.Context()

	// Close と並行して投入しても panic しない（結果は成功・失敗のどちらでもよい）
	var wg sync.WaitGroup
	for range 50 {
		wg.Go(func() {
			rows, err := db.Query(ctx, "SELECT id FROM products LIMIT 1")
			if err != nil {
				return
			}
			if err := rows.Close(); err != nil {
				t.Logf("Failed to close rows: %v", err)
			}
		})
	}
	wg.Go(func() {
		if err := db.Close(); err != nil {
			t.Errorf("Close failed: %v", err)
		}
	})
	wg.Wait()

	// 停止後の投入はエラーになる
	if _, err := db.Query(ctx, "SELECT 1"); err == nil {
		t.Error("Query after Close should fail")
	}
}
