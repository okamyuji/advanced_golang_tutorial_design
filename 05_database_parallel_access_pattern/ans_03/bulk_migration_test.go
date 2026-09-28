package main

import (
	"database/sql"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

// setupTestDB 移行元と移行先のテーブルを作り、移行元に rows 行を入れた接続を返す
func setupTestDB(t *testing.T, rows int) *sql.DB {
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

	// NUMERIC・TIMESTAMP・NULL を含めて、lib/pq で読んだ値をそのまま書き戻せることを確かめる
	_, err = db.ExecContext(ctx, `
		CREATE TABLE source_items (
			id BIGINT PRIMARY KEY, name TEXT NOT NULL, price NUMERIC(10,2) NOT NULL,
			created_at TIMESTAMP NOT NULL, note TEXT);
		CREATE TABLE target_items (
			id BIGINT PRIMARY KEY, name TEXT NOT NULL, price NUMERIC(10,2) NOT NULL,
			created_at TIMESTAMP NOT NULL, note TEXT);`)
	if err != nil {
		t.Fatalf("スキーマ作成に失敗した: %v", err)
	}
	_, err = db.ExecContext(ctx, `
		INSERT INTO source_items (id, name, price, created_at, note)
		SELECT g, 'item-' || g, g * 1.25, TIMESTAMP '2026-01-01 00:00:00' + g * INTERVAL '1 minute',
		       CASE WHEN g % 3 = 0 THEN NULL ELSE 'note-' || g END
		FROM generate_series(1, $1) AS g`, rows)
	if err != nil {
		t.Fatalf("移行元データの投入に失敗した: %v", err)
	}
	return db
}

func newTestMigrationManager(db *sql.DB) *MigrationManager {
	return NewMigrationManager(db, &MigrationConfig{
		SourceTable:    "source_items",
		TargetTable:    "target_items",
		ChunkSize:      10,
		MaxConcurrency: 2,
		// テスト中に定期処理が走らないよう長めにする（0 だと time.Tick が nil を返し永久に発火しない）
		ProgressInterval:   time.Hour,
		CheckpointInterval: time.Hour,
		ResourceThresholds: ResourceThresholds{MaxMemoryPercent: 100, MaxConnections: 1000},
	})
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

func TestStartMigration_CopiesAllRowsAndIsIdempotent(t *testing.T) {
	const rows = 25
	db := setupTestDB(t, rows)

	// 2 回目は全チャンクが完了済みなので何もせず成功する
	for run := range 2 {
		if err := newTestMigrationManager(db).StartMigration(t.Context()); err != nil {
			t.Fatalf("%d 回目の移行が失敗した: %v", run+1, err)
		}

		if got := queryInt(t, db, "SELECT COUNT(*) FROM target_items"); got != rows {
			t.Errorf("%d 回目: 移行先 = %d 行, 期待値 %d 行", run+1, got, rows)
		}
		matched := queryInt(t, db, `
			SELECT COUNT(*) FROM source_items s JOIN target_items t USING (id)
			WHERE s.name = t.name AND s.price = t.price AND s.created_at = t.created_at
			  AND s.note IS NOT DISTINCT FROM t.note`)
		if matched != rows {
			t.Errorf("%d 回目: 内容が一致した行 = %d, 期待値 %d", run+1, matched, rows)
		}
	}

	// ID 1〜25 をチャンクサイズ 10 で分けるので 3 チャンク
	if got := queryInt(t, db, "SELECT COUNT(*) FROM migration_chunks WHERE status = 'COMPLETED'"); got != 3 {
		t.Errorf("完了チャンク = %d, 期待値 3", got)
	}
}

func TestStartMigration_EmptySourceSucceeds(t *testing.T) {
	db := setupTestDB(t, 0)

	if err := newTestMigrationManager(db).StartMigration(t.Context()); err != nil {
		t.Fatalf("空の移行元で失敗した: %v", err)
	}
	if got := queryInt(t, db, "SELECT COUNT(*) FROM target_items"); got != 0 {
		t.Errorf("移行先 = %d 行, 期待値 0 行", got)
	}
}
