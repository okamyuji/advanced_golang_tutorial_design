// 01_connection_pool.go
// 高性能コネクションプール管理システム
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/lib/pq"
)

// DBConfig はデータベース設定を管理します
type DBConfig struct {
	DSN             string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	ConnMaxIdleTime time.Duration
	PingInterval    time.Duration
}

// Database はコネクションプール付きデータベースラッパーです
type Database struct {
	db     *sql.DB
	config *DBConfig
	stats  *DatabaseStats
}

// DatabaseStats はデータベース統計情報を管理します
type DatabaseStats struct {
	mu               sync.RWMutex
	queryCount       int64
	errorCount       int64
	totalQueryTime   time.Duration
	connectionErrors int64
	lastHealthCheck  time.Time
	avgResponseTime  time.Duration
}

// NewDatabase は新しいデータベースインスタンスを作成します
func NewDatabase(config *DBConfig) (*Database, error) {
	db, err := sql.Open("postgres", config.DSN)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %v", err)
	}

	// コネクションプール設定
	db.SetMaxOpenConns(config.MaxOpenConns)
	db.SetMaxIdleConns(config.MaxIdleConns)
	db.SetConnMaxLifetime(config.ConnMaxLifetime)
	db.SetConnMaxIdleTime(config.ConnMaxIdleTime)

	database := &Database{
		db:     db,
		config: config,
		stats:  &DatabaseStats{},
	}

	// 初期接続テスト
	if err := database.ping(); err != nil {
		if closeErr := db.Close(); closeErr != nil {
			log.Printf("Failed to close database connection: %v", closeErr)
		}
		return nil, fmt.Errorf("initial connection test failed: %v", err)
	}

	return database, nil
}

// NewDatabaseSQLite はSQLite用のデータベースインスタンスを作成（テスト用）
func NewDatabaseSQLite(config *DBConfig) (*Database, error) {
	// SQLiteの場合は "sqlite3" ドライバーを使用
	db, err := sql.Open("sqlite3", config.DSN)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %v", err)
	}

	// コネクションプール設定
	db.SetMaxOpenConns(config.MaxOpenConns)
	db.SetMaxIdleConns(config.MaxIdleConns)
	db.SetConnMaxLifetime(config.ConnMaxLifetime)
	db.SetConnMaxIdleTime(config.ConnMaxIdleTime)

	database := &Database{
		db:     db,
		config: config,
		stats:  &DatabaseStats{},
	}

	// 初期接続テスト
	if err := database.ping(); err != nil {
		if closeErr := db.Close(); closeErr != nil {
			log.Printf("Failed to close database connection: %v", closeErr)
		}
		return nil, fmt.Errorf("initial connection test failed: %v", err)
	}

	return database, nil
}

// ping はデータベース接続をテストします
func (d *Database) ping() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	start := time.Now()
	err := d.db.PingContext(ctx)
	elapsed := time.Since(start)

	d.stats.mu.Lock()
	d.stats.lastHealthCheck = time.Now()
	if err != nil {
		atomic.AddInt64(&d.stats.connectionErrors, 1)
	} else {
		d.updateResponseTime(elapsed)
	}
	d.stats.mu.Unlock()

	return err
}

// updateResponseTime は平均応答時間を更新します
func (d *Database) updateResponseTime(elapsed time.Duration) {
	queryCount := atomic.LoadInt64(&d.stats.queryCount)
	if queryCount == 0 {
		d.stats.avgResponseTime = elapsed
	} else {
		// 移動平均の計算
		currentAvg := int64(d.stats.avgResponseTime)
		newAvg := (currentAvg*queryCount + int64(elapsed)) / (queryCount + 1)
		d.stats.avgResponseTime = time.Duration(newAvg)
	}
}

// MonitorConnectionPool は接続プールを監視します
func (d *Database) MonitorConnectionPool(ctx context.Context) {
	ticker := time.NewTicker(d.config.PingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("Connection pool monitor stopping")
			return
		case <-ticker.C:
			d.healthCheck()
			d.logPoolStats()
			d.adjustPoolIfNeeded()
		}
	}
}

// healthCheck はヘルスチェックを実行します
func (d *Database) healthCheck() {
	if err := d.ping(); err != nil {
		log.Printf("Database health check failed: %v", err)
	}
}

// logPoolStats は接続プールの統計をログ出力します
func (d *Database) logPoolStats() {
	stats := d.db.Stats()
	dbStats := d.GetStats()

	log.Printf("Connection Pool Stats: Open=%d, InUse=%d, Idle=%d, "+
		"WaitCount=%d, WaitDuration=%v, MaxIdleClosed=%d, MaxLifetimeClosed=%d",
		stats.OpenConnections, stats.InUse, stats.Idle,
		stats.WaitCount, stats.WaitDuration,
		stats.MaxIdleClosed, stats.MaxLifetimeClosed)

	log.Printf("Database Stats: Queries=%d, Errors=%d, AvgResponseTime=%v, ConnectionErrors=%d",
		dbStats.QueryCount, dbStats.ErrorCount, dbStats.AvgResponseTime, dbStats.ConnectionErrors)
}

// adjustPoolIfNeeded は必要に応じて接続プールを調整します
func (d *Database) adjustPoolIfNeeded() {
	stats := d.db.Stats()

	// 待機が多い場合の警告
	if stats.WaitCount > 0 && stats.WaitDuration > 100*time.Millisecond {
		log.Printf("High connection wait detected: Count=%d, Duration=%v. Consider increasing MaxOpenConns",
			stats.WaitCount, stats.WaitDuration)
	}

	// アイドル接続が多すぎる場合の警告
	idleRatio := float64(stats.Idle) / float64(stats.OpenConnections)
	if stats.OpenConnections > d.config.MaxIdleConns && idleRatio > 0.8 {
		log.Printf("High idle connection ratio: %.2f. Consider decreasing MaxIdleConns", idleRatio)
	}
}

// Query はクエリを実行し統計を記録します
func (d *Database) Query(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	start := time.Now()
	rows, err := d.db.QueryContext(ctx, query, args...)
	elapsed := time.Since(start)

	// 統計更新
	atomic.AddInt64(&d.stats.queryCount, 1)
	if err != nil {
		atomic.AddInt64(&d.stats.errorCount, 1)
	}

	d.stats.mu.Lock()
	d.updateResponseTime(elapsed)
	d.stats.totalQueryTime += elapsed
	d.stats.mu.Unlock()

	return rows, err
}

// QueryRow は単一行クエリを実行します
func (d *Database) QueryRow(ctx context.Context, query string, args ...interface{}) *sql.Row {
	start := time.Now()
	row := d.db.QueryRowContext(ctx, query, args...)
	elapsed := time.Since(start)

	// 統計更新
	atomic.AddInt64(&d.stats.queryCount, 1)

	d.stats.mu.Lock()
	d.updateResponseTime(elapsed)
	d.stats.totalQueryTime += elapsed
	d.stats.mu.Unlock()

	return row
}

// Exec はクエリを実行します
func (d *Database) Exec(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	start := time.Now()
	result, err := d.db.ExecContext(ctx, query, args...)
	elapsed := time.Since(start)

	// 統計更新
	atomic.AddInt64(&d.stats.queryCount, 1)
	if err != nil {
		atomic.AddInt64(&d.stats.errorCount, 1)
	}

	d.stats.mu.Lock()
	d.updateResponseTime(elapsed)
	d.stats.totalQueryTime += elapsed
	d.stats.mu.Unlock()

	return result, err
}

// BeginTx はトランザクションを開始します
func (d *Database) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return d.db.BeginTx(ctx, opts)
}

// GetStats は統計情報を取得します
func (d *Database) GetStats() DatabaseStatsSnapshot {
	d.stats.mu.RLock()
	defer d.stats.mu.RUnlock()

	return DatabaseStatsSnapshot{
		QueryCount:       atomic.LoadInt64(&d.stats.queryCount),
		ErrorCount:       atomic.LoadInt64(&d.stats.errorCount),
		TotalQueryTime:   d.stats.totalQueryTime,
		ConnectionErrors: atomic.LoadInt64(&d.stats.connectionErrors),
		LastHealthCheck:  d.stats.lastHealthCheck,
		AvgResponseTime:  d.stats.avgResponseTime,
	}
}

// DatabaseStatsSnapshot は統計情報のスナップショットです
type DatabaseStatsSnapshot struct {
	QueryCount       int64
	ErrorCount       int64
	TotalQueryTime   time.Duration
	ConnectionErrors int64
	LastHealthCheck  time.Time
	AvgResponseTime  time.Duration
}

// Close はデータベース接続を閉じます
func (d *Database) Close() error {
	return d.db.Close()
}

// 使用例とテスト用のmain関数
func main() {
	// データベース設定
	config := &DBConfig{
		DSN:             "postgres://test_user:test_password@localhost:5432/advanced_golang_tutorial?sslmode=disable",
		MaxOpenConns:    25,
		MaxIdleConns:    5,
		ConnMaxLifetime: 30 * time.Minute,
		ConnMaxIdleTime: 5 * time.Minute,
		PingInterval:    30 * time.Second,
	}

	// データベース接続
	db, err := NewDatabase(config)
	if err != nil {
		log.Fatalf("Failed to create database: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Printf("Failed to close database: %v", err)
		}
	}()

	// 監視開始
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go db.MonitorConnectionPool(ctx)

	// テスト用クエリ実行
	testConnectionPool(db)

	// 30秒間統計を収集
	time.Sleep(30 * time.Second)

	// 最終統計表示
	finalStats := db.GetStats()
	fmt.Printf("Final Statistics:\n")
	fmt.Printf("  Total Queries: %d\n", finalStats.QueryCount)
	fmt.Printf("  Total Errors: %d\n", finalStats.ErrorCount)
	fmt.Printf("  Average Response Time: %v\n", finalStats.AvgResponseTime)
	fmt.Printf("  Connection Errors: %d\n", finalStats.ConnectionErrors)
}

// testConnectionPool はコネクションプールのテストを実行します
func testConnectionPool(db *Database) {
	ctx := context.Background()

	// 並行クエリの実行
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			for j := 0; j < 5; j++ {
				// 商品情報の取得
				rows, err := db.Query(ctx, "SELECT id, name, price FROM products LIMIT 10")
				if err != nil {
					log.Printf("Query failed: %v", err)
					continue
				}

				count := 0
				for rows.Next() {
					var id int
					var name string
					var price float64
					if err := rows.Scan(&id, &name, &price); err != nil {
						log.Printf("Scan failed: %v", err)
					}
					count++
				}
				if err := rows.Close(); err != nil {
					log.Printf("Failed to close rows: %v", err)
				}

				log.Printf("Worker %d: Query %d completed, got %d rows", id, j+1, count)
				time.Sleep(100 * time.Millisecond)
			}
		}(i)
	}

	wg.Wait()
	log.Println("All test queries completed")
}
