package main

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// setupTestDB はテスト用のPostgreSQLコンテナを起動し、初期化されたデータベースを返す
func setupTestDB(t *testing.T) (*sql.DB, func()) {
	ctx := context.Background()

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

// createTestTables はテスト用テーブルを作成
func createTestTables(db *sql.DB) error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS products (
			id SERIAL PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			price DECIMAL(10,2) NOT NULL,
			category_id INTEGER NOT NULL,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE TABLE IF NOT EXISTS categories (
			id SERIAL PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE TABLE IF NOT EXISTS batch_test_items (
			id SERIAL PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			value INTEGER NOT NULL,
			processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`,
	}

	for _, query := range queries {
		_, err := db.Exec(query)
		if err != nil {
			return fmt.Errorf("failed to create table: %v", err)
		}
	}

	// テスト用カテゴリデータ
	_, err := db.Exec(`
		INSERT INTO categories (name) VALUES 
		('Electronics'),
		('Books'),
		('Clothing'),
		('Home & Garden'),
		('Sports')
	`)
	if err != nil {
		return fmt.Errorf("failed to insert test categories: %v", err)
	}

	return nil
}

// TestRecord はテスト用のレコード実装
type TestRecord struct {
	ID    int64
	Name  string
	Value int
}

func (tr *TestRecord) GetID() int64 {
	return tr.ID
}

func (tr *TestRecord) Validate() error {
	if tr.Name == "" {
		return fmt.Errorf("name is required")
	}
	if tr.Value < 0 {
		return fmt.Errorf("value must be non-negative")
	}
	return nil
}

func TestBatchProcessor_BasicProcessing(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	config := &BatchConfig{
		ChunkSize:        10,
		Concurrency:      2,
		BatchTimeout:     10 * time.Second,
		RetryAttempts:    2,
		RetryDelay:       100 * time.Millisecond,
		ProgressInterval: 1 * time.Second,
	}

	bp := NewBatchProcessor(db, config)
	ctx := context.Background()

	// テストレコード作成
	const numRecords = 25
	records := make([]Record, numRecords)
	for i := 0; i < numRecords; i++ {
		records[i] = &TestRecord{
			ID:    int64(i + 1),
			Name:  fmt.Sprintf("Test Item %d", i+1),
			Value: (i + 1) * 10,
		}
	}

	// バッチ処理実行
	processFn := func(ctx context.Context, tx *sql.Tx, records []Record) error {
		for _, record := range records {
			testRecord := record.(*TestRecord)
			_, err := tx.ExecContext(ctx,
				"INSERT INTO batch_test_items (name, value) VALUES ($1, $2)",
				testRecord.Name, testRecord.Value)
			if err != nil {
				return fmt.Errorf("failed to insert record %d: %v", testRecord.ID, err)
			}
		}
		return nil
	}

	err := bp.ProcessBatch(ctx, records, processFn)
	if err != nil {
		t.Fatalf("Batch processing should succeed: %v", err)
	}

	// 結果確認
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM batch_test_items").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count inserted records: %v", err)
	}

	if count != numRecords {
		t.Errorf("Expected %d records, got %d", numRecords, count)
	}

	// 統計確認
	progress := bp.GetProgress()
	stats := bp.GetStats()

	if progress.TotalRecords != numRecords {
		t.Errorf("Expected %d total records in progress, got %d", numRecords, progress.TotalRecords)
	}

	if progress.SuccessRecords != numRecords {
		t.Errorf("Expected %d successful records, got %d", numRecords, progress.SuccessRecords)
	}

	if stats.CompletedBatches == 0 {
		t.Error("Should have completed batches")
	}

	t.Logf("Batch processing stats: %+v", stats)
	t.Logf("Progress: %+v", progress)
}

func TestBatchProcessor_WithFailures(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	config := &BatchConfig{
		ChunkSize:        5,
		Concurrency:      2,
		BatchTimeout:     10 * time.Second,
		RetryAttempts:    1,
		RetryDelay:       50 * time.Millisecond,
		ProgressInterval: 1 * time.Second,
	}

	bp := NewBatchProcessor(db, config)
	ctx := context.Background()

	// テストレコード作成（いくつかは無効なデータ）
	records := make([]Record, 10)
	for i := 0; i < 10; i++ {
		if i%3 == 0 {
			// 無効なレコード（空の名前）
			records[i] = &TestRecord{
				ID:    int64(i + 1),
				Name:  "", // バリデーションエラーを発生させる
				Value: i + 1,
			}
		} else {
			records[i] = &TestRecord{
				ID:    int64(i + 1),
				Name:  fmt.Sprintf("Valid Item %d", i+1),
				Value: i + 1,
			}
		}
	}

	// バッチ処理実行
	processFn := func(ctx context.Context, tx *sql.Tx, records []Record) error {
		for _, record := range records {
			testRecord := record.(*TestRecord)
			_, err := tx.ExecContext(ctx,
				"INSERT INTO batch_test_items (name, value) VALUES ($1, $2)",
				testRecord.Name, testRecord.Value)
			if err != nil {
				return fmt.Errorf("failed to insert record %d: %v", testRecord.ID, err)
			}
		}
		return nil
	}

	err := bp.ProcessBatch(ctx, records, processFn)
	if err == nil {
		t.Error("Batch processing should fail due to validation errors")
	}

	// 統計確認
	progress := bp.GetProgress()
	stats := bp.GetStats()

	if progress.FailedRecords == 0 {
		t.Error("Should have failed records")
	}

	if stats.FailedBatches == 0 {
		t.Error("Should have failed batches")
	}

	if stats.TotalRetries == 0 {
		t.Error("Should have retry attempts")
	}

	t.Logf("Batch processing with failures - Progress: %+v", progress)
	t.Logf("Batch processing with failures - Stats: %+v", stats)
}

func TestBatchProcessor_Concurrency(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	config := &BatchConfig{
		ChunkSize:        5,
		Concurrency:      4,
		BatchTimeout:     10 * time.Second,
		RetryAttempts:    1,
		RetryDelay:       50 * time.Millisecond,
		ProgressInterval: 500 * time.Millisecond,
	}

	bp := NewBatchProcessor(db, config)
	ctx := context.Background()

	// 大量のテストレコード作成
	const numRecords = 100
	records := make([]Record, numRecords)
	for i := 0; i < numRecords; i++ {
		records[i] = &TestRecord{
			ID:    int64(i + 1),
			Name:  fmt.Sprintf("Concurrent Item %d", i+1),
			Value: i + 1,
		}
	}

	// 処理時間を測定
	start := time.Now()

	// バッチ処理実行
	processFn := func(ctx context.Context, tx *sql.Tx, records []Record) error {
		// 処理時間をシミュレート
		time.Sleep(10 * time.Millisecond)

		for _, record := range records {
			testRecord := record.(*TestRecord)
			_, err := tx.ExecContext(ctx,
				"INSERT INTO batch_test_items (name, value) VALUES ($1, $2)",
				testRecord.Name, testRecord.Value)
			if err != nil {
				return fmt.Errorf("failed to insert record %d: %v", testRecord.ID, err)
			}
		}
		return nil
	}

	err := bp.ProcessBatch(ctx, records, processFn)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Batch processing should succeed: %v", err)
	}

	// 結果確認
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM batch_test_items").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count inserted records: %v", err)
	}

	if count != numRecords {
		t.Errorf("Expected %d records, got %d", numRecords, count)
	}

	// 統計確認
	progress := bp.GetProgress()
	stats := bp.GetStats()

	if progress.CurrentThroughput <= 0 {
		t.Error("Throughput should be positive")
	}

	expectedChunks := (numRecords + config.ChunkSize - 1) / config.ChunkSize
	if stats.CompletedBatches != int64(expectedChunks) {
		t.Errorf("Expected %d completed batches, got %d", expectedChunks, stats.CompletedBatches)
	}

	t.Logf("Processed %d records in %v (%.2f rec/sec)", numRecords, elapsed, progress.CurrentThroughput)
	t.Logf("Concurrent processing stats: %+v", stats)
}

func TestBatchProcessor_Timeout(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	config := &BatchConfig{
		ChunkSize:        5,
		Concurrency:      1,
		BatchTimeout:     100 * time.Millisecond, // 短いタイムアウト
		RetryAttempts:    0,
		RetryDelay:       50 * time.Millisecond,
		ProgressInterval: 1 * time.Second,
	}

	bp := NewBatchProcessor(db, config)
	ctx := context.Background()

	// テストレコード作成
	records := make([]Record, 5)
	for i := 0; i < 5; i++ {
		records[i] = &TestRecord{
			ID:    int64(i + 1),
			Name:  fmt.Sprintf("Timeout Test Item %d", i+1),
			Value: i + 1,
		}
	}

	// バッチ処理実行（意図的に長時間実行）
	processFn := func(ctx context.Context, tx *sql.Tx, records []Record) error {
		// タイムアウトを発生させるために長時間待機
		time.Sleep(200 * time.Millisecond)
		return nil
	}

	start := time.Now()
	err := bp.ProcessBatch(ctx, records, processFn)
	elapsed := time.Since(start)

	if err == nil {
		t.Error("Batch processing should timeout")
	}

	// タイムアウト時間内で終了していることを確認
	if elapsed > 300*time.Millisecond {
		t.Errorf("Batch processing should timeout quickly, took %v", elapsed)
	}

	t.Logf("Batch processing correctly timed out after %v", elapsed)
}

func TestBulkInsertProcessor_Products(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	bip := NewBulkInsertProcessor(db)
	ctx := context.Background()

	// テスト用商品データ
	products := []*ProductRecord{
		{Name: "Bulk Product 1", Price: 1000.0, CategoryID: 1},
		{Name: "Bulk Product 2", Price: 2000.0, CategoryID: 2},
		{Name: "Bulk Product 3", Price: 3000.0, CategoryID: 3},
		{Name: "Bulk Product 4", Price: 4000.0, CategoryID: 4},
		{Name: "Bulk Product 5", Price: 5000.0, CategoryID: 5},
	}

	start := time.Now()
	err := bip.BulkInsertProducts(ctx, products)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Bulk insert should succeed: %v", err)
	}

	// 結果確認
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM products").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count inserted products: %v", err)
	}

	if count != len(products) {
		t.Errorf("Expected %d products, got %d", len(products), count)
	}

	// 挿入されたデータの確認
	rows, err := db.Query("SELECT name, price, category_id FROM products ORDER BY id")
	if err != nil {
		t.Fatalf("Failed to query inserted products: %v", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			t.Logf("Failed to close rows: %v", err)
		}
	}()

	i := 0
	for rows.Next() {
		var name string
		var price float64
		var categoryID int64

		err := rows.Scan(&name, &price, &categoryID)
		if err != nil {
			t.Fatalf("Failed to scan product: %v", err)
		}

		if i < len(products) {
			expected := products[i]
			if name != expected.Name || price != expected.Price || categoryID != expected.CategoryID {
				t.Errorf("Product %d mismatch: expected (%s, %f, %d), got (%s, %f, %d)",
					i, expected.Name, expected.Price, expected.CategoryID, name, price, categoryID)
			}
		}
		i++
	}

	t.Logf("Bulk inserted %d products in %v", len(products), elapsed)
}

func TestBulkInsertProcessor_LargeDataset(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	bip := NewBulkInsertProcessor(db)
	ctx := context.Background()

	// 大量のテスト用商品データ
	const numProducts = 1000
	products := make([]*ProductRecord, numProducts)
	categories := []int64{1, 2, 3, 4, 5}

	for i := 0; i < numProducts; i++ {
		products[i] = &ProductRecord{
			Name:       fmt.Sprintf("Large Dataset Product %d", i+1),
			Price:      float64((i%100 + 1) * 100),
			CategoryID: categories[i%len(categories)],
		}
	}

	start := time.Now()
	err := bip.BulkInsertProducts(ctx, products)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Large bulk insert should succeed: %v", err)
	}

	// 結果確認
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM products").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count inserted products: %v", err)
	}

	if count != numProducts {
		t.Errorf("Expected %d products, got %d", numProducts, count)
	}

	// パフォーマンス確認
	throughput := float64(numProducts) / elapsed.Seconds()
	t.Logf("Bulk inserted %d products in %v (%.0f products/sec)", numProducts, elapsed, throughput)

	if throughput < 100 {
		t.Logf("Warning: Low throughput %.0f products/sec", throughput)
	}
}

func TestBulkInsertProcessor_ValidationError(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	bip := NewBulkInsertProcessor(db)
	ctx := context.Background()

	// 無効なデータを含む商品データ
	products := []*ProductRecord{
		{Name: "Valid Product 1", Price: 1000.0, CategoryID: 1},
		{Name: "", Price: 2000.0, CategoryID: 2}, // 無効な名前
		{Name: "Valid Product 3", Price: 3000.0, CategoryID: 3},
	}

	err := bip.BulkInsertProducts(ctx, products)
	if err == nil {
		t.Error("Bulk insert should fail due to validation error")
	}

	// エラーが正しく検出されることを確認
	if err.Error() == "" {
		t.Error("Error message should not be empty")
	}

	t.Logf("Validation error correctly detected: %v", err)

	// 部分的な挿入が発生していないことを確認
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM products").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count products: %v", err)
	}

	if count != 0 {
		t.Errorf("No products should be inserted due to validation failure, got %d", count)
	}
}

func TestBatchProcessor_Progress_Monitoring(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	config := &BatchConfig{
		ChunkSize:        10,
		Concurrency:      2,
		BatchTimeout:     10 * time.Second,
		RetryAttempts:    1,
		RetryDelay:       100 * time.Millisecond,
		ProgressInterval: 200 * time.Millisecond, // 短い間隔で進捗監視
	}

	bp := NewBatchProcessor(db, config)
	ctx := context.Background()

	// テストレコード作成
	const numRecords = 50
	records := make([]Record, numRecords)
	for i := 0; i < numRecords; i++ {
		records[i] = &TestRecord{
			ID:    int64(i + 1),
			Name:  fmt.Sprintf("Progress Test Item %d", i+1),
			Value: i + 1,
		}
	}

	// 進捗監視用チャンネル
	progressUpdates := make(chan BatchProgressSnapshot, 10)
	progressCtx, progressCancel := context.WithCancel(ctx)

	// 進捗監視のgoroutine
	go func() {
		ticker := time.NewTicker(300 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-progressCtx.Done():
				return
			case <-ticker.C:
				progress := bp.GetProgress()
				if progress.ProcessedRecords > 0 {
					select {
					case progressUpdates <- progress:
					case <-progressCtx.Done():
						return
					}
				}
			}
		}
	}()

	// バッチ処理実行
	processFn := func(ctx context.Context, tx *sql.Tx, records []Record) error {
		// 処理時間をシミュレート
		time.Sleep(200 * time.Millisecond)

		for _, record := range records {
			testRecord := record.(*TestRecord)
			_, err := tx.ExecContext(ctx,
				"INSERT INTO batch_test_items (name, value) VALUES ($1, $2)",
				testRecord.Name, testRecord.Value)
			if err != nil {
				return fmt.Errorf("failed to insert record %d: %v", testRecord.ID, err)
			}
		}
		return nil
	}

	err := bp.ProcessBatch(ctx, records, processFn)
	if err != nil {
		t.Fatalf("Batch processing should succeed: %v", err)
	}

	// 進捗監視停止
	progressCancel()
	time.Sleep(100 * time.Millisecond) // goroutineの停止を待つ

	// 進捗更新を確認
	close(progressUpdates)
	updateCount := 0

	for progress := range progressUpdates {
		updateCount++
		t.Logf("Progress update %d: %.1f%% (%d/%d) - Throughput: %.1f rec/sec",
			updateCount,
			float64(progress.ProcessedRecords)/float64(progress.TotalRecords)*100,
			progress.ProcessedRecords,
			progress.TotalRecords,
			progress.CurrentThroughput)
	}

	if updateCount == 0 {
		t.Error("Should have received progress updates")
	}

	// 最終進捗確認
	finalProgress := bp.GetProgress()
	if finalProgress.ProcessedRecords != numRecords {
		t.Errorf("Expected %d processed records, got %d", numRecords, finalProgress.ProcessedRecords)
	}

	if finalProgress.SuccessRecords != numRecords {
		t.Errorf("Expected %d successful records, got %d", numRecords, finalProgress.SuccessRecords)
	}

	if finalProgress.CurrentThroughput <= 0 {
		t.Error("Final throughput should be positive")
	}

	t.Logf("Final progress: %+v", finalProgress)
}
