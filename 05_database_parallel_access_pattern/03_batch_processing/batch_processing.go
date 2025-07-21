// 03_batch_processing.go
// 高効率バッチ処理システム
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lib/pq"
)

// BatchConfig バッチ処理設定です
type BatchConfig struct {
	ChunkSize        int
	Concurrency      int
	BatchTimeout     time.Duration
	RetryAttempts    int
	RetryDelay       time.Duration
	ProgressInterval time.Duration
}

// BatchProgress バッチ処理の進捗情報です
type BatchProgress struct {
	mu                sync.RWMutex
	TotalRecords      int64
	ProcessedRecords  int64
	SuccessRecords    int64
	FailedRecords     int64
	StartTime         time.Time
	LastUpdateTime    time.Time
	EstimatedEndTime  time.Time
	CurrentThroughput float64
}

// BatchProcessor バッチ処理エンジンです
type BatchProcessor struct {
	db       *sql.DB
	config   *BatchConfig
	progress *BatchProgress
	stats    *BatchStats
}

// BatchStats バッチ処理統計です
type BatchStats struct {
	mu               sync.RWMutex
	totalBatches     int64
	completedBatches int64
	failedBatches    int64
	totalRetries     int64
	avgBatchTime     time.Duration
	maxBatchTime     time.Duration
	minBatchTime     time.Duration
}

// Record バッチ処理対象のレコードです
type Record interface {
	GetID() int64
	Validate() error
}

// ProductRecord 商品レコードです
type ProductRecord struct {
	ID         int64
	Name       string
	Price      float64
	CategoryID int64
}

func (p *ProductRecord) GetID() int64 {
	return p.ID
}

func (p *ProductRecord) Validate() error {
	if p.Name == "" {
		return fmt.Errorf("product name is required")
	}
	if p.Price < 0 {
		return fmt.Errorf("product price must be non-negative")
	}
	if p.CategoryID <= 0 {
		return fmt.Errorf("category ID must be positive")
	}
	return nil
}

// NewBatchProcessor 新しいバッチプロセッサーを作成します
func NewBatchProcessor(db *sql.DB, config *BatchConfig) *BatchProcessor {
	return &BatchProcessor{
		db:     db,
		config: config,
		progress: &BatchProgress{
			StartTime: time.Now(),
		},
		stats: &BatchStats{
			minBatchTime: time.Hour, // 初期値は大きな値
		},
	}
}

// ProcessBatch バッチ処理を実行します
func (bp *BatchProcessor) ProcessBatch(ctx context.Context, records []Record,
	processFn func(context.Context, *sql.Tx, []Record) error) error {

	// 進捗初期化
	bp.progress.mu.Lock()
	bp.progress.TotalRecords = int64(len(records))
	bp.progress.ProcessedRecords = 0
	bp.progress.SuccessRecords = 0
	bp.progress.FailedRecords = 0
	bp.progress.StartTime = time.Now()
	bp.progress.LastUpdateTime = time.Now()
	bp.progress.mu.Unlock()

	// 進捗監視開始
	progressCtx, progressCancel := context.WithCancel(ctx)
	defer progressCancel()
	go bp.monitorProgress(progressCtx)

	// レコードをチャンクに分割
	chunks := bp.createChunks(records)
	log.Printf("Processing %d records in %d chunks (chunk size: %d, concurrency: %d)",
		len(records), len(chunks), bp.config.ChunkSize, bp.config.Concurrency)

	// 並行処理制御用のセマフォ
	semaphore := make(chan struct{}, bp.config.Concurrency)
	var wg sync.WaitGroup
	errors := make(chan error, len(chunks))

	// 各チャンクを並行処理
	for i, chunk := range chunks {
		wg.Add(1)
		go func(chunkIndex int, chunkData []Record) {
			defer wg.Done()

			// セマフォで並行数制御
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			if err := bp.processChunk(ctx, chunkIndex, chunkData, processFn); err != nil {
				errors <- fmt.Errorf("chunk %d failed: %v", chunkIndex, err)
			}
		}(i, chunk)
	}

	// 全チャンク完了を待機
	wg.Wait()
	close(errors)

	// エラー収集
	var allErrors []error
	for err := range errors {
		allErrors = append(allErrors, err)
	}

	// 最終統計更新
	bp.updateFinalStats()

	if len(allErrors) > 0 {
		return fmt.Errorf("batch processing completed with %d errors: %v", len(allErrors), allErrors)
	}

	log.Printf("Batch processing completed successfully: %d records processed", len(records))
	return nil
}

// createChunks レコードをチャンクに分割します
func (bp *BatchProcessor) createChunks(records []Record) [][]Record {
	chunks := make([][]Record, 0)
	chunkSize := bp.config.ChunkSize

	for i := 0; i < len(records); i += chunkSize {
		end := i + chunkSize
		if end > len(records) {
			end = len(records)
		}
		chunks = append(chunks, records[i:end])
	}

	return chunks
}

// processChunk 単一チャンクを処理します
func (bp *BatchProcessor) processChunk(ctx context.Context, chunkIndex int, chunk []Record,
	processFn func(context.Context, *sql.Tx, []Record) error) error {

	start := time.Now()
	atomic.AddInt64(&bp.stats.totalBatches, 1)

	// タイムアウト設定
	chunkCtx := ctx
	if bp.config.BatchTimeout > 0 {
		var cancel context.CancelFunc
		chunkCtx, cancel = context.WithTimeout(ctx, bp.config.BatchTimeout)
		defer cancel()
	}

	// リトライ付きで処理実行
	err := bp.executeWithRetry(chunkCtx, chunkIndex, chunk, processFn)

	elapsed := time.Since(start)
	bp.updateBatchStats(elapsed, err == nil)

	return err
}

// executeWithRetry リトライ付きでチャンクを処理します
func (bp *BatchProcessor) executeWithRetry(ctx context.Context, chunkIndex int, chunk []Record,
	processFn func(context.Context, *sql.Tx, []Record) error) error {

	var lastErr error

	for attempt := 0; attempt <= bp.config.RetryAttempts; attempt++ {
		if attempt > 0 {
			atomic.AddInt64(&bp.stats.totalRetries, 1)
			log.Printf("Retrying chunk %d (attempt %d/%d)", chunkIndex, attempt+1, bp.config.RetryAttempts+1)

			// リトライ前の待機
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(bp.config.RetryDelay):
			}
		}

		lastErr = bp.executeSingleChunk(ctx, chunkIndex, chunk, processFn)
		if lastErr == nil {
			bp.updateProgress(int64(len(chunk)), 0)
			log.Printf("Chunk %d completed successfully (%d records)", chunkIndex, len(chunk))
			return nil
		}

		log.Printf("Chunk %d failed (attempt %d): %v", chunkIndex, attempt+1, lastErr)
	}

	// 全試行失敗
	bp.updateProgress(0, int64(len(chunk)))
	return fmt.Errorf("chunk %d failed after %d attempts: %v", chunkIndex, bp.config.RetryAttempts+1, lastErr)
}

// executeSingleChunk 単一チャンクを実行します
func (bp *BatchProcessor) executeSingleChunk(ctx context.Context, _ int, chunk []Record,
	processFn func(context.Context, *sql.Tx, []Record) error) error {

	tx, err := bp.db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
	})
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %v", err)
	}

	defer func() {
		if r := recover(); r != nil {
			if err := tx.Rollback(); err != nil {
				log.Printf("Failed to rollback transaction during panic: %v", err)
			}
			panic(r)
		}
	}()

	// バリデーション実行
	for _, record := range chunk {
		if err := record.Validate(); err != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				log.Printf("Failed to rollback transaction: %v", rollbackErr)
			}
			return fmt.Errorf("validation failed for record %d: %v", record.GetID(), err)
		}
	}

	// 処理関数実行
	if err := processFn(ctx, tx, chunk); err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			log.Printf("Failed to rollback transaction: %v", rollbackErr)
		}
		return err
	}

	// コミット
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %v", err)
	}

	return nil
}

// updateProgress 進捗を更新します
func (bp *BatchProcessor) updateProgress(successCount, failCount int64) {
	bp.progress.mu.Lock()
	defer bp.progress.mu.Unlock()

	bp.progress.SuccessRecords += successCount
	bp.progress.FailedRecords += failCount
	bp.progress.ProcessedRecords = bp.progress.SuccessRecords + bp.progress.FailedRecords
	bp.progress.LastUpdateTime = time.Now()

	// スループット計算
	elapsed := time.Since(bp.progress.StartTime).Seconds()
	if elapsed > 0 {
		bp.progress.CurrentThroughput = float64(bp.progress.ProcessedRecords) / elapsed
	}

	// 完了予想時間計算
	if bp.progress.CurrentThroughput > 0 {
		remaining := bp.progress.TotalRecords - bp.progress.ProcessedRecords
		remainingSeconds := float64(remaining) / bp.progress.CurrentThroughput
		bp.progress.EstimatedEndTime = time.Now().Add(time.Duration(remainingSeconds) * time.Second)
	}
}

// updateBatchStats バッチ統計を更新します
func (bp *BatchProcessor) updateBatchStats(elapsed time.Duration, success bool) {
	bp.stats.mu.Lock()
	defer bp.stats.mu.Unlock()

	if success {
		atomic.AddInt64(&bp.stats.completedBatches, 1)
	} else {
		atomic.AddInt64(&bp.stats.failedBatches, 1)
	}

	// 実行時間統計更新
	if elapsed > bp.stats.maxBatchTime {
		bp.stats.maxBatchTime = elapsed
	}
	if elapsed < bp.stats.minBatchTime {
		bp.stats.minBatchTime = elapsed
	}

	// 平均時間更新
	totalBatches := atomic.LoadInt64(&bp.stats.totalBatches)
	if totalBatches == 1 {
		bp.stats.avgBatchTime = elapsed
	} else {
		currentAvg := int64(bp.stats.avgBatchTime)
		newAvg := (currentAvg*(totalBatches-1) + int64(elapsed)) / totalBatches
		bp.stats.avgBatchTime = time.Duration(newAvg)
	}
}

// updateFinalStats 最終統計を更新します
func (bp *BatchProcessor) updateFinalStats() {
	bp.progress.mu.Lock()
	defer bp.progress.mu.Unlock()

	bp.progress.LastUpdateTime = time.Now()
}

// monitorProgress 進捗を監視します
func (bp *BatchProcessor) monitorProgress(ctx context.Context) {
	ticker := time.NewTicker(bp.config.ProgressInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			bp.logProgress()
		}
	}
}

// logProgress 進捗をログ出力します
func (bp *BatchProcessor) logProgress() {
	bp.progress.mu.RLock()
	defer bp.progress.mu.RUnlock()

	elapsed := time.Since(bp.progress.StartTime)
	progressPercent := float64(bp.progress.ProcessedRecords) / float64(bp.progress.TotalRecords) * 100

	log.Printf("Progress: %.1f%% (%d/%d) | Success: %d, Failed: %d | "+
		"Throughput: %.1f rec/sec | Elapsed: %v | ETA: %s",
		progressPercent,
		bp.progress.ProcessedRecords,
		bp.progress.TotalRecords,
		bp.progress.SuccessRecords,
		bp.progress.FailedRecords,
		bp.progress.CurrentThroughput,
		elapsed.Round(time.Second),
		bp.progress.EstimatedEndTime.Format("15:04:05"))
}

// GetProgress 現在の進捗を取得します
func (bp *BatchProcessor) GetProgress() BatchProgressSnapshot {
	bp.progress.mu.RLock()
	defer bp.progress.mu.RUnlock()

	return BatchProgressSnapshot{
		TotalRecords:      bp.progress.TotalRecords,
		ProcessedRecords:  bp.progress.ProcessedRecords,
		SuccessRecords:    bp.progress.SuccessRecords,
		FailedRecords:     bp.progress.FailedRecords,
		StartTime:         bp.progress.StartTime,
		LastUpdateTime:    bp.progress.LastUpdateTime,
		EstimatedEndTime:  bp.progress.EstimatedEndTime,
		CurrentThroughput: bp.progress.CurrentThroughput,
	}
}

// BatchProgressSnapshot 進捗のスナップショットです
type BatchProgressSnapshot struct {
	TotalRecords      int64
	ProcessedRecords  int64
	SuccessRecords    int64
	FailedRecords     int64
	StartTime         time.Time
	LastUpdateTime    time.Time
	EstimatedEndTime  time.Time
	CurrentThroughput float64
}

// GetStats バッチ統計を取得します
func (bp *BatchProcessor) GetStats() BatchStatsSnapshot {
	bp.stats.mu.RLock()
	defer bp.stats.mu.RUnlock()

	return BatchStatsSnapshot{
		TotalBatches:     atomic.LoadInt64(&bp.stats.totalBatches),
		CompletedBatches: atomic.LoadInt64(&bp.stats.completedBatches),
		FailedBatches:    atomic.LoadInt64(&bp.stats.failedBatches),
		TotalRetries:     atomic.LoadInt64(&bp.stats.totalRetries),
		AvgBatchTime:     bp.stats.avgBatchTime,
		MaxBatchTime:     bp.stats.maxBatchTime,
		MinBatchTime:     bp.stats.minBatchTime,
	}
}

// BatchStatsSnapshot バッチ統計のスナップショットです
type BatchStatsSnapshot struct {
	TotalBatches     int64
	CompletedBatches int64
	FailedBatches    int64
	TotalRetries     int64
	AvgBatchTime     time.Duration
	MaxBatchTime     time.Duration
	MinBatchTime     time.Duration
}

// BulkInsertProcessor バルクインサート処理を行います
type BulkInsertProcessor struct {
	db *sql.DB
}

// NewBulkInsertProcessor 新しいバルクインサートプロセッサーを作成します
func NewBulkInsertProcessor(db *sql.DB) *BulkInsertProcessor {
	return &BulkInsertProcessor{db: db}
}

// BulkInsertProducts 商品を一括挿入します
func (bip *BulkInsertProcessor) BulkInsertProducts(ctx context.Context, products []*ProductRecord) error {
	tx, err := bip.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil {
			log.Printf("Failed to rollback transaction: %v", err)
		}
	}()

	// COPY文用のステートメント準備
	stmt, err := tx.PrepareContext(ctx, pq.CopyIn("products", "name", "price", "category_id"))
	if err != nil {
		return fmt.Errorf("failed to prepare copy statement: %v", err)
	}

	// データを一括挿入
	for _, product := range products {
		if err := product.Validate(); err != nil {
			return fmt.Errorf("validation failed for product %s: %v", product.Name, err)
		}

		_, err = stmt.ExecContext(ctx, product.Name, product.Price, product.CategoryID)
		if err != nil {
			return fmt.Errorf("failed to add product to batch: %v", err)
		}
	}

	// COPY実行
	_, err = stmt.ExecContext(ctx)
	if err != nil {
		return fmt.Errorf("failed to execute bulk insert: %v", err)
	}

	// ステートメントクローズ
	if err = stmt.Close(); err != nil {
		return fmt.Errorf("failed to close statement: %v", err)
	}

	// コミット
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %v", err)
	}

	log.Printf("Successfully bulk inserted %d products", len(products))
	return nil
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

	// バッチ処理設定
	config := &BatchConfig{
		ChunkSize:        100,
		Concurrency:      4,
		BatchTimeout:     30 * time.Second,
		RetryAttempts:    2,
		RetryDelay:       1 * time.Second,
		ProgressInterval: 2 * time.Second,
	}

	// テストデータ準備
	testRecords := generateTestRecords(1000)

	// バッチ処理実行
	bp := NewBatchProcessor(db, config)
	ctx := context.Background()

	log.Println("Starting batch processing...")
	err = bp.ProcessBatch(ctx, testRecords, processProducts)
	if err != nil {
		log.Printf("Batch processing failed: %v", err)
	}

	// 統計表示
	progress := bp.GetProgress()
	stats := bp.GetStats()

	fmt.Printf("\nBatch Processing Results:\n")
	fmt.Printf("  Total Records: %d\n", progress.TotalRecords)
	fmt.Printf("  Processed: %d\n", progress.ProcessedRecords)
	fmt.Printf("  Successful: %d\n", progress.SuccessRecords)
	fmt.Printf("  Failed: %d\n", progress.FailedRecords)
	fmt.Printf("  Total Batches: %d\n", stats.TotalBatches)
	fmt.Printf("  Retries: %d\n", stats.TotalRetries)
	fmt.Printf("  Average Batch Time: %v\n", stats.AvgBatchTime)
	fmt.Printf("  Final Throughput: %.2f rec/sec\n", progress.CurrentThroughput)

	// バルクインサートのテスト
	log.Println("\nTesting bulk insert...")
	testBulkInsert(db)
}

// generateTestRecords テスト用レコードを生成します
func generateTestRecords(count int) []Record {
	records := make([]Record, count)
	categories := []int64{1, 2, 3, 4, 5}

	for i := 0; i < count; i++ {
		records[i] = &ProductRecord{
			ID:         int64(i + 1),
			Name:       fmt.Sprintf("Test Product %d", i+1),
			Price:      float64((i%100 + 1) * 100),
			CategoryID: categories[i%len(categories)],
		}
	}

	return records
}

// processProducts 商品処理関数です
func processProducts(ctx context.Context, tx *sql.Tx, records []Record) error {
	// 商品の更新処理をシミュレート
	for _, record := range records {
		product := record.(*ProductRecord)

		// 価格を10%増加
		newPrice := product.Price * 1.1

		_, err := tx.ExecContext(ctx,
			"UPDATE products SET price = $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2",
			newPrice, product.ID)
		if err != nil {
			return fmt.Errorf("failed to update product %d: %v", product.ID, err)
		}
	}

	return nil
}

// testBulkInsert バルクインサートのテストを実行します
func testBulkInsert(db *sql.DB) {
	bip := NewBulkInsertProcessor(db)
	ctx := context.Background()

	// テスト用商品データ
	products := []*ProductRecord{
		{Name: "Bulk Product 1", Price: 1000.0, CategoryID: 1},
		{Name: "Bulk Product 2", Price: 2000.0, CategoryID: 2},
		{Name: "Bulk Product 3", Price: 3000.0, CategoryID: 3},
	}

	start := time.Now()
	err := bip.BulkInsertProducts(ctx, products)
	elapsed := time.Since(start)

	if err != nil {
		log.Printf("Bulk insert failed: %v", err)
	} else {
		log.Printf("Bulk insert completed in %v", elapsed)
	}
}
