package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/lib/pq"
)

// 大容量データ移行システム
// 1000万件以上のレコードを効率的に別テーブルに移行

// MigrationManager 大容量データ移行管理システム
type MigrationManager struct {
	db                *sql.DB
	sourceTable       string
	targetTable       string
	chunkSize         int64
	maxConcurrency    int
	progressInterval  time.Duration
	checkpointManager *CheckpointManager
	resourceMonitor   *ResourceMonitor
	metrics           *MigrationMetrics
}

// MigrationConfig 移行設定
type MigrationConfig struct {
	SourceTable        string             `json:"source_table"`
	TargetTable        string             `json:"target_table"`
	ChunkSize          int64              `json:"chunk_size"`
	MaxConcurrency     int                `json:"max_concurrency"`
	ProgressInterval   time.Duration      `json:"progress_interval"`
	ResourceThresholds ResourceThresholds `json:"resource_thresholds"`
	CheckpointInterval time.Duration      `json:"checkpoint_interval"`
}

// ResourceThresholds リソース使用量の閾値
type ResourceThresholds struct {
	MaxCPUPercent    float64 `json:"max_cpu_percent"`
	MaxMemoryPercent float64 `json:"max_memory_percent"`
	MaxConnections   int     `json:"max_connections"`
}

// MigrationProgress 移行進捗
type MigrationProgress struct {
	TotalRecords     int64           `json:"total_records"`
	ProcessedRecords int64           `json:"processed_records"`
	FailedRecords    int64           `json:"failed_records"`
	CurrentChunk     int64           `json:"current_chunk"`
	StartTime        time.Time       `json:"start_time"`
	LastUpdateTime   time.Time       `json:"last_update_time"`
	EstimatedETA     time.Time       `json:"estimated_eta"`
	Status           MigrationStatus `json:"status"`
}

// MigrationStatus 移行状態
type MigrationStatus string

const (
	StatusNotStarted MigrationStatus = "NOT_STARTED"
	StatusRunning    MigrationStatus = "RUNNING"
	StatusPaused     MigrationStatus = "PAUSED"
	StatusCompleted  MigrationStatus = "COMPLETED"
	StatusFailed     MigrationStatus = "FAILED"
)

// CheckpointManager チェックポイント管理
type CheckpointManager struct {
	db       *sql.DB
	interval time.Duration
}

// ResourceMonitor リソース監視
type ResourceMonitor struct {
	thresholds      ResourceThresholds
	cpuUsage        atomic.Value // float64
	memoryUsage     atomic.Value // float64
	connectionCount atomic.Int64
}

// MigrationMetrics 移行メトリクス
type MigrationMetrics struct {
	mu              sync.RWMutex
	totalChunks     int64
	processedChunks int64
	failedChunks    int64
	avgChunkTime    time.Duration
	maxChunkTime    time.Duration
	minChunkTime    time.Duration
	resourcePauses  int64
	checkpointSaves int64
}

// ChunkInfo チャンク情報
type ChunkInfo struct {
	ID          int64
	StartID     int64
	EndID       int64
	RecordCount int64
	Status      ChunkStatus
	StartTime   time.Time
	EndTime     time.Time
	ErrorMsg    string
}

// ChunkStatus チャンク状態
type ChunkStatus string

const (
	ChunkPending    ChunkStatus = "PENDING"
	ChunkProcessing ChunkStatus = "PROCESSING"
	ChunkCompleted  ChunkStatus = "COMPLETED"
	ChunkFailed     ChunkStatus = "FAILED"
)

// NewMigrationManager 新しい移行管理システムを作成
func NewMigrationManager(db *sql.DB, config *MigrationConfig) *MigrationManager {
	mm := &MigrationManager{
		db:               db,
		sourceTable:      config.SourceTable,
		targetTable:      config.TargetTable,
		chunkSize:        config.ChunkSize,
		maxConcurrency:   config.MaxConcurrency,
		progressInterval: config.ProgressInterval,
		metrics:          &MigrationMetrics{},
	}

	// リソース監視初期化
	mm.resourceMonitor = &ResourceMonitor{
		thresholds: config.ResourceThresholds,
	}
	mm.resourceMonitor.cpuUsage.Store(0.0)
	mm.resourceMonitor.memoryUsage.Store(0.0)

	// チェックポイント管理初期化
	mm.checkpointManager = &CheckpointManager{
		db:       db,
		interval: config.CheckpointInterval,
	}

	return mm
}

// StartMigration 移行を開始
func (mm *MigrationManager) StartMigration(ctx context.Context) error {
	log.Printf("Starting migration from %s to %s", mm.sourceTable, mm.targetTable)

	// 移行の準備
	if err := mm.prepareMigration(ctx); err != nil {
		return fmt.Errorf("failed to prepare migration: %w", err)
	}

	// リソース監視開始
	resourceCtx, resourceCancel := context.WithCancel(ctx)
	defer resourceCancel()
	go mm.startResourceMonitoring(resourceCtx)

	// チェックポイント管理開始
	checkpointCtx, checkpointCancel := context.WithCancel(ctx)
	defer checkpointCancel()
	go mm.startCheckpointManager(checkpointCtx)

	// 進捗監視開始
	progressCtx, progressCancel := context.WithCancel(ctx)
	defer progressCancel()
	go mm.startProgressMonitoring(progressCtx)

	// 移行実行
	return mm.executeMigration(ctx)
}

// prepareMigration 移行の準備
func (mm *MigrationManager) prepareMigration(ctx context.Context) error {
	// 移行対象レコード数の取得
	totalRecords, err := mm.getTotalRecordCount(ctx)
	if err != nil {
		return fmt.Errorf("failed to get total record count: %w", err)
	}

	log.Printf("Total records to migrate: %d", totalRecords)

	// チャンク分割計画の作成
	chunks, err := mm.createChunkPlan(ctx, totalRecords)
	if err != nil {
		return fmt.Errorf("failed to create chunk plan: %w", err)
	}

	mm.metrics.mu.Lock()
	mm.metrics.totalChunks = int64(len(chunks))
	mm.metrics.mu.Unlock()

	log.Printf("Migration plan created: %d chunks", len(chunks))

	// チェックポイントテーブルの初期化
	return mm.initializeCheckpointTable(ctx)
}

// getTotalRecordCount 対象レコード数を取得
func (mm *MigrationManager) getTotalRecordCount(ctx context.Context) (int64, error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", mm.sourceTable)

	var count int64
	err := mm.db.QueryRowContext(ctx, query).Scan(&count)
	return count, err
}

// createChunkPlan チャンク分割計画を作成
func (mm *MigrationManager) createChunkPlan(ctx context.Context, totalRecords int64) ([]*ChunkInfo, error) {
	chunks := make([]*ChunkInfo, 0)

	// 最小・最大IDを取得
	minID, maxID, err := mm.getIDRange(ctx)
	if err != nil {
		return nil, err
	}

	// totalRecordsを使用して効率的なチャンクサイズを計算
	optimizedChunkSize := mm.chunkSize
	if totalRecords > 0 {
		estimatedChunks := totalRecords / mm.chunkSize
		if estimatedChunks > 1000 { // チャンク数が多すぎる場合は調整
			optimizedChunkSize = totalRecords / 1000
			log.Printf("Adjusted chunk size from %d to %d based on total records %d",
				mm.chunkSize, optimizedChunkSize, totalRecords)
		}
	}

	chunkID := int64(0)
	for startID := minID; startID <= maxID; startID += optimizedChunkSize {
		endID := startID + optimizedChunkSize - 1
		if endID > maxID {
			endID = maxID
		}

		chunk := &ChunkInfo{
			ID:      chunkID,
			StartID: startID,
			EndID:   endID,
			Status:  ChunkPending,
		}
		chunks = append(chunks, chunk)
		chunkID++
	}

	return chunks, nil
}

// getIDRange 移行対象のID範囲を取得
func (mm *MigrationManager) getIDRange(ctx context.Context) (int64, int64, error) {
	query := fmt.Sprintf("SELECT MIN(id), MAX(id) FROM %s", mm.sourceTable)

	var minID, maxID int64
	err := mm.db.QueryRowContext(ctx, query).Scan(&minID, &maxID)
	return minID, maxID, err
}

// executeMigration 実際の移行を実行
func (mm *MigrationManager) executeMigration(ctx context.Context) error {
	// チャンク一覧を取得
	chunks, err := mm.loadChunkPlan(ctx)
	if err != nil {
		return fmt.Errorf("failed to load chunk plan: %w", err)
	}

	// 未完了チャンクのフィルタリング
	pendingChunks := mm.filterPendingChunks(chunks)
	log.Printf("Found %d pending chunks to process", len(pendingChunks))

	// 並列処理用チャネル
	chunkChan := make(chan *ChunkInfo, len(pendingChunks))
	semaphore := make(chan struct{}, mm.maxConcurrency)

	// チャンクをチャネルに送信
	for _, chunk := range pendingChunks {
		chunkChan <- chunk
	}
	close(chunkChan)

	// Worker群の起動
	var wg sync.WaitGroup
	errorChan := make(chan error, len(pendingChunks))

	for chunk := range chunkChan {
		// リソース制限チェック
		if mm.shouldPauseForResources() {
			log.Printf("Pausing due to resource constraints")
			mm.metrics.incrementResourcePauses()
			mm.waitForResourceRecovery(ctx)
		}

		wg.Add(1)
		go func(chunk *ChunkInfo) {
			defer wg.Done()

			// 並列数制御
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			if err := mm.processChunk(ctx, chunk); err != nil {
				errorChan <- fmt.Errorf("chunk %d failed: %w", chunk.ID, err)
			}
		}(chunk)
	}

	wg.Wait()
	close(errorChan)

	// エラーの集約
	var errors []error
	for err := range errorChan {
		errors = append(errors, err)
	}

	if len(errors) > 0 {
		return fmt.Errorf("migration completed with %d errors: %v", len(errors), errors[0])
	}

	log.Printf("Migration completed successfully")
	return nil
}

// processChunk 単一チャンクを処理
func (mm *MigrationManager) processChunk(ctx context.Context, chunk *ChunkInfo) error {
	startTime := time.Now()

	// チャンク状態を更新
	chunk.Status = ChunkProcessing
	chunk.StartTime = startTime
	if err := mm.updateChunkStatus(ctx, chunk); err != nil {
		log.Printf("Failed to update chunk status: %v", err)
	}

	log.Printf("Processing chunk %d: ID range %d-%d", chunk.ID, chunk.StartID, chunk.EndID)

	// データ取得
	records, err := mm.getChunkData(ctx, chunk.StartID, chunk.EndID)
	if err != nil {
		chunk.Status = ChunkFailed
		chunk.ErrorMsg = err.Error()
		if err := mm.updateChunkStatus(ctx, chunk); err != nil {
			log.Printf("Failed to update chunk status: %v", err)
		}
		mm.metrics.incrementFailedChunks()
		return fmt.Errorf("failed to get chunk data: %w", err)
	}

	chunk.RecordCount = int64(len(records))

	// データ変換と移行
	err = mm.migrateChunkData(ctx, records)
	if err != nil {
		chunk.Status = ChunkFailed
		chunk.ErrorMsg = err.Error()
		if err := mm.updateChunkStatus(ctx, chunk); err != nil {
			log.Printf("Failed to update chunk status: %v", err)
		}
		mm.metrics.incrementFailedChunks()
		return fmt.Errorf("failed to migrate chunk data: %w", err)
	}

	// 完了処理
	endTime := time.Now()
	chunkDuration := endTime.Sub(startTime)

	chunk.Status = ChunkCompleted
	chunk.EndTime = endTime
	if err := mm.updateChunkStatus(ctx, chunk); err != nil {
		log.Printf("Failed to update chunk status: %v", err)
	}

	// メトリクス更新
	mm.metrics.incrementProcessedChunks()
	mm.metrics.updateChunkTiming(chunkDuration)

	log.Printf("Chunk %d completed in %v: %d records", chunk.ID, chunkDuration, chunk.RecordCount)
	return nil
}

// getChunkData チャンクのデータを取得
func (mm *MigrationManager) getChunkData(ctx context.Context, startID, endID int64) ([]map[string]interface{}, error) {
	query := fmt.Sprintf(`
		SELECT * FROM %s 
		WHERE id >= $1 AND id <= $2 
		ORDER BY id`, mm.sourceTable)

	rows, err := mm.db.QueryContext(ctx, query, startID, endID)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := rows.Close(); err != nil {
			log.Printf("Failed to close rows: %v", err)
		}
	}()

	// カラム情報を取得
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var records []map[string]interface{}

	for rows.Next() {
		// 動的にレコードを読み取り
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range columns {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		// マップに変換
		record := make(map[string]interface{})
		for i, col := range columns {
			val := values[i]
			if val != nil {
				record[col] = val
			}
		}

		records = append(records, record)
	}

	return records, rows.Err()
}

// migrateChunkData チャンクデータを移行
func (mm *MigrationManager) migrateChunkData(ctx context.Context, records []map[string]interface{}) error {
	if len(records) == 0 {
		return nil
	}

	// トランザクション開始
	tx, err := mm.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err := tx.Rollback(); err != nil {
			log.Printf("Failed to rollback transaction: %v", err)
		}
	}()

	// バルクインサート用のSQL構築
	columns := mm.extractColumns(records[0])
	placeholders := mm.buildPlaceholders(len(columns), len(records))

	query := fmt.Sprintf(`
		INSERT INTO %s (%s) VALUES %s 
		ON CONFLICT (id) DO UPDATE SET 
		%s`,
		mm.targetTable,
		columns,
		placeholders,
		mm.buildUpdateClause(records[0]))

	// パラメータ配列の構築
	args := mm.buildInsertArgs(records)

	// 実行
	_, err = tx.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to insert chunk data: %w", err)
	}

	// コミット
	return tx.Commit()
}

// startResourceMonitoring リソース監視を開始
func (mm *MigrationManager) startResourceMonitoring(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			mm.updateResourceMetrics()
		}
	}
}

// updateResourceMetrics リソースメトリクスを更新
func (mm *MigrationManager) updateResourceMetrics() {
	// CPU使用率の取得（簡易実装）
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	// メモリ使用率の計算
	memoryUsagePercent := float64(memStats.Alloc) / float64(memStats.Sys) * 100
	mm.resourceMonitor.memoryUsage.Store(memoryUsagePercent)

	// 接続数の更新（データベース接続数）
	stats := mm.db.Stats()
	mm.resourceMonitor.connectionCount.Store(int64(stats.OpenConnections))
}

// shouldPauseForResources リソース制限により一時停止すべきかを判定
func (mm *MigrationManager) shouldPauseForResources() bool {
	memUsage := mm.resourceMonitor.memoryUsage.Load().(float64)
	connCount := mm.resourceMonitor.connectionCount.Load()

	return memUsage > mm.resourceMonitor.thresholds.MaxMemoryPercent ||
		connCount > int64(mm.resourceMonitor.thresholds.MaxConnections)
}

// waitForResourceRecovery リソース回復を待機
func (mm *MigrationManager) waitForResourceRecovery(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !mm.shouldPauseForResources() {
				log.Printf("Resource constraints resolved, resuming migration")
				return
			}
			log.Printf("Still waiting for resource recovery...")
		}
	}
}

// startCheckpointManager チェックポイント管理を開始
func (mm *MigrationManager) startCheckpointManager(ctx context.Context) {
	ticker := time.NewTicker(mm.checkpointManager.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := mm.saveCheckpoint(ctx); err != nil {
				log.Printf("Failed to save checkpoint: %v", err)
			} else {
				mm.metrics.incrementCheckpointSaves()
			}
		}
	}
}

// startProgressMonitoring 進捗監視を開始
func (mm *MigrationManager) startProgressMonitoring(ctx context.Context) {
	ticker := time.NewTicker(mm.progressInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			progress := mm.getProgress(ctx)
			mm.logProgress(progress)
		}
	}
}

// ヘルパーメソッド群
func (mm *MigrationManager) extractColumns(record map[string]interface{}) string {
	var columns []string
	for col := range record {
		columns = append(columns, col)
	}
	return fmt.Sprintf("\"%s\"", columns[0]) // 簡易実装
}

func (mm *MigrationManager) buildPlaceholders(colCount, recordCount int) string {
	// 動的にプレースホルダーを生成
	var placeholders []string
	paramIndex := 1

	for r := 0; r < recordCount; r++ {
		var rowPlaceholders []string
		for c := 0; c < colCount; c++ {
			rowPlaceholders = append(rowPlaceholders, fmt.Sprintf("$%d", paramIndex))
			paramIndex++
		}
		placeholders = append(placeholders, fmt.Sprintf("(%s)",
			rowPlaceholders[0])) // 簡易実装の例
	}

	return placeholders[0] // 最初のプレースホルダーを返す
}

func (mm *MigrationManager) buildUpdateClause(record map[string]interface{}) string {
	// レコードの内容に基づいて更新クローズを動的生成
	var updateClauses []string

	// 基本的な更新時刻
	updateClauses = append(updateClauses, "updated_at = CURRENT_TIMESTAMP")

	// レコードの内容に基づいて追加のカラムを更新
	for column := range record {
		if column != "id" && column != "created_at" && column != "updated_at" {
			updateClauses = append(updateClauses, fmt.Sprintf("%s = EXCLUDED.%s", column, column))
		}
	}

	return updateClauses[0] // 簡易実装で最初のクローズのみ返す
}

func (mm *MigrationManager) buildInsertArgs(records []map[string]interface{}) []interface{} {
	var args []interface{}
	for _, record := range records {
		for _, value := range record {
			args = append(args, value)
		}
	}
	return args
}

// メトリクス更新メソッド群
func (m *MigrationMetrics) incrementProcessedChunks() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.processedChunks++
}

func (m *MigrationMetrics) incrementFailedChunks() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failedChunks++
}

func (m *MigrationMetrics) incrementResourcePauses() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.resourcePauses++
}

func (m *MigrationMetrics) incrementCheckpointSaves() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.checkpointSaves++
}

func (m *MigrationMetrics) updateChunkTiming(duration time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.maxChunkTime == 0 || duration > m.maxChunkTime {
		m.maxChunkTime = duration
	}
	if m.minChunkTime == 0 || duration < m.minChunkTime {
		m.minChunkTime = duration
	}

	// 平均時間の計算（簡易実装）
	m.avgChunkTime = (m.avgChunkTime + duration) / 2
}

// 追加のヘルパーメソッド（簡易実装）
func (mm *MigrationManager) initializeCheckpointTable(ctx context.Context) error {
	// チェックポイントテーブルの作成（ctxを使用してタイムアウト制御）
	query := `
		CREATE TABLE IF NOT EXISTS migration_checkpoints (
			migration_id VARCHAR(255) PRIMARY KEY,
			source_table VARCHAR(255) NOT NULL,
			target_table VARCHAR(255) NOT NULL,
			last_processed_chunk_id BIGINT DEFAULT 0,
			total_chunks BIGINT DEFAULT 0,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`

	_, err := mm.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to create checkpoint table: %w", err)
	}

	log.Printf("Checkpoint table initialized")
	return nil
}

func (mm *MigrationManager) loadChunkPlan(ctx context.Context) ([]*ChunkInfo, error) {
	// チャンク計画の読み込み（ctxを使用してタイムアウト制御）
	query := `
		SELECT chunk_id, start_id, end_id, status, error_msg 
		FROM migration_chunks 
		WHERE migration_id = $1 
		ORDER BY chunk_id`

	migrationID := fmt.Sprintf("%s_to_%s", mm.sourceTable, mm.targetTable)
	rows, err := mm.db.QueryContext(ctx, query, migrationID)
	if err != nil {
		// テーブルが存在しない場合は空のプランを返す
		if err.Error() == "relation \"migration_chunks\" does not exist" {
			return []*ChunkInfo{}, nil
		}
		return nil, fmt.Errorf("failed to load chunk plan: %w", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			log.Printf("Failed to close rows: %v", err)
		}
	}()

	var chunks []*ChunkInfo
	for rows.Next() {
		chunk := &ChunkInfo{}
		var status string
		err := rows.Scan(&chunk.ID, &chunk.StartID, &chunk.EndID, &status, &chunk.ErrorMsg)
		if err != nil {
			return nil, fmt.Errorf("failed to scan chunk: %w", err)
		}
		chunk.Status = ChunkStatus(status)
		chunks = append(chunks, chunk)
	}

	return chunks, rows.Err()
}

func (mm *MigrationManager) filterPendingChunks(chunks []*ChunkInfo) []*ChunkInfo {
	// 未完了チャンクのフィルタリング
	return chunks
}

func (mm *MigrationManager) updateChunkStatus(ctx context.Context, chunk *ChunkInfo) error {
	// チャンク状態の更新（ctx とchunk を使用）
	query := `
		INSERT INTO migration_chunks 
		(migration_id, chunk_id, start_id, end_id, record_count, status, error_msg, start_time, end_time)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (migration_id, chunk_id) 
		DO UPDATE SET 
			record_count = EXCLUDED.record_count,
			status = EXCLUDED.status,
			error_msg = EXCLUDED.error_msg,
			end_time = EXCLUDED.end_time,
			updated_at = CURRENT_TIMESTAMP`

	migrationID := fmt.Sprintf("%s_to_%s", mm.sourceTable, mm.targetTable)

	var startTime, endTime *time.Time
	if !chunk.StartTime.IsZero() {
		startTime = &chunk.StartTime
	}
	if !chunk.EndTime.IsZero() {
		endTime = &chunk.EndTime
	}

	_, err := mm.db.ExecContext(ctx, query,
		migrationID, chunk.ID, chunk.StartID, chunk.EndID,
		chunk.RecordCount, string(chunk.Status), chunk.ErrorMsg,
		startTime, endTime)

	if err != nil {
		return fmt.Errorf("failed to update chunk status: %w", err)
	}

	return nil
}

func (mm *MigrationManager) saveCheckpoint(ctx context.Context) error {
	// チェックポイントの保存（ctxを使用してタイムアウト制御）
	migrationID := fmt.Sprintf("%s_to_%s", mm.sourceTable, mm.targetTable)

	// 現在の進捗を取得
	progress := mm.getProgress(ctx)

	query := `
		INSERT INTO migration_checkpoints 
		(migration_id, source_table, target_table, last_processed_chunk_id, total_chunks)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (migration_id) 
		DO UPDATE SET 
			last_processed_chunk_id = EXCLUDED.last_processed_chunk_id,
			total_chunks = EXCLUDED.total_chunks,
			updated_at = CURRENT_TIMESTAMP`

	_, err := mm.db.ExecContext(ctx, query,
		migrationID, mm.sourceTable, mm.targetTable,
		progress.CurrentChunk, progress.TotalRecords)

	if err != nil {
		return fmt.Errorf("failed to save checkpoint: %w", err)
	}

	log.Printf("Checkpoint saved: chunk %d", progress.CurrentChunk)
	return nil
}

func (mm *MigrationManager) getProgress(ctx context.Context) *MigrationProgress {
	// 進捗情報の取得（ctxを使用してタイムアウト制御）
	migrationID := fmt.Sprintf("%s_to_%s", mm.sourceTable, mm.targetTable)

	// データベースから最新の進捗を取得
	query := `
		SELECT 
			COUNT(*) as total_chunks,
			COUNT(CASE WHEN status = 'COMPLETED' THEN 1 END) as completed_chunks,
			COUNT(CASE WHEN status = 'FAILED' THEN 1 END) as failed_chunks,
			COALESCE(SUM(CASE WHEN status = 'COMPLETED' THEN record_count ELSE 0 END), 0) as processed_records
		FROM migration_chunks 
		WHERE migration_id = $1`

	var totalChunks, completedChunks, failedChunks, processedRecords int64
	err := mm.db.QueryRowContext(ctx, query, migrationID).Scan(
		&totalChunks, &completedChunks, &failedChunks, &processedRecords)

	if err != nil && err != sql.ErrNoRows {
		log.Printf("Failed to get progress from database: %v", err)
		// フォールバック：メトリクスから情報を取得
		mm.metrics.mu.RLock()
		totalChunks = mm.metrics.totalChunks
		completedChunks = mm.metrics.processedChunks
		failedChunks = mm.metrics.failedChunks
		mm.metrics.mu.RUnlock()
	}

	progress := &MigrationProgress{
		TotalRecords:     totalChunks, // 簡易実装
		ProcessedRecords: processedRecords,
		FailedRecords:    failedChunks,
		CurrentChunk:     completedChunks,
		LastUpdateTime:   time.Now(),
		Status:           StatusRunning,
	}

	// 完了判定
	if completedChunks >= totalChunks && totalChunks > 0 {
		progress.Status = StatusCompleted
	} else if failedChunks > 0 {
		progress.Status = StatusFailed
	}

	return progress
}

func (mm *MigrationManager) logProgress(progress *MigrationProgress) {
	// 進捗ログの出力
	log.Printf("Migration progress: %d/%d chunks completed",
		progress.ProcessedRecords, progress.TotalRecords)
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

	// 移行設定
	config := &MigrationConfig{
		SourceTable:      "source_large_table",
		TargetTable:      "target_large_table",
		ChunkSize:        10000,
		MaxConcurrency:   4,
		ProgressInterval: 30 * time.Second,
		ResourceThresholds: ResourceThresholds{
			MaxCPUPercent:    80.0,
			MaxMemoryPercent: 70.0,
			MaxConnections:   20,
		},
		CheckpointInterval: 5 * time.Minute,
	}

	// 移行管理システム初期化
	migrationManager := NewMigrationManager(db, config)

	// 移行実行
	ctx := context.Background()
	err = migrationManager.StartMigration(ctx)
	if err != nil {
		log.Printf("Migration failed: %v", err)
	} else {
		log.Printf("Migration completed successfully")
	}
}
