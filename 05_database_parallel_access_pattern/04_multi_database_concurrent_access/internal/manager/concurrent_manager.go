package manager

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"maps"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"multi-db/internal/adapters"
	"multi-db/internal/config"
	"multi-db/internal/models"
)

// ConcurrentDBManager 並行データベースマネージャー
type ConcurrentDBManager struct {
	adapters   map[string]adapters.DBAdapter
	workerPool chan struct{}
	resultChan chan *TaskResult
	errorChan  chan error
	ctx        context.Context
	cancel     context.CancelFunc
	metrics    *PerformanceMetrics
	mutex      sync.RWMutex
	registry   *adapters.AdapterRegistry
}

// TaskResult タスク実行結果
type TaskResult struct {
	TaskID    string           `json:"task_id"`
	DBType    string           `json:"db_type"`
	Data      []map[string]any `json:"data,omitempty"`
	Users     []*models.User   `json:"users,omitempty"`
	Error     error            `json:"error,omitempty"`
	Elapsed   time.Duration    `json:"elapsed"`
	Timestamp time.Time        `json:"timestamp"`
	Operation string           `json:"operation"`
}

// PerformanceMetrics パフォーマンス指標
type PerformanceMetrics struct {
	TotalTasks     int64         `json:"total_tasks"`
	SuccessTasks   int64         `json:"success_tasks"`
	FailedTasks    int64         `json:"failed_tasks"`
	AverageLatency time.Duration `json:"average_latency"`
	MaxLatency     time.Duration `json:"max_latency"`
	MinLatency     time.Duration `json:"min_latency"`
	ActiveTasks    atomic.Int64  `json:"-"`
	mutex          sync.RWMutex  `json:"-"`
}

// ErrManagerClosed Close 後に投入されたタスクへ返すエラー
var ErrManagerClosed = errors.New("マネージャーは停止済みです")

// WorkerPoolConfig ワーカープール設定
type WorkerPoolConfig struct {
	MaxWorkers      int           `json:"max_workers"`
	QueueSize       int           `json:"queue_size"`
	TimeoutDuration time.Duration `json:"timeout_duration"`
}

// DatabaseTask データベースタスク
type DatabaseTask struct {
	ID         string            `json:"id"`
	DBType     string            `json:"db_type"`
	Operation  string            `json:"operation"`
	Query      string            `json:"query"`
	Args       []any             `json:"args"`
	ResultChan chan *TaskResult  `json:"-"`
	Context    context.Context   `json:"-"`
	Callback   func(*TaskResult) `json:"-"`
}

// NewConcurrentDBManager 新しい並行データベースマネージャーを作成
func NewConcurrentDBManager(poolConfig WorkerPoolConfig) *ConcurrentDBManager {
	ctx, cancel := context.WithCancel(context.Background())

	if poolConfig.MaxWorkers <= 0 {
		poolConfig.MaxWorkers = runtime.GOMAXPROCS(0) * 4
	}
	if poolConfig.QueueSize <= 0 {
		poolConfig.QueueSize = poolConfig.MaxWorkers * 2
	}
	if poolConfig.TimeoutDuration <= 0 {
		poolConfig.TimeoutDuration = 30 * time.Second
	}

	manager := &ConcurrentDBManager{
		adapters:   make(map[string]adapters.DBAdapter),
		workerPool: make(chan struct{}, poolConfig.MaxWorkers),
		resultChan: make(chan *TaskResult, poolConfig.QueueSize),
		errorChan:  make(chan error, poolConfig.QueueSize),
		ctx:        ctx,
		cancel:     cancel,
		metrics:    &PerformanceMetrics{MinLatency: time.Duration(^uint64(0) >> 1)}, // 最大値で初期化
		registry:   adapters.DefaultRegistry,
	}

	// デフォルトアダプターを登録
	adapters.RegisterDefaultAdapters()

	// 並行数は workerPool（セマフォ）で各タスクが制御するので、常駐ワーカーは置かない
	return manager
}

// adapterFor 停止済みなら ErrManagerClosed、未登録ならエラーを返す
func (cm *ConcurrentDBManager) adapterFor(dbType string) (adapters.DBAdapter, error) {
	cm.mutex.RLock()
	defer cm.mutex.RUnlock()

	if cm.ctx.Err() != nil {
		return nil, ErrManagerClosed
	}
	adapter, exists := cm.adapters[dbType]
	if !exists {
		return nil, fmt.Errorf("データベースアダプターが見つかりません: %s", dbType)
	}
	return adapter, nil
}

// RegisterDatabase データベース登録
func (cm *ConcurrentDBManager) RegisterDatabase(dbType string, cfg config.DatabaseConfig) error {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	if cm.ctx.Err() != nil {
		return ErrManagerClosed
	}

	adapter, err := cm.registry.Create(dbType)
	if err != nil {
		return fmt.Errorf("アダプター作成エラー: %w", err)
	}

	if err := adapter.Connect(cm.ctx, cfg); err != nil {
		return fmt.Errorf("データベース接続エラー (%s): %w", dbType, err)
	}

	cm.adapters[dbType] = adapter
	log.Printf("データベース登録成功: %s", dbType)
	return nil
}

// ExecuteQuery 並行クエリ実行
func (cm *ConcurrentDBManager) ExecuteQuery(ctx context.Context, dbType, query string, args ...any) <-chan *TaskResult {
	taskID := fmt.Sprintf("%s_%d", dbType, time.Now().UnixNano())
	resultChan := make(chan *TaskResult, 1)

	task := &DatabaseTask{
		ID:         taskID,
		DBType:     dbType,
		Operation:  "query",
		Query:      query,
		Args:       args,
		ResultChan: resultChan,
		Context:    ctx,
	}

	go cm.processTask(task)
	return resultChan
}

// ExecuteTransaction トランザクション実行
// fnにはアダプターが開始した*sql.Txを渡す。fnの中の操作は、すべてこのtxを通して行うこと。
func (cm *ConcurrentDBManager) ExecuteTransaction(ctx context.Context, dbType string, fn func(*sql.Tx) error) <-chan *TaskResult {
	taskID := fmt.Sprintf("tx_%s_%d", dbType, time.Now().UnixNano())
	resultChan := make(chan *TaskResult, 1)

	go func() {
		defer close(resultChan)
		start := time.Now()

		// ワーカープールから許可取得
		select {
		case cm.workerPool <- struct{}{}:
			defer func() { <-cm.workerPool }()
		case <-ctx.Done():
			resultChan <- &TaskResult{
				TaskID:    taskID,
				DBType:    dbType,
				Error:     ctx.Err(),
				Elapsed:   time.Since(start),
				Timestamp: time.Now(),
				Operation: "transaction",
			}
			return
		}

		adapter, err := cm.adapterFor(dbType)
		if err != nil {
			resultChan <- &TaskResult{
				TaskID:    taskID,
				DBType:    dbType,
				Error:     err,
				Elapsed:   time.Since(start),
				Timestamp: time.Now(),
				Operation: "transaction",
			}
			return
		}

		// トランザクション実行
		err = adapter.Transaction(ctx, fn)

		result := &TaskResult{
			TaskID:    taskID,
			DBType:    dbType,
			Error:     err,
			Elapsed:   time.Since(start),
			Timestamp: time.Now(),
			Operation: "transaction",
		}

		cm.updateMetrics(result)
		resultChan <- result
	}()

	return resultChan
}

// processTask タスク処理
func (cm *ConcurrentDBManager) processTask(task *DatabaseTask) {
	defer close(task.ResultChan)
	start := time.Now()
	cm.metrics.ActiveTasks.Add(1)
	defer cm.metrics.ActiveTasks.Add(-1)

	// ワーカープールから許可取得
	select {
	case cm.workerPool <- struct{}{}:
		defer func() { <-cm.workerPool }()
	case <-task.Context.Done():
		task.ResultChan <- &TaskResult{
			TaskID:    task.ID,
			DBType:    task.DBType,
			Error:     task.Context.Err(),
			Elapsed:   time.Since(start),
			Timestamp: time.Now(),
			Operation: task.Operation,
		}
		return
	}

	adapter, err := cm.adapterFor(task.DBType)
	if err != nil {
		result := &TaskResult{
			TaskID:    task.ID,
			DBType:    task.DBType,
			Error:     err,
			Elapsed:   time.Since(start),
			Timestamp: time.Now(),
			Operation: task.Operation,
		}
		cm.updateMetrics(result)
		task.ResultChan <- result
		return
	}

	// 再試行機能付きクエリ実行
	result := cm.executeWithRetry(task.Context, adapter, task.Query, task.Args...)
	result.TaskID = task.ID
	result.Operation = task.Operation
	result.Timestamp = time.Now()

	// メトリクス更新
	cm.updateMetrics(result)

	// コールバック実行
	if task.Callback != nil {
		task.Callback(result)
	}

	task.ResultChan <- result
}

// executeWithRetry 再試行機能付き実行
func (cm *ConcurrentDBManager) executeWithRetry(ctx context.Context, adapter adapters.DBAdapter, query string, args ...any) *TaskResult {
	var lastErr error
	maxRetries := 3
	baseDelay := 100 * time.Millisecond
	start := time.Now()

	for attempt := range maxRetries + 1 {
		if attempt > 0 {
			// 指数バックオフ
			delay := time.Duration(attempt) * baseDelay
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return &TaskResult{
					DBType:  adapter.GetType(),
					Error:   ctx.Err(),
					Elapsed: time.Since(start),
				}
			}
		}

		queryResult, err := adapter.Query(ctx, query, args...)
		if err == nil && queryResult.Error == nil {
			return &TaskResult{
				DBType:  adapter.GetType(),
				Data:    queryResult.Data,
				Elapsed: time.Since(start),
			}
		}

		if queryResult != nil && queryResult.Error != nil {
			lastErr = queryResult.Error
		} else {
			lastErr = err
		}

		// 回復不可能なエラーの場合は即座に失敗
		if cm.isUnrecoverableError(lastErr) {
			break
		}
	}

	return &TaskResult{
		DBType:  adapter.GetType(),
		Error:   fmt.Errorf("最大再試行回数に到達: %w", lastErr),
		Elapsed: time.Since(start),
	}
}

// isUnrecoverableError 回復不可能エラー判定
func (cm *ConcurrentDBManager) isUnrecoverableError(err error) bool {
	if err == nil {
		return false
	}

	errorStr := strings.ToLower(err.Error())
	unrecoverableKeywords := []string{
		"syntax error",
		"permission denied",
		"access denied",
		"authentication failed",
		"table doesn't exist",
		"column doesn't exist",
		"invalid argument",
	}

	return slices.ContainsFunc(unrecoverableKeywords, func(keyword string) bool {
		return strings.Contains(errorStr, keyword)
	})
}

// ExecuteParallelQueries 並列クエリ実行
func (cm *ConcurrentDBManager) ExecuteParallelQueries(ctx context.Context, queries map[string]string) map[string]*TaskResult {
	results := make(map[string]*TaskResult)
	resultChannels := make(map[string]<-chan *TaskResult)

	// 全データベースに対して並列実行開始
	for dbType, query := range queries {
		resultChannels[dbType] = cm.ExecuteQuery(ctx, dbType, query)
	}

	// 全結果を収集
	for dbType, resultChan := range resultChannels {
		select {
		case result := <-resultChan:
			results[dbType] = result
		case <-ctx.Done():
			results[dbType] = &TaskResult{
				DBType:    dbType,
				Error:     ctx.Err(),
				Timestamp: time.Now(),
				Operation: "parallel_query",
			}
		}
	}

	return results
}

// ExecuteBulkOperation バルク操作実行
func (cm *ConcurrentDBManager) ExecuteBulkOperation(ctx context.Context, dbType string, users []*models.User) <-chan *TaskResult {
	taskID := fmt.Sprintf("bulk_%s_%d", dbType, time.Now().UnixNano())
	resultChan := make(chan *TaskResult, 1)

	go func() {
		defer close(resultChan)
		start := time.Now()

		// ワーカープールから許可取得
		select {
		case cm.workerPool <- struct{}{}:
			defer func() { <-cm.workerPool }()
		case <-ctx.Done():
			resultChan <- &TaskResult{
				TaskID:    taskID,
				DBType:    dbType,
				Error:     ctx.Err(),
				Elapsed:   time.Since(start),
				Timestamp: time.Now(),
				Operation: "bulk_insert",
			}
			return
		}

		adapter, err := cm.adapterFor(dbType)
		if err != nil {
			resultChan <- &TaskResult{
				TaskID:    taskID,
				DBType:    dbType,
				Error:     err,
				Elapsed:   time.Since(start),
				Timestamp: time.Now(),
				Operation: "bulk_insert",
			}
			return
		}

		// バルク挿入実行
		err = adapter.BulkInsertUsers(ctx, users)

		result := &TaskResult{
			TaskID:    taskID,
			DBType:    dbType,
			Error:     err,
			Elapsed:   time.Since(start),
			Timestamp: time.Now(),
			Operation: "bulk_insert",
		}

		cm.updateMetrics(result)
		resultChan <- result
	}()

	return resultChan
}

// updateMetrics メトリクス更新
func (cm *ConcurrentDBManager) updateMetrics(result *TaskResult) {
	m := cm.metrics
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.TotalTasks++
	if result.Error != nil {
		m.FailedTasks++
	} else {
		m.SuccessTasks++
	}

	if result.Elapsed > 0 {
		m.MaxLatency = max(m.MaxLatency, result.Elapsed)
		m.MinLatency = min(m.MinLatency, result.Elapsed)

		// 移動平均計算
		totalLatency := m.AverageLatency * time.Duration(m.TotalTasks-1)
		m.AverageLatency = (totalLatency + result.Elapsed) / time.Duration(m.TotalTasks)
	}
}

// MetricsResponse JSON用メトリクス構造体
type MetricsResponse struct {
	TotalTasks     int64         `json:"total_tasks"`
	SuccessTasks   int64         `json:"success_tasks"`
	FailedTasks    int64         `json:"failed_tasks"`
	AverageLatency time.Duration `json:"average_latency"`
	MaxLatency     time.Duration `json:"max_latency"`
	MinLatency     time.Duration `json:"min_latency"`
	ActiveTasks    int64         `json:"active_tasks"`
}

// GetMetrics メトリクス取得
func (cm *ConcurrentDBManager) GetMetrics() MetricsResponse {
	cm.metrics.mutex.RLock()
	defer cm.metrics.mutex.RUnlock()

	return MetricsResponse{
		TotalTasks:     cm.metrics.TotalTasks,
		SuccessTasks:   cm.metrics.SuccessTasks,
		FailedTasks:    cm.metrics.FailedTasks,
		AverageLatency: cm.metrics.AverageLatency,
		MaxLatency:     cm.metrics.MaxLatency,
		MinLatency:     cm.metrics.MinLatency,
		ActiveTasks:    cm.metrics.ActiveTasks.Load(),
	}
}

// HealthCheck ヘルスチェック
func (cm *ConcurrentDBManager) HealthCheck(ctx context.Context) map[string]error {
	cm.mutex.RLock()
	defer cm.mutex.RUnlock()

	results := make(map[string]error)
	var resultsMu sync.Mutex
	var wg sync.WaitGroup

	for dbType, adapter := range cm.adapters {
		wg.Go(func() {
			ctxTimeout, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			err := adapter.Ping(ctxTimeout)

			resultsMu.Lock()
			results[dbType] = err
			resultsMu.Unlock()
		})
	}

	wg.Wait()
	return results
}

// GetDatabaseStats データベース統計情報取得
func (cm *ConcurrentDBManager) GetDatabaseStats() map[string]models.DatabaseStats {
	cm.mutex.RLock()
	defer cm.mutex.RUnlock()

	results := make(map[string]models.DatabaseStats)

	for dbType, adapter := range cm.adapters {
		stats := adapter.GetStats()
		results[dbType] = models.DatabaseStats{
			MaxOpenConnections: stats.MaxOpenConnections,
			OpenConnections:    stats.OpenConnections,
			InUse:              stats.InUse,
			Idle:               stats.Idle,
			WaitCount:          stats.WaitCount,
			WaitDuration:       stats.WaitDuration,
			MaxIdleClosed:      stats.MaxIdleClosed,
			MaxIdleTimeClosed:  stats.MaxIdleTimeClosed,
			MaxLifetimeClosed:  stats.MaxLifetimeClosed,
		}
	}

	return results
}

// GetRegisteredDatabases 登録済みデータベース一覧取得
func (cm *ConcurrentDBManager) GetRegisteredDatabases() []string {
	cm.mutex.RLock()
	defer cm.mutex.RUnlock()

	// 呼び出し側が先頭を既定の DB に選ぶので、マップの順序に左右されないよう並べて返す
	return slices.Sorted(maps.Keys(cm.adapters))
}

// CreateUser ユーザー作成（アダプター固有メソッド使用）
func (cm *ConcurrentDBManager) CreateUser(ctx context.Context, dbType string, user *models.User) chan *TaskResult {
	resultChan := make(chan *TaskResult, 1)

	go func() {
		defer close(resultChan)

		start := time.Now()
		taskID := fmt.Sprintf("createuser_%s_%d", dbType, time.Now().UnixNano())

		adapter, err := cm.adapterFor(dbType)
		if err != nil {
			result := &TaskResult{
				TaskID:    taskID,
				DBType:    dbType,
				Error:     err,
				Elapsed:   time.Since(start),
				Timestamp: time.Now(),
				Operation: "CreateUser",
			}
			cm.updateMetrics(result)
			resultChan <- result
			return
		}

		// アダプター固有のCreateUserメソッドを呼び出し
		createdUser, err := adapter.CreateUser(ctx, user)
		elapsed := time.Since(start)

		var result *TaskResult
		if err != nil {
			result = &TaskResult{
				TaskID:    taskID,
				DBType:    dbType,
				Error:     err,
				Elapsed:   elapsed,
				Timestamp: time.Now(),
				Operation: "CreateUser",
			}
		} else {
			// ユーザー情報をマップ形式に変換
			userData := map[string]any{
				"id":         createdUser.ID,
				"name":       createdUser.Name,
				"email":      createdUser.Email,
				"age":        createdUser.Age,
				"created_at": createdUser.CreatedAt.Format("2006-01-02 15:04:05"),
				"updated_at": createdUser.UpdatedAt.Format("2006-01-02 15:04:05"),
			}

			result = &TaskResult{
				TaskID:    taskID,
				DBType:    dbType,
				Data:      []map[string]any{userData},
				Elapsed:   elapsed,
				Timestamp: time.Now(),
				Operation: "CreateUser",
			}
		}

		// メトリクス更新
		cm.updateMetrics(result)

		resultChan <- result
	}()

	return resultChan
}

// GetUsersByAgeRange 年齢範囲でユーザー検索（アダプター固有メソッド使用）
func (cm *ConcurrentDBManager) GetUsersByAgeRange(ctx context.Context, dbType string, minAge, maxAge int) chan *TaskResult {
	resultChan := make(chan *TaskResult, 1)

	go func() {
		defer close(resultChan)

		start := time.Now()
		taskID := fmt.Sprintf("searchusers_%s_%d", dbType, time.Now().UnixNano())

		adapter, err := cm.adapterFor(dbType)
		if err != nil {
			result := &TaskResult{
				TaskID:  taskID,
				DBType:  dbType,
				Error:   err,
				Elapsed: time.Since(start),
			}
			cm.updateMetrics(result)
			resultChan <- result
			return
		}

		// アダプター固有のGetUsersByAgeRangeメソッドを呼び出し
		users, err := adapter.GetUsersByAgeRange(ctx, minAge, maxAge)

		result := &TaskResult{
			TaskID:  taskID,
			DBType:  dbType,
			Elapsed: time.Since(start),
		}

		if err != nil {
			result.Error = err
		} else {
			result.Users = users
		}

		// メトリクス更新
		cm.updateMetrics(result)

		resultChan <- result
	}()

	return resultChan
}

// Close リソース解放
func (cm *ConcurrentDBManager) Close() error {
	// adapterFor と同じロックの中で停止を記録するので、Close 後の投入は必ず ErrManagerClosed になる
	cm.mutex.Lock()
	defer cm.mutex.Unlock()
	cm.cancel()

	var errs []error
	for dbType, adapter := range cm.adapters {
		if err := adapter.Close(); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", dbType, err))
		}
	}
	clear(cm.adapters)

	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("データベース終了エラー: %w", err)
	}

	return nil
}
