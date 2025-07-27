package adapters

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"multi-db/internal/config"
	"multi-db/internal/models"

	_ "github.com/lib/pq"
)

// PostgreSQLAdapter PostgreSQL専用のデータベースアダプター
type PostgreSQLAdapter struct {
	db     *sql.DB
	config config.DatabaseConfig
	mutex  sync.RWMutex
}

// NewPostgreSQLAdapter 新しいPostgreSQLアダプターを作成
func NewPostgreSQLAdapter() *PostgreSQLAdapter {
	return &PostgreSQLAdapter{}
}

// Connect PostgreSQLデータベースに接続
func (p *PostgreSQLAdapter) Connect(ctx context.Context, cfg config.DatabaseConfig) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	// PostgreSQL接続文字列構築
	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable connect_timeout=30",
		cfg.Host, cfg.Port, cfg.Username, cfg.Password, cfg.Database)

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return fmt.Errorf("PostgreSQL接続エラー: %w", err)
	}

	// PostgreSQL特有の接続プール設定
	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	db.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	db.SetConnMaxIdleTime(cfg.ConnMaxIdleTime)

	// 接続テスト
	ctxTimeout, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	if err := db.PingContext(ctxTimeout); err != nil {
		db.Close()
		return fmt.Errorf("PostgreSQL接続テストエラー: %w", err)
	}

	// PostgreSQL固有の設定を実行
	if err := p.configurePostgreSQLSession(ctx, db); err != nil {
		db.Close()
		return fmt.Errorf("PostgreSQLセッション設定エラー: %w", err)
	}

	p.db = db
	p.config = cfg
	return nil
}

// configurePostgreSQLSession PostgreSQL固有のセッション設定
func (p *PostgreSQLAdapter) configurePostgreSQLSession(ctx context.Context, db *sql.DB) error {
	configurations := []string{
		"SET statement_timeout = '30s'",
		"SET lock_timeout = '30s'",
		"SET idle_in_transaction_session_timeout = '60s'",
		"SET default_transaction_isolation = 'read committed'",
		"SET timezone = 'UTC'",
		"SET datestyle = 'ISO, MDY'",
		"SET enable_seqscan = on",
		"SET enable_indexscan = on",
		"SET shared_preload_libraries = ''",
	}

	for _, query := range configurations {
		if _, err := db.ExecContext(ctx, query); err != nil {
			// 一部の設定は権限によって失敗する可能性があるが、続行
			continue
		}
	}

	return nil
}

// Query データを取得
func (p *PostgreSQLAdapter) Query(ctx context.Context, query string, args ...interface{}) (*QueryResult, error) {
	start := time.Now()

	p.mutex.RLock()
	defer p.mutex.RUnlock()

	if p.db == nil {
		return nil, fmt.Errorf("データベース未接続")
	}

	rows, err := p.db.QueryContext(ctx, query, args...)
	if err != nil {
		return &QueryResult{
			Error:   fmt.Errorf("PostgreSQLクエリエラー: %w", err),
			Elapsed: time.Since(start),
			DBType:  "PostgreSQL",
		}, nil
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return &QueryResult{
			Error:   fmt.Errorf("カラム取得エラー: %w", err),
			Elapsed: time.Since(start),
			DBType:  "PostgreSQL",
		}, nil
	}

	var results []map[string]interface{}
	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range columns {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return &QueryResult{
				Error:   fmt.Errorf("行スキャンエラー: %w", err),
				Elapsed: time.Since(start),
				DBType:  "PostgreSQL",
			}, nil
		}

		row := make(map[string]interface{})
		for i, col := range columns {
			// PostgreSQL特有のデータ型変換
			val := values[i]
			if val != nil {
				switch v := val.(type) {
				case []byte:
					row[col] = string(v)
				case time.Time:
					row[col] = v.Format("2006-01-02 15:04:05")
				default:
					row[col] = v
				}
			} else {
				row[col] = nil
			}
		}
		results = append(results, row)
	}

	if err := rows.Err(); err != nil {
		return &QueryResult{
			Error:   fmt.Errorf("行イテレーションエラー: %w", err),
			Elapsed: time.Since(start),
			DBType:  "PostgreSQL",
		}, nil
	}

	return &QueryResult{
		Data:    results,
		Elapsed: time.Since(start),
		DBType:  "PostgreSQL",
	}, nil
}

// Execute データを変更
func (p *PostgreSQLAdapter) Execute(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	if p.db == nil {
		return nil, fmt.Errorf("データベース未接続")
	}

	return p.db.ExecContext(ctx, query, args...)
}

// Transaction トランザクション実行
func (p *PostgreSQLAdapter) Transaction(ctx context.Context, fn func(*sql.Tx) error) error {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	if p.db == nil {
		return fmt.Errorf("データベース未接続")
	}

	tx, err := p.db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
	})
	if err != nil {
		return fmt.Errorf("PostgreSQLトランザクション開始エラー: %w", err)
	}

	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
			panic(r)
		}
	}()

	if err := fn(tx); err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit()
}

// Close 接続を閉じる
func (p *PostgreSQLAdapter) Close() error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.db != nil {
		return p.db.Close()
	}
	return nil
}

// Ping 接続確認
func (p *PostgreSQLAdapter) Ping(ctx context.Context) error {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	if p.db == nil {
		return fmt.Errorf("データベース未接続")
	}

	return p.db.PingContext(ctx)
}

// GetStats 統計情報取得
func (p *PostgreSQLAdapter) GetStats() sql.DBStats {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	if p.db == nil {
		return sql.DBStats{}
	}

	return p.db.Stats()
}

// GetType データベースタイプを取得
func (p *PostgreSQLAdapter) GetType() string {
	return "PostgreSQL"
}

// CreateUser ユーザー作成（PostgreSQL特有の最適化）
func (p *PostgreSQLAdapter) CreateUser(ctx context.Context, user *models.User) (*models.User, error) {
	// まず存在チェック
	var existingID int64
	checkQuery := `SELECT id FROM users WHERE email = $1`
	err := p.db.QueryRowContext(ctx, checkQuery, user.Email).Scan(&existingID)
	
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("存在チェックエラー: %w", err)
	}

	var id int64
	var createdAt, updatedAt time.Time

	if err == sql.ErrNoRows {
		// レコードが存在しない場合はINSERT
		insertQuery := `
			INSERT INTO users (name, email, age) 
			VALUES ($1, $2, $3) 
			RETURNING id, created_at, updated_at
		`
		err = p.db.QueryRowContext(ctx, insertQuery, user.Name, user.Email, user.Age).Scan(&id, &createdAt, &updatedAt)
	} else {
		// レコードが存在する場合はUPDATE
		updateQuery := `
			UPDATE users SET name = $1, age = $2, updated_at = CURRENT_TIMESTAMP 
			WHERE email = $3 
			RETURNING id, created_at, updated_at
		`
		err = p.db.QueryRowContext(ctx, updateQuery, user.Name, user.Age, user.Email).Scan(&id, &createdAt, &updatedAt)
	}
	if err != nil {
		return nil, fmt.Errorf("ユーザー作成エラー: %w", err)
	}

	user.ID = id
	user.CreatedAt = createdAt
	user.UpdatedAt = updatedAt

	return user, nil
}

// GetUsersByAgeRange 年齢範囲でユーザーを取得（PostgreSQL特有の最適化）
func (p *PostgreSQLAdapter) GetUsersByAgeRange(ctx context.Context, minAge, maxAge int) ([]*models.User, error) {
	query := `
		SELECT id, name, email, age, created_at, updated_at 
		FROM users 
		WHERE age BETWEEN $1 AND $2 
		ORDER BY created_at DESC 
		LIMIT 1000
	`

	result, err := p.Query(ctx, query, minAge, maxAge)
	if err != nil {
		return nil, fmt.Errorf("年齢範囲クエリエラー: %w", err)
	}

	var users []*models.User
	for _, row := range result.Data {
		user := &models.User{}
		if id, ok := row["id"].(int64); ok {
			user.ID = id
		}
		if name, ok := row["name"].(string); ok {
			user.Name = name
		}
		if email, ok := row["email"].(string); ok {
			user.Email = email
		}
		if age, ok := row["age"].(int64); ok {
			user.Age = int(age)
		}
		if createdAt, ok := row["created_at"].(string); ok {
			if t, err := time.Parse("2006-01-02 15:04:05", createdAt); err == nil {
				user.CreatedAt = t
			}
		}
		if updatedAt, ok := row["updated_at"].(string); ok {
			if t, err := time.Parse("2006-01-02 15:04:05", updatedAt); err == nil {
				user.UpdatedAt = t
			}
		}

		users = append(users, user)
	}

	return users, nil
}

// BulkInsertUsers バッチでユーザーを挿入（PostgreSQL特有の最適化）
func (p *PostgreSQLAdapter) BulkInsertUsers(ctx context.Context, users []*models.User) error {
	if len(users) == 0 {
		return nil
	}

	// PostgreSQL最適化: COPY文またはUNNESTを使用
	// ここではパフォーマンスの良いVALUES文を使用
	batchSize := 1000
	for i := 0; i < len(users); i += batchSize {
		end := i + batchSize
		if end > len(users) {
			end = len(users)
		}

		if err := p.insertBatch(ctx, users[i:end]); err != nil {
			return fmt.Errorf("バッチ挿入エラー (batch %d-%d): %w", i, end-1, err)
		}
	}

	return nil
}

func (p *PostgreSQLAdapter) insertBatch(ctx context.Context, users []*models.User) error {
	if len(users) == 0 {
		return nil
	}

	// PostgreSQL用の効率的なバッチ挿入
	var valueStrings []string
	var valueArgs []interface{}

	for i, user := range users {
		valueStrings = append(valueStrings, fmt.Sprintf("($%d, $%d, $%d)", i*3+1, i*3+2, i*3+3))
		valueArgs = append(valueArgs, user.Name, user.Email, user.Age)
	}

	query := fmt.Sprintf("INSERT INTO users (name, email, age) VALUES %s", strings.Join(valueStrings, ", "))

	_, err := p.Execute(ctx, query, valueArgs...)
	return err
}

// GetExplainPlan 実行計画取得（PostgreSQL特有）
func (p *PostgreSQLAdapter) GetExplainPlan(ctx context.Context, query string, args ...interface{}) (*QueryResult, error) {
	start := time.Now()

	// PostgreSQLのEXPLAIN ANALYZE実行
	explainQuery := "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + query

	result, err := p.Query(ctx, explainQuery, args...)
	if err != nil {
		return &QueryResult{
			Error:   fmt.Errorf("実行計画取得エラー: %w", err),
			Elapsed: time.Since(start),
			DBType:  "PostgreSQL",
		}, nil
	}

	result.Elapsed = time.Since(start)
	return result, nil
}

// VacuumAnalyze VACUUM ANALYZE実行（PostgreSQL特有）
func (p *PostgreSQLAdapter) VacuumAnalyze(ctx context.Context, tableName string) error {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	if p.db == nil {
		return fmt.Errorf("データベース未接続")
	}

	var query string
	if tableName == "" {
		query = "VACUUM ANALYZE"
	} else {
		query = fmt.Sprintf("VACUUM ANALYZE %s", tableName)
	}

	_, err := p.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("VACUUM ANALYZE実行エラー: %w", err)
	}

	return nil
}

// CreateIndex インデックス作成（PostgreSQL特有）
func (p *PostgreSQLAdapter) CreateIndex(ctx context.Context, indexName, tableName, columns string, unique bool) error {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	if p.db == nil {
		return fmt.Errorf("データベース未接続")
	}

	var query string
	if unique {
		query = fmt.Sprintf("CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS %s ON %s (%s)", indexName, tableName, columns)
	} else {
		query = fmt.Sprintf("CREATE INDEX CONCURRENTLY IF NOT EXISTS %s ON %s (%s)", indexName, tableName, columns)
	}

	_, err := p.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("インデックス作成エラー: %w", err)
	}

	return nil
}

// GetTableStats テーブル統計情報取得（PostgreSQL特有）
func (p *PostgreSQLAdapter) GetTableStats(ctx context.Context, tableName string) (*QueryResult, error) {
	query := `
		SELECT 
			schemaname,
			tablename,
			attname,
			n_distinct,
			most_common_vals,
			most_common_freqs,
			histogram_bounds
		FROM pg_stats 
		WHERE tablename = $1
		ORDER BY attname
	`

	return p.Query(ctx, query, tableName)
}

// GetConnectionStats 接続統計情報取得（PostgreSQL特有）
func (p *PostgreSQLAdapter) GetConnectionStats(ctx context.Context) (*QueryResult, error) {
	query := `
		SELECT 
			state,
			COUNT(*) as count,
			MAX(now() - backend_start) as max_backend_duration,
			MAX(now() - state_change) as max_state_duration
		FROM pg_stat_activity 
		WHERE pid != pg_backend_pid()
		GROUP BY state
		ORDER BY count DESC
	`

	return p.Query(ctx, query)
}
