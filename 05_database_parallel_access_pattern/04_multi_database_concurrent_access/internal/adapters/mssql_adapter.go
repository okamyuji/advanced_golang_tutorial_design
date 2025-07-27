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

	_ "github.com/denisenkom/go-mssqldb"
)

// MSSQLAdapter MSSQL専用のデータベースアダプター
type MSSQLAdapter struct {
	db     *sql.DB
	config config.DatabaseConfig
	mutex  sync.RWMutex
}

// NewMSSQLAdapter 新しいMSSQLアダプターを作成
func NewMSSQLAdapter() *MSSQLAdapter {
	return &MSSQLAdapter{}
}

// Connect MSSQLデータベースに接続
func (ms *MSSQLAdapter) Connect(ctx context.Context, cfg config.DatabaseConfig) error {
	ms.mutex.Lock()
	defer ms.mutex.Unlock()

	// MSSQL接続文字列構築
	dsn := fmt.Sprintf("server=%s;port=%d;database=%s;user id=%s;password=%s;encrypt=disable;connection timeout=30",
		cfg.Host, cfg.Port, cfg.Database, cfg.Username, cfg.Password)

	db, err := sql.Open("mssql", dsn)
	if err != nil {
		return fmt.Errorf("MSSQL接続エラー: %w", err)
	}

	// MSSQL特有の接続プール設定
	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	db.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	db.SetConnMaxIdleTime(cfg.ConnMaxIdleTime)

	// 接続テスト
	ctxTimeout, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	if err := db.PingContext(ctxTimeout); err != nil {
		db.Close()
		return fmt.Errorf("MSSQL接続テストエラー: %w", err)
	}

	// MSSQL固有の設定を実行
	if err := ms.configureMSSQLSession(ctx, db); err != nil {
		db.Close()
		return fmt.Errorf("MSSQLセッション設定エラー: %w", err)
	}

	ms.db = db
	ms.config = cfg
	return nil
}

// configureMSSQLSession MSSQL固有のセッション設定
func (ms *MSSQLAdapter) configureMSSQLSession(ctx context.Context, db *sql.DB) error {
	configurations := []string{
		"SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
		"SET LOCK_TIMEOUT 30000", // 30秒
		"SET QUERY_GOVERNOR_COST_LIMIT 0",
		"SET ARITHABORT ON",
		"SET ANSI_NULL_DFLT_ON ON",
		"SET ANSI_NULLS ON",
		"SET ANSI_PADDING ON",
		"SET ANSI_WARNINGS ON",
		"SET CONCAT_NULL_YIELDS_NULL ON",
		"SET QUOTED_IDENTIFIER ON",
	}

	for _, query := range configurations {
		if _, err := db.ExecContext(ctx, query); err != nil {
			return fmt.Errorf("設定実行エラー [%s]: %w", query, err)
		}
	}

	return nil
}

// Query データを取得
func (ms *MSSQLAdapter) Query(ctx context.Context, query string, args ...interface{}) (*QueryResult, error) {
	start := time.Now()

	ms.mutex.RLock()
	defer ms.mutex.RUnlock()

	if ms.db == nil {
		return nil, fmt.Errorf("データベース未接続")
	}

	rows, err := ms.db.QueryContext(ctx, query, args...)
	if err != nil {
		return &QueryResult{
			Error:   fmt.Errorf("MSSQLクエリエラー: %w", err),
			Elapsed: time.Since(start),
			DBType:  "MSSQL",
		}, nil
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return &QueryResult{
			Error:   fmt.Errorf("カラム取得エラー: %w", err),
			Elapsed: time.Since(start),
			DBType:  "MSSQL",
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
				DBType:  "MSSQL",
			}, nil
		}

		row := make(map[string]interface{})
		for i, col := range columns {
			// MSSQL特有のデータ型変換
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
			DBType:  "MSSQL",
		}, nil
	}

	return &QueryResult{
		Data:    results,
		Elapsed: time.Since(start),
		DBType:  "MSSQL",
	}, nil
}

// Execute データを変更
func (ms *MSSQLAdapter) Execute(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	ms.mutex.RLock()
	defer ms.mutex.RUnlock()

	if ms.db == nil {
		return nil, fmt.Errorf("データベース未接続")
	}

	return ms.db.ExecContext(ctx, query, args...)
}

// Transaction トランザクション実行
func (ms *MSSQLAdapter) Transaction(ctx context.Context, fn func(*sql.Tx) error) error {
	ms.mutex.RLock()
	defer ms.mutex.RUnlock()

	if ms.db == nil {
		return fmt.Errorf("データベース未接続")
	}

	tx, err := ms.db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
	})
	if err != nil {
		return fmt.Errorf("MSSQLトランザクション開始エラー: %w", err)
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
func (ms *MSSQLAdapter) Close() error {
	ms.mutex.Lock()
	defer ms.mutex.Unlock()

	if ms.db != nil {
		return ms.db.Close()
	}
	return nil
}

// Ping 接続確認
func (ms *MSSQLAdapter) Ping(ctx context.Context) error {
	ms.mutex.RLock()
	defer ms.mutex.RUnlock()

	if ms.db == nil {
		return fmt.Errorf("データベース未接続")
	}

	return ms.db.PingContext(ctx)
}

// GetStats 統計情報取得
func (ms *MSSQLAdapter) GetStats() sql.DBStats {
	ms.mutex.RLock()
	defer ms.mutex.RUnlock()

	if ms.db == nil {
		return sql.DBStats{}
	}

	return ms.db.Stats()
}

// GetType データベースタイプを取得
func (ms *MSSQLAdapter) GetType() string {
	return "MSSQL"
}

// CreateUser ユーザー作成（MSSQL特有の最適化）
func (ms *MSSQLAdapter) CreateUser(ctx context.Context, user *models.User) (*models.User, error) {
	// まず存在チェック
	var existingID int64
	checkQuery := `SELECT id FROM users WHERE email = ?`
	err := ms.db.QueryRowContext(ctx, checkQuery, user.Email).Scan(&existingID)
	
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("存在チェックエラー: %w", err)
	}

	var id int64
	var createdAt, updatedAt time.Time

	if err == sql.ErrNoRows {
		// レコードが存在しない場合はINSERT
		insertQuery := `
			INSERT INTO users (name, email, age) 
			OUTPUT INSERTED.id, INSERTED.created_at, INSERTED.updated_at
			VALUES (?, ?, ?)
		`
		rows, err := ms.db.QueryContext(ctx, insertQuery, user.Name, user.Email, user.Age)
		if err != nil {
			return nil, fmt.Errorf("ユーザー作成エラー: %w", err)
		}
		defer rows.Close()

		if rows.Next() {
			if err := rows.Scan(&id, &createdAt, &updatedAt); err != nil {
				return nil, fmt.Errorf("挿入結果取得エラー: %w", err)
			}
		}
	} else {
		// レコードが存在する場合はUPDATE
		updateQuery := `
			UPDATE users 
			SET name = ?, age = ?, updated_at = CURRENT_TIMESTAMP 
			OUTPUT INSERTED.id, INSERTED.created_at, INSERTED.updated_at
			WHERE email = ?
		`
		rows, err := ms.db.QueryContext(ctx, updateQuery, user.Name, user.Age, user.Email)
		if err != nil {
			return nil, fmt.Errorf("ユーザー更新エラー: %w", err)
		}
		defer rows.Close()

		if rows.Next() {
			if err := rows.Scan(&id, &createdAt, &updatedAt); err != nil {
				return nil, fmt.Errorf("更新結果取得エラー: %w", err)
			}
		}
	}

	user.ID = id
	user.CreatedAt = createdAt
	user.UpdatedAt = updatedAt

	return user, nil
}

// GetUsersByAgeRange 年齢範囲でユーザーを取得（MSSQL特有の最適化）
func (ms *MSSQLAdapter) GetUsersByAgeRange(ctx context.Context, minAge, maxAge int) ([]*models.User, error) {
	query := `
		SELECT TOP 1000 id, name, email, age, created_at, updated_at 
		FROM users WITH (NOLOCK)
		WHERE age BETWEEN ? AND ? 
		ORDER BY created_at DESC
	`

	result, err := ms.Query(ctx, query, minAge, maxAge)
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

// BulkInsertUsers バッチでユーザーを挿入（MSSQL特有の最適化）
func (ms *MSSQLAdapter) BulkInsertUsers(ctx context.Context, users []*models.User) error {
	if len(users) == 0 {
		return nil
	}

	// MSSQL最適化: Table Valued Parametersを使用したバッチ挿入
	// ここでは簡易版として通常のバッチ挿入を実装
	batchSize := 1000
	for i := 0; i < len(users); i += batchSize {
		end := i + batchSize
		if end > len(users) {
			end = len(users)
		}

		if err := ms.insertBatch(ctx, users[i:end]); err != nil {
			return fmt.Errorf("バッチ挿入エラー (batch %d-%d): %w", i, end-1, err)
		}
	}

	return nil
}

func (ms *MSSQLAdapter) insertBatch(ctx context.Context, users []*models.User) error {
	if len(users) == 0 {
		return nil
	}

	// MSSQLの場合、パラメータ数に制限があるため小さなバッチサイズを使用
	maxParamsPerBatch := 2000 // SQL Server limitation
	paramsPerUser := 3
	usersPerBatch := maxParamsPerBatch / paramsPerUser

	for i := 0; i < len(users); i += usersPerBatch {
		end := i + usersPerBatch
		if end > len(users) {
			end = len(users)
		}

		batchUsers := users[i:end]
		query := "INSERT INTO users (name, email, age) VALUES "
		values := make([]interface{}, 0, len(batchUsers)*paramsPerUser)
		placeholders := make([]string, len(batchUsers))

		for j, user := range batchUsers {
			paramStart := j*paramsPerUser + 1
			placeholders[j] = fmt.Sprintf("(@p%d, @p%d, @p%d)", paramStart, paramStart+1, paramStart+2)
			values = append(values, user.Name, user.Email, user.Age)
		}

		query += strings.Join(placeholders, ", ")

		if _, err := ms.Execute(ctx, query, values...); err != nil {
			return fmt.Errorf("バッチ挿入実行エラー: %w", err)
		}
	}

	return nil
}

// ExecuteStoredProcedure ストアドプロシージャ実行（MSSQL特有）
func (ms *MSSQLAdapter) ExecuteStoredProcedure(ctx context.Context, procName string, args ...interface{}) (*QueryResult, error) {
	start := time.Now()

	ms.mutex.RLock()
	defer ms.mutex.RUnlock()

	if ms.db == nil {
		return nil, fmt.Errorf("データベース未接続")
	}

	// ストアドプロシージャ呼び出し文字列構築
	placeholders := make([]string, len(args))
	for i := range args {
		placeholders[i] = fmt.Sprintf("@p%d", i+1)
	}

	query := fmt.Sprintf("EXEC %s %s", procName, strings.Join(placeholders, ", "))

	rows, err := ms.db.QueryContext(ctx, query, args...)
	if err != nil {
		return &QueryResult{
			Error:   fmt.Errorf("ストアドプロシージャ実行エラー: %w", err),
			Elapsed: time.Since(start),
			DBType:  "MSSQL",
		}, nil
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return &QueryResult{
			Error:   fmt.Errorf("カラム取得エラー: %w", err),
			Elapsed: time.Since(start),
			DBType:  "MSSQL",
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
				DBType:  "MSSQL",
			}, nil
		}

		row := make(map[string]interface{})
		for i, col := range columns {
			row[col] = values[i]
		}
		results = append(results, row)
	}

	return &QueryResult{
		Data:    results,
		Elapsed: time.Since(start),
		DBType:  "MSSQL",
	}, nil
}
