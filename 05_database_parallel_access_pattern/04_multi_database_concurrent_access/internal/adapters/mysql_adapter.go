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

	_ "github.com/go-sql-driver/mysql"
)

// MySQLAdapter MySQL専用のデータベースアダプター
type MySQLAdapter struct {
	db     *sql.DB
	config config.DatabaseConfig
	mutex  sync.RWMutex
}

// NewMySQLAdapter 新しいMySQLアダプターを作成
func NewMySQLAdapter() *MySQLAdapter {
	return &MySQLAdapter{}
}

// Connect MySQLデータベースに接続
func (m *MySQLAdapter) Connect(ctx context.Context, cfg config.DatabaseConfig) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// MySQL接続文字列構築
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local&timeout=30s&readTimeout=30s&writeTimeout=30s",
		cfg.Username, cfg.Password, cfg.Host, cfg.Port, cfg.Database)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("MySQL接続エラー: %w", err)
	}

	// MySQL特有の接続プール設定
	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	db.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	db.SetConnMaxIdleTime(cfg.ConnMaxIdleTime)

	// 接続テスト
	ctxTimeout, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	if err := db.PingContext(ctxTimeout); err != nil {
		db.Close()
		return fmt.Errorf("MySQL接続テストエラー: %w", err)
	}

	// MySQL固有の設定を実行
	if err := m.configureMySQLSession(ctx, db); err != nil {
		db.Close()
		return fmt.Errorf("MySQLセッション設定エラー: %w", err)
	}

	m.db = db
	m.config = cfg
	return nil
}

// configureMySQLSession MySQL固有のセッション設定
func (m *MySQLAdapter) configureMySQLSession(ctx context.Context, db *sql.DB) error {
	configurations := []string{
		"SET SESSION sql_mode = 'STRICT_TRANS_TABLES,NO_ZERO_DATE,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO'",
		"SET SESSION innodb_lock_wait_timeout = 50",
		"SET SESSION wait_timeout = 3600",
		"SET SESSION interactive_timeout = 3600",
		"SET SESSION max_execution_time = 30000", // 30秒
		"SET SESSION transaction_isolation = 'READ-COMMITTED'",
	}

	for _, query := range configurations {
		if _, err := db.ExecContext(ctx, query); err != nil {
			return fmt.Errorf("設定実行エラー [%s]: %w", query, err)
		}
	}

	return nil
}

// Query データを取得
func (m *MySQLAdapter) Query(ctx context.Context, query string, args ...interface{}) (*QueryResult, error) {
	start := time.Now()

	m.mutex.RLock()
	defer m.mutex.RUnlock()

	if m.db == nil {
		return nil, fmt.Errorf("データベース未接続")
	}

	rows, err := m.db.QueryContext(ctx, query, args...)
	if err != nil {
		return &QueryResult{
			Error:   fmt.Errorf("MySQLクエリエラー: %w", err),
			Elapsed: time.Since(start),
			DBType:  "MySQL",
		}, nil
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return &QueryResult{
			Error:   fmt.Errorf("カラム取得エラー: %w", err),
			Elapsed: time.Since(start),
			DBType:  "MySQL",
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
				DBType:  "MySQL",
			}, nil
		}

		row := make(map[string]interface{})
		for i, col := range columns {
			// MySQL特有のデータ型変換
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
			DBType:  "MySQL",
		}, nil
	}

	return &QueryResult{
		Data:    results,
		Elapsed: time.Since(start),
		DBType:  "MySQL",
	}, nil
}

// Execute データを変更
func (m *MySQLAdapter) Execute(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	if m.db == nil {
		return nil, fmt.Errorf("データベース未接続")
	}

	return m.db.ExecContext(ctx, query, args...)
}

// Transaction トランザクション実行
func (m *MySQLAdapter) Transaction(ctx context.Context, fn func(*sql.Tx) error) error {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	if m.db == nil {
		return fmt.Errorf("データベース未接続")
	}

	tx, err := m.db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
	})
	if err != nil {
		return fmt.Errorf("MySQLトランザクション開始エラー: %w", err)
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
func (m *MySQLAdapter) Close() error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.db != nil {
		return m.db.Close()
	}
	return nil
}

// Ping 接続確認
func (m *MySQLAdapter) Ping(ctx context.Context) error {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	if m.db == nil {
		return fmt.Errorf("データベース未接続")
	}

	return m.db.PingContext(ctx)
}

// GetStats 統計情報取得
func (m *MySQLAdapter) GetStats() sql.DBStats {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	if m.db == nil {
		return sql.DBStats{}
	}

	return m.db.Stats()
}

// GetType データベースタイプを取得
func (m *MySQLAdapter) GetType() string {
	return "MySQL"
}

// CreateUser ユーザー作成（MySQL特有の最適化）
func (m *MySQLAdapter) CreateUser(ctx context.Context, user *models.User) (*models.User, error) {
	// まず存在チェック
	var existingID int64
	checkQuery := `SELECT id FROM users WHERE email = ?`
	err := m.db.QueryRowContext(ctx, checkQuery, user.Email).Scan(&existingID)
	
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("存在チェックエラー: %w", err)
	}

	if err == sql.ErrNoRows {
		// レコードが存在しない場合はINSERT
		insertQuery := `INSERT INTO users (name, email, age) VALUES (?, ?, ?)`
		result, err := m.Execute(ctx, insertQuery, user.Name, user.Email, user.Age)
		if err != nil {
			return nil, fmt.Errorf("ユーザー作成エラー: %w", err)
		}
		
		id, err := result.LastInsertId()
		if err != nil {
			return nil, fmt.Errorf("挿入ID取得エラー: %w", err)
		}
		
		user.ID = id
		user.CreatedAt = time.Now()
		user.UpdatedAt = time.Now()
	} else {
		// レコードが存在する場合はUPDATE
		updateQuery := `UPDATE users SET name = ?, age = ?, updated_at = CURRENT_TIMESTAMP WHERE email = ?`
		_, err := m.Execute(ctx, updateQuery, user.Name, user.Age, user.Email)
		if err != nil {
			return nil, fmt.Errorf("ユーザー更新エラー: %w", err)
		}
		
		user.ID = existingID
		user.UpdatedAt = time.Now()
		
		// created_atを取得
		selectQuery := `SELECT created_at FROM users WHERE id = ?`
		err = m.db.QueryRowContext(ctx, selectQuery, existingID).Scan(&user.CreatedAt)
		if err != nil {
			return nil, fmt.Errorf("作成日時取得エラー: %w", err)
		}
	}

	return user, nil
}

// GetUsersByAgeRange 年齢範囲でユーザーを取得（MySQL特有の最適化）
func (m *MySQLAdapter) GetUsersByAgeRange(ctx context.Context, minAge, maxAge int) ([]*models.User, error) {
	query := `
		SELECT id, name, email, age, created_at, updated_at 
		FROM users 
		WHERE age BETWEEN ? AND ? 
		ORDER BY created_at DESC 
		LIMIT 1000
	`

	result, err := m.Query(ctx, query, minAge, maxAge)
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

// BulkInsertUsers バッチでユーザーを挿入（MySQL特有の最適化）
func (m *MySQLAdapter) BulkInsertUsers(ctx context.Context, users []*models.User) error {
	if len(users) == 0 {
		return nil
	}

	// MySQL最適化: バッチサイズを制限
	batchSize := 1000
	for i := 0; i < len(users); i += batchSize {
		end := i + batchSize
		if end > len(users) {
			end = len(users)
		}

		if err := m.insertBatch(ctx, users[i:end]); err != nil {
			return fmt.Errorf("バッチ挿入エラー (batch %d-%d): %w", i, end-1, err)
		}
	}

	return nil
}

func (m *MySQLAdapter) insertBatch(ctx context.Context, users []*models.User) error {
	if len(users) == 0 {
		return nil
	}

	// 動的にINSERT文を構築
	query := "INSERT INTO users (name, email, age) VALUES "
	values := make([]interface{}, 0, len(users)*3)
	placeholders := make([]string, len(users))

	for i, user := range users {
		placeholders[i] = "(?, ?, ?)"
		values = append(values, user.Name, user.Email, user.Age)
	}

	query += strings.Join(placeholders, ", ")

	_, err := m.Execute(ctx, query, values...)
	return err
}
