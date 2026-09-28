package adapters

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"multi-db/internal/config"
	"multi-db/internal/models"

	_ "github.com/sijms/go-ora/v2"
)

// OracleAdapter Oracle専用のデータベースアダプター
type OracleAdapter struct {
	db     *sql.DB
	config config.DatabaseConfig
	mutex  sync.RWMutex
}

// NewOracleAdapter 新しいOracleアダプターを作成
func NewOracleAdapter() *OracleAdapter {
	return &OracleAdapter{}
}

// Connect Oracleデータベースに接続
func (o *OracleAdapter) Connect(ctx context.Context, cfg config.DatabaseConfig) error {
	o.mutex.Lock()
	defer o.mutex.Unlock()

	// Oracle接続文字列構築
	dsn := fmt.Sprintf("oracle://%s:%s@%s:%d/%s",
		cfg.Username, cfg.Password, cfg.Host, cfg.Port, cfg.Database)

	db, err := sql.Open("oracle", dsn)
	if err != nil {
		return fmt.Errorf("データベース接続エラー (Oracle): %w", err)
	}

	// Oracle特有の接続プール設定
	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	db.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	db.SetConnMaxIdleTime(cfg.ConnMaxIdleTime)

	// 接続テスト
	ctxTimeout, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	if err := db.PingContext(ctxTimeout); err != nil {
		return fmt.Errorf("データベース接続テストエラー (Oracle): %w", errors.Join(err, db.Close()))
	}

	// Oracle固有の設定を実行
	if err := o.configureOracleSession(ctx, db); err != nil {
		return fmt.Errorf("データベースセッション設定エラー (Oracle): %w", errors.Join(err, db.Close()))
	}

	o.db = db
	o.config = cfg
	return nil
}

// configureOracleSession Oracle固有のセッション設定
func (o *OracleAdapter) configureOracleSession(ctx context.Context, db *sql.DB) error {
	configurations := []string{
		"ALTER SESSION SET CURRENT_SCHEMA = TESTUSER",
		"ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'",
		"ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF'",
		"ALTER SESSION SET NLS_NUMERIC_CHARACTERS = '.,'",
		"ALTER SESSION SET OPTIMIZER_MODE = ALL_ROWS",
		"ALTER SESSION SET CURSOR_SHARING = EXACT",
		"ALTER SESSION SET SORT_AREA_SIZE = 65536",
		"ALTER SESSION SET HASH_AREA_SIZE = 131072",
	}

	for _, query := range configurations {
		if _, err := db.ExecContext(ctx, query); err != nil {
			// 一部の設定は失敗しても続行（権限の問題など）
			// 本番環境では適切なログ出力を行う
			continue
		}
	}

	return nil
}

// Query データを取得
func (o *OracleAdapter) Query(ctx context.Context, query string, args ...any) (*QueryResult, error) {
	start := time.Now()

	o.mutex.RLock()
	defer o.mutex.RUnlock()

	if o.db == nil {
		return nil, fmt.Errorf("データベース未接続")
	}

	rows, err := o.db.QueryContext(ctx, query, args...)
	if err != nil {
		return &QueryResult{
			Error:   fmt.Errorf("クエリエラー (Oracle): %w", err),
			Elapsed: time.Since(start),
			DBType:  "Oracle",
		}, nil
	}
	defer closeRows(rows)

	columns, err := rows.Columns()
	if err != nil {
		return &QueryResult{
			Error:   fmt.Errorf("カラム取得エラー: %w", err),
			Elapsed: time.Since(start),
			DBType:  "Oracle",
		}, nil
	}

	var results []map[string]any
	for rows.Next() {
		values := make([]any, len(columns))
		valuePtrs := make([]any, len(columns))
		for i := range columns {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return &QueryResult{
				Error:   fmt.Errorf("行スキャンエラー: %w", err),
				Elapsed: time.Since(start),
				DBType:  "Oracle",
			}, nil
		}

		row := make(map[string]any)
		for i, col := range columns {
			// Oracle特有のデータ型変換
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
			DBType:  "Oracle",
		}, nil
	}

	return &QueryResult{
		Data:    results,
		Elapsed: time.Since(start),
		DBType:  "Oracle",
	}, nil
}

// Execute データを変更
func (o *OracleAdapter) Execute(ctx context.Context, query string, args ...any) (sql.Result, error) {
	o.mutex.RLock()
	defer o.mutex.RUnlock()

	if o.db == nil {
		return nil, fmt.Errorf("データベース未接続")
	}

	return o.db.ExecContext(ctx, query, args...)
}

// Transaction トランザクション実行
func (o *OracleAdapter) Transaction(ctx context.Context, fn func(*sql.Tx) error) error {
	o.mutex.RLock()
	defer o.mutex.RUnlock()

	if o.db == nil {
		return fmt.Errorf("データベース未接続")
	}

	tx, err := o.db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
	})
	if err != nil {
		return fmt.Errorf("トランザクション開始エラー (Oracle): %w", err)
	}

	defer func() {
		if r := recover(); r != nil {
			if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
				log.Printf("ロールバックエラー: %v", err)
			}
			panic(r)
		}
	}()

	if err := fn(tx); err != nil {
		if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
			return errors.Join(err, fmt.Errorf("ロールバックエラー: %w", rbErr))
		}
		return err
	}

	return tx.Commit()
}

// Close 接続を閉じる
func (o *OracleAdapter) Close() error {
	o.mutex.Lock()
	defer o.mutex.Unlock()

	if o.db != nil {
		return o.db.Close()
	}
	return nil
}

// Ping 接続確認
func (o *OracleAdapter) Ping(ctx context.Context) error {
	o.mutex.RLock()
	defer o.mutex.RUnlock()

	if o.db == nil {
		return fmt.Errorf("データベース未接続")
	}

	return o.db.PingContext(ctx)
}

// GetStats 統計情報取得
func (o *OracleAdapter) GetStats() sql.DBStats {
	o.mutex.RLock()
	defer o.mutex.RUnlock()

	if o.db == nil {
		return sql.DBStats{}
	}

	return o.db.Stats()
}

// GetType データベースタイプを取得
func (o *OracleAdapter) GetType() string {
	return "Oracle"
}

// CreateUser ユーザー作成（Oracle特有の最適化）
func (o *OracleAdapter) CreateUser(ctx context.Context, user *models.User) (*models.User, error) {
	// まず存在チェック
	var existingID int64
	checkQuery := `SELECT id FROM users WHERE email = :1`
	err := o.db.QueryRowContext(ctx, checkQuery, user.Email).Scan(&existingID)

	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("存在チェックエラー: %w", err)
	}

	var id int64
	var createdAt, updatedAt time.Time

	if errors.Is(err, sql.ErrNoRows) {
		// レコードが存在しない場合はINSERT
		insertQuery := `
			INSERT INTO users (name, email, age) 
			VALUES (:1, :2, :3)
		`
		_, err := o.Execute(ctx, insertQuery, user.Name, user.Email, user.Age)
		if err != nil {
			return nil, fmt.Errorf("ユーザー作成エラー: %w", err)
		}

		// 挿入したレコードのIDを取得
		selectQuery := `SELECT id, created_at, updated_at FROM users WHERE email = :1`
		err = o.db.QueryRowContext(ctx, selectQuery, user.Email).Scan(&id, &createdAt, &updatedAt)
		if err != nil {
			return nil, fmt.Errorf("挿入ID取得エラー: %w", err)
		}
	} else {
		// レコードが存在する場合はUPDATE
		updateQuery := `
			UPDATE users 
			SET name = :1, age = :2, updated_at = CURRENT_TIMESTAMP 
			WHERE email = :3
		`
		_, err := o.Execute(ctx, updateQuery, user.Name, user.Age, user.Email)
		if err != nil {
			return nil, fmt.Errorf("ユーザー更新エラー: %w", err)
		}

		// 更新したレコードの情報を取得
		selectQuery := `SELECT id, created_at, updated_at FROM users WHERE email = :1`
		err = o.db.QueryRowContext(ctx, selectQuery, user.Email).Scan(&id, &createdAt, &updatedAt)
		if err != nil {
			return nil, fmt.Errorf("更新結果取得エラー: %w", err)
		}
	}

	user.ID = id
	user.CreatedAt = createdAt
	user.UpdatedAt = updatedAt

	return user, nil
}

// GetUsersByAgeRange 年齢範囲でユーザーを取得（Oracle特有の最適化）
func (o *OracleAdapter) GetUsersByAgeRange(ctx context.Context, minAge, maxAge int) ([]*models.User, error) {
	query := `
		SELECT id, name, email, age, created_at, updated_at 
		FROM users 
		WHERE age BETWEEN :1 AND :2 
		ORDER BY created_at DESC 
		FETCH FIRST 1000 ROWS ONLY
	`

	result, err := o.Query(ctx, query, minAge, maxAge)
	if err != nil {
		return nil, fmt.Errorf("年齢範囲クエリエラー: %w", err)
	}
	if result.Error != nil {
		return nil, fmt.Errorf("年齢範囲クエリエラー: %w", result.Error)
	}

	var users []*models.User
	for _, row := range result.Data {
		user := &models.User{}
		user.ID = oracleInt64(row["ID"]) // Oracle のカラム名は大文字で返る
		if name, ok := row["NAME"].(string); ok {
			user.Name = name
		}
		if email, ok := row["EMAIL"].(string); ok {
			user.Email = email
		}
		user.Age = int(oracleInt64(row["AGE"]))
		// Query が time.Time を文字列に変換して返すので、ここで戻す
		if createdAt, ok := row["CREATED_AT"].(string); ok {
			if t, err := time.Parse("2006-01-02 15:04:05", createdAt); err == nil {
				user.CreatedAt = t
			}
		}
		if updatedAt, ok := row["UPDATED_AT"].(string); ok {
			if t, err := time.Parse("2006-01-02 15:04:05", updatedAt); err == nil {
				user.UpdatedAt = t
			}
		}

		users = append(users, user)
	}

	return users, nil
}

// BulkInsertUsers バッチでユーザーを挿入（Oracle特有の最適化）
func (o *OracleAdapter) BulkInsertUsers(ctx context.Context, users []*models.User) error {
	if len(users) == 0 {
		return nil
	}

	// Oracle最適化: BULK COLLECT/FORALL を使用
	// ここでは簡易版として通常のバッチ挿入を実装
	batchSize := 1000
	offset := 0
	for batch := range slices.Chunk(users, batchSize) {
		if err := o.insertBatch(ctx, batch); err != nil {
			return fmt.Errorf("バッチ挿入エラー (batch %d-%d): %w", offset, offset+len(batch)-1, err)
		}
		offset += len(batch)
	}

	return nil
}

func (o *OracleAdapter) insertBatch(ctx context.Context, users []*models.User) error {
	if len(users) == 0 {
		return nil
	}

	// Oracleの場合、複数行挿入を使用
	var valueStrings []string
	var valueArgs []any

	for i, user := range users {
		valueStrings = append(valueStrings, fmt.Sprintf("(:p%d, :p%d, :p%d)", i*3+1, i*3+2, i*3+3))
		valueArgs = append(valueArgs, user.Name, user.Email, user.Age)
	}

	query := fmt.Sprintf("INSERT INTO users (name, email, age) VALUES %s", strings.Join(valueStrings, ", "))

	_, err := o.Execute(ctx, query, valueArgs...)
	return err
}

// ExecuteStoredProcedure ストアドプロシージャ実行（Oracle特有）
func (o *OracleAdapter) ExecuteStoredProcedure(ctx context.Context, procName string, args ...any) (*QueryResult, error) {
	start := time.Now()

	o.mutex.RLock()
	defer o.mutex.RUnlock()

	if o.db == nil {
		return nil, fmt.Errorf("データベース未接続")
	}

	// Oracleストアドプロシージャ呼び出し
	placeholders := make([]string, len(args))
	for i := range args {
		placeholders[i] = fmt.Sprintf(":p%d", i+1)
	}

	query := fmt.Sprintf("BEGIN %s(%s); END;", procName, strings.Join(placeholders, ", "))

	_, err := o.db.ExecContext(ctx, query, args...)
	if err != nil {
		return &QueryResult{
			Error:   fmt.Errorf("ストアドプロシージャ実行エラー: %w", err),
			Elapsed: time.Since(start),
			DBType:  "Oracle",
		}, nil
	}

	return &QueryResult{
		Elapsed: time.Since(start),
		DBType:  "Oracle",
	}, nil
}

// ExecuteFunction ファンション実行（Oracle特有）
func (o *OracleAdapter) ExecuteFunction(ctx context.Context, funcName string, returnType any, args ...any) (*QueryResult, error) {
	start := time.Now()

	o.mutex.RLock()
	defer o.mutex.RUnlock()

	if o.db == nil {
		return nil, fmt.Errorf("データベース未接続")
	}

	// Oracleファンクション呼び出し
	placeholders := make([]string, len(args))
	for i := range args {
		placeholders[i] = fmt.Sprintf(":p%d", i+2) // :p1は戻り値用
	}

	query := fmt.Sprintf("BEGIN :p1 := %s(%s); END;", funcName, strings.Join(placeholders, ", "))

	allArgs := append([]any{sql.Out{Dest: returnType}}, args...)
	_, err := o.db.ExecContext(ctx, query, allArgs...)
	if err != nil {
		return &QueryResult{
			Error:   fmt.Errorf("ファンクション実行エラー: %w", err),
			Elapsed: time.Since(start),
			DBType:  "Oracle",
		}, nil
	}

	// 結果をマップに変換
	result := map[string]any{
		"return_value": returnType,
	}

	return &QueryResult{
		Data:    []map[string]any{result},
		Elapsed: time.Since(start),
		DBType:  "Oracle",
	}, nil
}

// GetExplainPlan 実行計画取得（Oracle特有）
func (o *OracleAdapter) GetExplainPlan(ctx context.Context, query string, args ...any) (*QueryResult, error) {
	start := time.Now()

	// ユニークなステートメントID生成
	statementID := fmt.Sprintf("STMT_%d", time.Now().UnixNano())

	// EXPLAIN PLAN実行
	explainQuery := fmt.Sprintf("EXPLAIN PLAN SET STATEMENT_ID = '%s' FOR %s", statementID, query)
	if _, err := o.Execute(ctx, explainQuery, args...); err != nil {
		return &QueryResult{
			Error:   fmt.Errorf("EXPLAIN PLAN実行エラー: %w", err),
			Elapsed: time.Since(start),
			DBType:  "Oracle",
		}, nil
	}

	// 実行計画取得
	planQuery := `
		SELECT LPAD(' ', 2*LEVEL) || OPERATION || ' ' || OPTIONS || ' ' || OBJECT_NAME AS plan_line,
		       COST, CARDINALITY, BYTES
		FROM PLAN_TABLE
		WHERE STATEMENT_ID = :1
		CONNECT BY PRIOR ID = PARENT_ID AND STATEMENT_ID = :2
		START WITH ID = 0 AND STATEMENT_ID = :3
		ORDER BY ID
	`

	result, err := o.Query(ctx, planQuery, statementID, statementID, statementID)
	if err != nil {
		return &QueryResult{
			Error:   fmt.Errorf("実行計画取得エラー: %w", err),
			Elapsed: time.Since(start),
			DBType:  "Oracle",
		}, nil
	}

	// 実行計画をクリーンアップ
	cleanupQuery := "DELETE FROM PLAN_TABLE WHERE STATEMENT_ID = :1"
	if _, err := o.Execute(ctx, cleanupQuery, statementID); err != nil {
		log.Printf("PLAN_TABLE の後始末エラー: %v", err)
	}

	result.Elapsed = time.Since(start)
	return result, nil
}

// oracleInt64 Query の結果では NUMBER 列が文字列になるので、整数に直す。変換できない値は0にする
func oracleInt64(v any) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case string:
		n, _ := strconv.ParseInt(x, 10, 64)
		return n
	}
	return 0
}
