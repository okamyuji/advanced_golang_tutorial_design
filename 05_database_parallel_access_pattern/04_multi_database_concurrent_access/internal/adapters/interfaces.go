package adapters

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"multi-db/internal/config"
	"multi-db/internal/models"
)

// QueryResult クエリ結果構造体
type QueryResult struct {
	Data    []map[string]interface{} `json:"data"`
	Error   error                    `json:"error"`
	Elapsed time.Duration            `json:"elapsed"`
	DBType  string                   `json:"db_type"`
}

// DBAdapter データベースアダプターインターフェース
type DBAdapter interface {
	// 基本的なデータベース操作
	Connect(ctx context.Context, config config.DatabaseConfig) error
	Query(ctx context.Context, query string, args ...interface{}) (*QueryResult, error)
	Execute(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	Transaction(ctx context.Context, fn func(*sql.Tx) error) error
	Close() error
	Ping(ctx context.Context) error
	GetStats() sql.DBStats
	GetType() string

	// アプリケーション固有の操作
	CreateUser(ctx context.Context, user *models.User) (*models.User, error)
	GetUsersByAgeRange(ctx context.Context, minAge, maxAge int) ([]*models.User, error)
	BulkInsertUsers(ctx context.Context, users []*models.User) error
}

// AdapterRegistry アダプター登録管理
type AdapterRegistry struct {
	adapters map[string]func() DBAdapter
}

// NewAdapterRegistry 新しいアダプターレジストリを作成
func NewAdapterRegistry() *AdapterRegistry {
	return &AdapterRegistry{
		adapters: make(map[string]func() DBAdapter),
	}
}

// Register アダプターを登録
func (r *AdapterRegistry) Register(dbType string, factory func() DBAdapter) {
	r.adapters[dbType] = factory
}

// Create アダプターを作成
func (r *AdapterRegistry) Create(dbType string) (DBAdapter, error) {
	factory, exists := r.adapters[dbType]
	if !exists {
		return nil, fmt.Errorf("サポートされていないデータベースタイプ: %s", dbType)
	}
	return factory(), nil
}

// GetSupportedTypes サポートされているデータベースタイプを取得
func (r *AdapterRegistry) GetSupportedTypes() []string {
	var types []string
	for dbType := range r.adapters {
		types = append(types, dbType)
	}
	return types
}

// デフォルトレジストリのインスタンス
var DefaultRegistry = NewAdapterRegistry()

// RegisterDefaultAdapters デフォルトアダプターを登録
func RegisterDefaultAdapters() {
	DefaultRegistry.Register("PostgreSQL", func() DBAdapter {
		return NewPostgreSQLAdapter()
	})
	DefaultRegistry.Register("MySQL", func() DBAdapter {
		return NewMySQLAdapter()
	})
	DefaultRegistry.Register("MSSQL", func() DBAdapter {
		return NewMSSQLAdapter()
	})
	DefaultRegistry.Register("Oracle", func() DBAdapter {
		return NewOracleAdapter()
	})
}
