package models

import (
	"fmt"
	"time"
)

// User ユーザーモデル
type User struct {
	ID        int64     `json:"id" db:"id"`
	Name      string    `json:"name" db:"name" validate:"required,min=1,max=255"`
	Email     string    `json:"email" db:"email" validate:"required,email,max=255"`
	Age       int       `json:"age" db:"age" validate:"min=0,max=150"`
	CreatedAt time.Time `json:"created_at" db:"created_at"`
	UpdatedAt time.Time `json:"updated_at" db:"updated_at"`
}

// CreateUserRequest ユーザー作成リクエスト
type CreateUserRequest struct {
	Name  string `json:"name" validate:"required,min=1,max=255"`
	Email string `json:"email" validate:"required,email,max=255"`
	Age   int    `json:"age" validate:"min=0,max=150"`
}

// UpdateUserRequest ユーザー更新リクエスト
type UpdateUserRequest struct {
	ID    int64  `json:"id" validate:"required,min=1"`
	Name  string `json:"name" validate:"omitempty,min=1,max=255"`
	Email string `json:"email" validate:"omitempty,email,max=255"`
	Age   int    `json:"age" validate:"omitempty,min=0,max=150"`
}

// UserSearchRequest ユーザー検索リクエスト
type UserSearchRequest struct {
	MinAge   int    `json:"min_age" validate:"omitempty,min=0,max=150"`
	MaxAge   int    `json:"max_age" validate:"omitempty,min=0,max=150"`
	Name     string `json:"name" validate:"omitempty,max=255"`
	Email    string `json:"email" validate:"omitempty,email,max=255"`
	Limit    int    `json:"limit" validate:"omitempty,min=1,max=10000"`
	Offset   int    `json:"offset" validate:"omitempty,min=0"`
	OrderBy  string `json:"order_by" validate:"omitempty,oneof=id name email age created_at updated_at"`
	OrderDir string `json:"order_dir" validate:"omitempty,oneof=ASC DESC asc desc"`
}

// UserResponse ユーザーレスポンス
type UserResponse struct {
	User    *User  `json:"user,omitempty"`
	Message string `json:"message,omitempty"`
	Success bool   `json:"success"`
}

// UsersResponse 複数ユーザーレスポンス
type UsersResponse struct {
	Users   []*User `json:"users"`
	Total   int64   `json:"total"`
	Page    int     `json:"page"`
	PerPage int     `json:"per_page"`
	Message string  `json:"message,omitempty"`
	Success bool    `json:"success"`
}

// BulkOperationResult バッチ操作結果
type BulkOperationResult struct {
	TotalProcessed int      `json:"total_processed"`
	Successful     int      `json:"successful"`
	Failed         int      `json:"failed"`
	Errors         []string `json:"errors,omitempty"`
	Duration       string   `json:"duration"`
}

// PerformanceMetrics パフォーマンス指標
type PerformanceMetrics struct {
	DatabaseType       string        `json:"database_type"`
	OperationType      string        `json:"operation_type"`
	TotalOperations    int64         `json:"total_operations"`
	SuccessfulOps      int64         `json:"successful_operations"`
	FailedOps          int64         `json:"failed_operations"`
	AverageLatency     time.Duration `json:"average_latency"`
	MaxLatency         time.Duration `json:"max_latency"`
	MinLatency         time.Duration `json:"min_latency"`
	Throughput         float64       `json:"throughput"` // operations per second
	ErrorRate          float64       `json:"error_rate"` // percentage
	LastUpdated        time.Time     `json:"last_updated"`
	ConcurrentRequests int           `json:"concurrent_requests"`
}

// DatabaseStats データベース統計情報
type DatabaseStats struct {
	MaxOpenConnections int           `json:"max_open_connections"`
	OpenConnections    int           `json:"open_connections"`
	InUse              int           `json:"in_use"`
	Idle               int           `json:"idle"`
	WaitCount          int64         `json:"wait_count"`
	WaitDuration       time.Duration `json:"wait_duration"`
	MaxIdleClosed      int64         `json:"max_idle_closed"`
	MaxIdleTimeClosed  int64         `json:"max_idle_time_closed"`
	MaxLifetimeClosed  int64         `json:"max_lifetime_closed"`
}

// HealthStatus ヘルスステータス
type HealthStatus struct {
	Service   string            `json:"service"`
	Status    string            `json:"status"` // "healthy", "unhealthy", "degraded"
	Timestamp time.Time         `json:"timestamp"`
	Databases map[string]string `json:"databases"`
	Uptime    time.Duration     `json:"uptime"`
	Version   string            `json:"version"`
	Details   map[string]string `json:"details,omitempty"`
}

// Validate ユーザーモデルの基本バリデーション
func (u *User) Validate() error {
	if u.Name == "" {
		return fmt.Errorf("名前は必須です")
	}
	if len(u.Name) > 255 {
		return fmt.Errorf("名前は255文字以内で入力してください")
	}
	if u.Email == "" {
		return fmt.Errorf("メールアドレスは必須です")
	}
	if len(u.Email) > 255 {
		return fmt.Errorf("メールアドレスは255文字以内で入力してください")
	}
	if u.Age < 0 || u.Age > 150 {
		return fmt.Errorf("年齢は0〜150の範囲で入力してください")
	}
	return nil
}

// ToUser CreateUserRequestからUserへの変換
func (req *CreateUserRequest) ToUser() *User {
	return &User{
		Name:      req.Name,
		Email:     req.Email,
		Age:       req.Age,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
}

// ApplyUpdates UpdateUserRequestをUserに適用
func (req *UpdateUserRequest) ApplyUpdates(user *User) {
	if req.Name != "" {
		user.Name = req.Name
	}
	if req.Email != "" {
		user.Email = req.Email
	}
	if req.Age > 0 {
		user.Age = req.Age
	}
	user.UpdatedAt = time.Now()
}

// SetDefaults UserSearchRequestのデフォルト値設定
func (req *UserSearchRequest) SetDefaults() {
	if req.Limit <= 0 {
		req.Limit = 50
	}
	if req.Limit > 10000 {
		req.Limit = 10000
	}
	if req.Offset < 0 {
		req.Offset = 0
	}
	if req.OrderBy == "" {
		req.OrderBy = "created_at"
	}
	if req.OrderDir == "" {
		req.OrderDir = "DESC"
	}
}
