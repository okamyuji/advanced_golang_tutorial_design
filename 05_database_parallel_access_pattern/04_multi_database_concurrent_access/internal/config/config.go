package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config アプリケーション設定
type Config struct {
	Server      ServerConfig      `yaml:"server"`
	Databases   DatabasesConfig   `yaml:"databases"`
	WorkerPool  WorkerPoolConfig  `yaml:"worker_pool"`
	Monitoring  MonitoringConfig  `yaml:"monitoring"`
	Logging     LoggingConfig     `yaml:"logging"`
	Security    SecurityConfig    `yaml:"security"`
	Performance PerformanceConfig `yaml:"performance"`
	Development DevelopmentConfig `yaml:"development"`
	LoadTesting LoadTestingConfig `yaml:"load_testing"`
	Alerts      AlertsConfig      `yaml:"alerts"`
	Backup      BackupConfig      `yaml:"backup"`
}

// ServerConfig サーバー設定
type ServerConfig struct {
	Port         int           `yaml:"port"`
	Host         string        `yaml:"host"`
	ReadTimeout  time.Duration `yaml:"read_timeout"`
	WriteTimeout time.Duration `yaml:"write_timeout"`
	IdleTimeout  time.Duration `yaml:"idle_timeout"`
}

// DatabasesConfig データベース設定群
type DatabasesConfig struct {
	PostgreSQL DatabaseInstanceConfig `yaml:"postgresql"`
	MySQL      DatabaseInstanceConfig `yaml:"mysql"`
	MSSQL      DatabaseInstanceConfig `yaml:"mssql"`
	Oracle     DatabaseInstanceConfig `yaml:"oracle"`
}

// DatabaseInstanceConfig データベースインスタンス設定
type DatabaseInstanceConfig struct {
	Enabled        bool           `yaml:"enabled"`
	DatabaseConfig DatabaseConfig `yaml:",inline"`
}

// DatabaseConfig データベース接続設定
type DatabaseConfig struct {
	Host            string        `yaml:"host"`
	Port            int           `yaml:"port"`
	Username        string        `yaml:"username"`
	Password        string        `yaml:"password"`
	Database        string        `yaml:"database"`
	MaxOpenConns    int           `yaml:"max_open_conns"`
	MaxIdleConns    int           `yaml:"max_idle_conns"`
	ConnMaxLifetime time.Duration `yaml:"conn_max_lifetime"`
	ConnMaxIdleTime time.Duration `yaml:"conn_max_idle_time"`
	RetryAttempts   int           `yaml:"retry_attempts"`
	RetryDelay      time.Duration `yaml:"retry_delay"`
}

// WorkerPoolConfig ワーカープール設定
type WorkerPoolConfig struct {
	MaxWorkers      int           `yaml:"max_workers"`
	QueueSize       int           `yaml:"queue_size"`
	TimeoutDuration time.Duration `yaml:"timeout_duration"`
}

// MonitoringConfig 監視設定
type MonitoringConfig struct {
	Enabled             bool             `yaml:"enabled"`
	MetricsPort         int              `yaml:"metrics_port"`
	HealthCheckInterval time.Duration    `yaml:"health_check_interval"`
	Prometheus          PrometheusConfig `yaml:"prometheus"`
	Grafana             GrafanaConfig    `yaml:"grafana"`
}

// PrometheusConfig Prometheus設定
type PrometheusConfig struct {
	Enabled   bool   `yaml:"enabled"`
	Namespace string `yaml:"namespace"`
	Subsystem string `yaml:"subsystem"`
}

// GrafanaConfig Grafana設定
type GrafanaConfig struct {
	Enabled          bool          `yaml:"enabled"`
	DashboardRefresh time.Duration `yaml:"dashboard_refresh"`
}

// LoggingConfig ログ設定
type LoggingConfig struct {
	Level      string `yaml:"level"`
	Format     string `yaml:"format"`
	Output     string `yaml:"output"`
	FilePath   string `yaml:"file_path"`
	MaxSize    string `yaml:"max_size"`
	MaxBackups int    `yaml:"max_backups"`
	MaxAge     int    `yaml:"max_age"`
	Compress   bool   `yaml:"compress"`
}

// SecurityConfig セキュリティ設定
type SecurityConfig struct {
	CORS         CORSConfig         `yaml:"cors"`
	RateLimiting RateLimitingConfig `yaml:"rate_limiting"`
	APIKey       APIKeyConfig       `yaml:"api_key"`
}

// CORSConfig CORS設定
type CORSConfig struct {
	AllowedOrigins   []string `yaml:"allowed_origins"`
	AllowedMethods   []string `yaml:"allowed_methods"`
	AllowedHeaders   []string `yaml:"allowed_headers"`
	ExposedHeaders   []string `yaml:"expose_headers"`
	AllowCredentials bool     `yaml:"allow_credentials"`
	MaxAge           int      `yaml:"max_age"`
}

// RateLimitingConfig レート制限設定
type RateLimitingConfig struct {
	Enabled           bool `yaml:"enabled"`
	RequestsPerMinute int  `yaml:"requests_per_minute"`
	Burst             int  `yaml:"burst"`
}

// APIKeyConfig APIキー設定
type APIKeyConfig struct {
	Enabled    bool     `yaml:"enabled"`
	HeaderName string   `yaml:"header_name"`
	Keys       []string `yaml:"keys"`
}

// PerformanceConfig パフォーマンス設定
type PerformanceConfig struct {
	EnableQueryCache   bool                    `yaml:"enable_query_cache"`
	CacheTTL           time.Duration           `yaml:"cache_ttl"`
	MaxCacheEntries    int                     `yaml:"max_cache_entries"`
	ConnectionPooling  ConnectionPoolingConfig `yaml:"connection_pooling"`
	BatchProcessing    BatchProcessingConfig   `yaml:"batch_processing"`
	QueryTimeout       time.Duration           `yaml:"query_timeout"`
	TransactionTimeout time.Duration           `yaml:"transaction_timeout"`
}

// ConnectionPoolingConfig 接続プーリング設定
type ConnectionPoolingConfig struct {
	EnableLoadBalancing bool          `yaml:"enable_load_balancing"`
	HealthCheckInterval time.Duration `yaml:"health_check_interval"`
}

// BatchProcessingConfig バッチ処理設定
type BatchProcessingConfig struct {
	MaxBatchSize int           `yaml:"max_batch_size"`
	BatchTimeout time.Duration `yaml:"batch_timeout"`
}

// DevelopmentConfig 開発設定
type DevelopmentConfig struct {
	Debug           bool                `yaml:"debug"`
	EnableProfiling bool                `yaml:"enable_profiling"`
	ProfilingPort   int                 `yaml:"profiling_port"`
	HotReload       bool                `yaml:"hot_reload"`
	MockDatabases   MockDatabasesConfig `yaml:"mock_databases"`
}

// MockDatabasesConfig モックデータベース設定
type MockDatabasesConfig struct {
	Enabled       bool          `yaml:"enabled"`
	ResponseDelay time.Duration `yaml:"response_delay"`
	ErrorRate     float64       `yaml:"error_rate"`
}

// LoadTestingConfig 負荷テスト設定
type LoadTestingConfig struct {
	DefaultConfig LoadTestScenario   `yaml:"default_config"`
	Scenarios     []LoadTestScenario `yaml:"scenarios"`
}

// LoadTestScenario 負荷テストシナリオ
type LoadTestScenario struct {
	Name              string        `yaml:"name"`
	ConcurrentClients int           `yaml:"concurrent_clients"`
	RequestsPerClient int           `yaml:"requests_per_client"`
	TestDuration      time.Duration `yaml:"test_duration"`
	RampUpDuration    time.Duration `yaml:"ramp_up_duration"`
}

// AlertsConfig アラート設定
type AlertsConfig struct {
	Enabled    bool               `yaml:"enabled"`
	Thresholds ThresholdsConfig   `yaml:"thresholds"`
	Channels   AlertChannelConfig `yaml:"channels"`
}

// ThresholdsConfig 閾値設定
type ThresholdsConfig struct {
	ErrorRate       float64       `yaml:"error_rate"`
	ResponseTimeP95 time.Duration `yaml:"response_time_p95"`
	ConnectionUsage float64       `yaml:"connection_usage"`
	MemoryUsage     float64       `yaml:"memory_usage"`
	CPUUsage        float64       `yaml:"cpu_usage"`
}

// AlertChannelConfig アラートチャネル設定
type AlertChannelConfig struct {
	Slack   SlackConfig   `yaml:"slack"`
	Email   EmailConfig   `yaml:"email"`
	Webhook WebhookConfig `yaml:"webhook"`
}

// SlackConfig Slack設定
type SlackConfig struct {
	Enabled    bool   `yaml:"enabled"`
	WebhookURL string `yaml:"webhook_url"`
	Channel    string `yaml:"channel"`
}

// EmailConfig Email設定
type EmailConfig struct {
	Enabled    bool     `yaml:"enabled"`
	SMTPHost   string   `yaml:"smtp_host"`
	SMTPPort   int      `yaml:"smtp_port"`
	Username   string   `yaml:"username"`
	Password   string   `yaml:"password"`
	Recipients []string `yaml:"recipients"`
}

// WebhookConfig Webhook設定
type WebhookConfig struct {
	Enabled bool          `yaml:"enabled"`
	URL     string        `yaml:"url"`
	Timeout time.Duration `yaml:"timeout"`
}

// BackupConfig バックアップ設定
type BackupConfig struct {
	Enabled       bool               `yaml:"enabled"`
	Schedule      string             `yaml:"schedule"`
	RetentionDays int                `yaml:"retention_days"`
	Destinations  BackupDestinations `yaml:"destinations"`
	Compression   bool               `yaml:"compression"`
	Encryption    bool               `yaml:"encryption"`
}

// BackupDestinations バックアップ保存先設定
type BackupDestinations struct {
	S3    S3Config    `yaml:"s3"`
	Local LocalConfig `yaml:"local"`
}

// S3Config S3設定
type S3Config struct {
	Enabled   bool   `yaml:"enabled"`
	Bucket    string `yaml:"bucket"`
	Region    string `yaml:"region"`
	AccessKey string `yaml:"access_key"`
	SecretKey string `yaml:"secret_key"`
}

// LocalConfig ローカル設定
type LocalConfig struct {
	Enabled bool   `yaml:"enabled"`
	Path    string `yaml:"path"`
}

// LoadConfig 設定ファイルを読み込む
func LoadConfig(configPath string) (*Config, error) {
	// 設定ファイルを読み込み
	configData, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("設定ファイル読み込みエラー: %w", err)
	}

	// 環境変数を展開
	expandedConfig := expandEnvVars(string(configData))

	var config Config
	if err := yaml.Unmarshal([]byte(expandedConfig), &config); err != nil {
		return nil, fmt.Errorf("設定ファイル解析エラー: %w", err)
	}

	// バリデーション
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("設定バリデーションエラー: %w", err)
	}

	return &config, nil
}

// expandEnvVars 環境変数を展開する
func expandEnvVars(config string) string {
	// ${VAR_NAME:default_value} 形式の環境変数を展開
	return os.Expand(config, func(key string) string {
		// デフォルト値の処理
		parts := strings.SplitN(key, ":", 2)
		envKey := parts[0]

		value := os.Getenv(envKey)
		if value == "" && len(parts) == 2 {
			value = parts[1]
		}

		return value
	})
}

// Validate 設定の妥当性を検証
func (c *Config) Validate() error {
	// サーバー設定検証
	if c.Server.Port <= 0 || c.Server.Port > 65535 {
		return fmt.Errorf("無効なサーバーポート: %d", c.Server.Port)
	}

	// データベース設定検証
	enabledDBs := 0
	if c.Databases.PostgreSQL.Enabled {
		enabledDBs++
		if err := c.Databases.PostgreSQL.DatabaseConfig.Validate(); err != nil {
			return fmt.Errorf("PostgreSQL設定エラー: %w", err)
		}
	}
	if c.Databases.MySQL.Enabled {
		enabledDBs++
		if err := c.Databases.MySQL.DatabaseConfig.Validate(); err != nil {
			return fmt.Errorf("MySQL設定エラー: %w", err)
		}
	}
	if c.Databases.MSSQL.Enabled {
		enabledDBs++
		if err := c.Databases.MSSQL.DatabaseConfig.Validate(); err != nil {
			return fmt.Errorf("MSSQL設定エラー: %w", err)
		}
	}
	if c.Databases.Oracle.Enabled {
		enabledDBs++
		if err := c.Databases.Oracle.DatabaseConfig.Validate(); err != nil {
			return fmt.Errorf("Oracle設定エラー: %w", err)
		}
	}

	if enabledDBs == 0 {
		return fmt.Errorf("少なくとも1つのデータベースを有効にする必要があります")
	}

	// ワーカープール設定検証
	if c.WorkerPool.MaxWorkers < 0 {
		return fmt.Errorf("無効なワーカー数: %d", c.WorkerPool.MaxWorkers)
	}
	if c.WorkerPool.QueueSize <= 0 {
		return fmt.Errorf("無効なキューサイズ: %d", c.WorkerPool.QueueSize)
	}

	return nil
}

// Validate データベース設定の妥当性を検証
func (d *DatabaseConfig) Validate() error {
	if d.Host == "" {
		return fmt.Errorf("ホストが設定されていません")
	}
	if d.Port <= 0 || d.Port > 65535 {
		return fmt.Errorf("無効なポート: %d", d.Port)
	}
	if d.Username == "" {
		return fmt.Errorf("ユーザー名が設定されていません")
	}
	if d.Database == "" {
		return fmt.Errorf("データベース名が設定されていません")
	}
	if d.MaxOpenConns <= 0 {
		return fmt.Errorf("無効な最大接続数: %d", d.MaxOpenConns)
	}
	if d.MaxIdleConns < 0 || d.MaxIdleConns > d.MaxOpenConns {
		return fmt.Errorf("無効なアイドル接続数: %d", d.MaxIdleConns)
	}

	return nil
}

// GetDSN データベース固有のDSN文字列を取得
func (d *DatabaseConfig) GetDSN(dbType string) string {
	switch strings.ToLower(dbType) {
	case "postgresql", "postgres":
		return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
			d.Host, d.Port, d.Username, d.Password, d.Database)
	case "mysql":
		return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local",
			d.Username, d.Password, d.Host, d.Port, d.Database)
	case "mssql", "sqlserver":
		return fmt.Sprintf("server=%s;port=%d;database=%s;user id=%s;password=%s;encrypt=disable",
			d.Host, d.Port, d.Database, d.Username, d.Password)
	case "oracle":
		return fmt.Sprintf("oracle://%s:%s@%s:%d/%s",
			d.Username, d.Password, d.Host, d.Port, d.Database)
	default:
		return ""
	}
}

// LoadConfigFromEnv 環境変数から設定を読み込む
func LoadConfigFromEnv() (*Config, error) {
	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		configPath = "configs/config.yaml"
	}

	return LoadConfig(configPath)
}

// GetEnvironment 現在の環境を取得
func GetEnvironment() string {
	env := os.Getenv("APP_ENV")
	if env == "" {
		env = "development"
	}
	return env
}

// IsDevelopment 開発環境かどうか
func IsDevelopment() bool {
	return GetEnvironment() == "development"
}

// IsProduction 本番環境かどうか
func IsProduction() bool {
	return GetEnvironment() == "production"
}
