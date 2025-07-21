// 復習問題3: ゼロダウンタイム設定更新システム
// 問題: サービス停止なしで設定変更を行うホットリロードシステムを実装してください：
// - 設定ファイル監視とリアルタイム更新
// - 設定変更の段階的適用（カナリアリリース）
// - 設定変更失敗時の自動ロールバック
// - 変更履歴の記録と監査機能

package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// ZeroDowntimeConfigSystem ゼロダウンタイム設定更新システム
type ZeroDowntimeConfigSystem struct {
	// 設定管理
	configManager     *HotReloadConfigManager
	versionController *ConfigVersionController
	rolloutManager    *CanaryRolloutManager
	auditLogger       *ConfigAuditLogger

	// 監視・検証
	healthChecker   *ConfigHealthChecker
	validator       *ConfigValidator
	rollbackManager *AutoRollbackManager

	// システム状態
	isRunning        bool
	startTime        time.Time
	lastConfigUpdate time.Time
	updateCount      int64

	// 制御チャンネル
	stopChan   chan struct{}
	updateChan chan *ConfigUpdateEvent
	wg         sync.WaitGroup

	// HTTP API
	httpServer *http.Server
}

// HotReloadConfigManager ホットリロード設定管理器
type HotReloadConfigManager struct {
	configFile      string
	backupDir       string
	currentConfig   *ApplicationConfig
	pendingConfig   *ApplicationConfig
	watchers        map[string]*FileWatcher
	mutex           sync.RWMutex
	reloadCallbacks []ConfigReloadCallback
}

// ApplicationConfig アプリケーション設定
type ApplicationConfig struct {
	Version        string                 `json:"version"`
	Timestamp      time.Time              `json:"timestamp"`
	Checksum       string                 `json:"checksum"`
	Server         *ServerConfig          `json:"server"`
	Database       *DatabaseConfig        `json:"database"`
	Cache          *CacheConfig           `json:"cache"`
	Logging        *LoggingConfig         `json:"logging"`
	Features       map[string]bool        `json:"features"`
	CustomSettings map[string]interface{} `json:"custom_settings"`
}

// ServerConfig サーバー設定
type ServerConfig struct {
	Port           int           `json:"port"`
	ReadTimeout    time.Duration `json:"read_timeout"`
	WriteTimeout   time.Duration `json:"write_timeout"`
	MaxConnections int           `json:"max_connections"`
	EnableTLS      bool          `json:"enable_tls"`
	TLSCertFile    string        `json:"tls_cert_file"`
	TLSKeyFile     string        `json:"tls_key_file"`
}

// DatabaseConfig データベース設定
type DatabaseConfig struct {
	Host           string        `json:"host"`
	Port           int           `json:"port"`
	Database       string        `json:"database"`
	Username       string        `json:"username"`
	Password       string        `json:"password"`
	MaxConnections int           `json:"max_connections"`
	ConnectTimeout time.Duration `json:"connect_timeout"`
	QueryTimeout   time.Duration `json:"query_timeout"`
	SSLMode        string        `json:"ssl_mode"`
}

// CacheConfig キャッシュ設定
type CacheConfig struct {
	Type            string        `json:"type"`
	Host            string        `json:"host"`
	Port            int           `json:"port"`
	TTL             time.Duration `json:"ttl"`
	MaxSize         int64         `json:"max_size"`
	CleanupInterval time.Duration `json:"cleanup_interval"`
}

// LoggingConfig ログ設定
type LoggingConfig struct {
	Level      string `json:"level"`
	Format     string `json:"format"`
	OutputFile string `json:"output_file"`
	MaxSize    int64  `json:"max_size"`
	MaxBackups int    `json:"max_backups"`
	Compress   bool   `json:"compress"`
}

// ConfigVersion 設定バージョン
type ConfigVersion struct {
	Version       string             `json:"version"`
	Timestamp     time.Time          `json:"timestamp"`
	Config        *ApplicationConfig `json:"config"`
	ChangeReason  string             `json:"change_reason"`
	ChangedBy     string             `json:"changed_by"`
	RolloutStatus string             `json:"rollout_status"`
	Checksum      string             `json:"checksum"`
	BackupPath    string             `json:"backup_path"`
}

// ConfigVersionController 設定バージョン制御器
type ConfigVersionController struct {
	versions       []*ConfigVersion
	currentVersion string
	maxVersions    int
	versionCounter int64
	mutex          sync.RWMutex
}

// CanaryRolloutManager カナリアリリース管理器
type CanaryRolloutManager struct {
	rolloutStrategy *RolloutStrategy
	currentRollout  *RolloutExecution
	trafficSplitter *TrafficSplitter
	progressMonitor *RolloutProgressMonitor
	mutex           sync.RWMutex
}

// RolloutStrategy ロールアウト戦略
type RolloutStrategy struct {
	Name                string             `json:"name"`
	Stages              []*RolloutStage    `json:"stages"`
	ValidationRules     []*ValidationRule  `json:"validation_rules"`
	RollbackTriggers    []*RollbackTrigger `json:"rollback_triggers"`
	MaxDuration         time.Duration      `json:"max_duration"`
	HealthCheckInterval time.Duration      `json:"health_check_interval"`
}

// RolloutStage ロールアウトステージ
type RolloutStage struct {
	Name            string           `json:"name"`
	TrafficPercent  int              `json:"traffic_percent"`
	Duration        time.Duration    `json:"duration"`
	SuccessCriteria *SuccessCriteria `json:"success_criteria"`
	AutoPromote     bool             `json:"auto_promote"`
}

// SuccessCriteria 成功基準
type SuccessCriteria struct {
	MinSuccessRate  float64       `json:"min_success_rate"`
	MaxErrorRate    float64       `json:"max_error_rate"`
	MaxResponseTime time.Duration `json:"max_response_time"`
	MinThroughput   float64       `json:"min_throughput"`
}

// ValidationRule 検証ルール
type ValidationRule struct {
	Name       string                 `json:"name"`
	Type       string                 `json:"type"`
	Parameters map[string]interface{} `json:"parameters"`
	Required   bool                   `json:"required"`
	ErrorLevel string                 `json:"error_level"`
}

// RollbackTrigger ロールバックトリガー
type RollbackTrigger struct {
	Name      string        `json:"name"`
	Type      string        `json:"type"`
	Threshold float64       `json:"threshold"`
	Duration  time.Duration `json:"duration"`
	Enabled   bool          `json:"enabled"`
	Actions   []string      `json:"actions"`
}

// RolloutExecution ロールアウト実行
type RolloutExecution struct {
	ID              string           `json:"id"`
	Strategy        *RolloutStrategy `json:"strategy"`
	StartTime       time.Time        `json:"start_time"`
	CurrentStage    int              `json:"current_stage"`
	Status          string           `json:"status"`
	Metrics         *RolloutMetrics  `json:"metrics"`
	ErrorCount      int64            `json:"error_count"`
	LastHealthCheck time.Time        `json:"last_health_check"`
}

// RolloutMetrics ロールアウトメトリクス
type RolloutMetrics struct {
	TotalRequests       int64         `json:"total_requests"`
	SuccessfulRequests  int64         `json:"successful_requests"`
	FailedRequests      int64         `json:"failed_requests"`
	AverageResponseTime time.Duration `json:"average_response_time"`
	ThroughputRPS       float64       `json:"throughput_rps"`
	ErrorRate           float64       `json:"error_rate"`
	LastUpdated         time.Time     `json:"last_updated"`
}

// TrafficSplitter トラフィック分散器
type TrafficSplitter struct {
	currentPercent int
	targetPercent  int
	isActive       bool
	requestCounter int64
	mutex          sync.RWMutex
}

// RolloutProgressMonitor ロールアウト進捗監視器
type RolloutProgressMonitor struct {
	checkInterval time.Duration
	metrics       *RolloutMetrics
}

// ConfigAuditLogger 設定監査ログ記録器
type ConfigAuditLogger struct {
	logFile      string
	auditEntries []*AuditEntry
	maxEntries   int
	mutex        sync.RWMutex
}

// AuditEntry 監査エントリ
type AuditEntry struct {
	ID            string                 `json:"id"`
	Timestamp     time.Time              `json:"timestamp"`
	Action        string                 `json:"action"`
	ConfigVersion string                 `json:"config_version"`
	ChangedBy     string                 `json:"changed_by"`
	ChangeReason  string                 `json:"change_reason"`
	Changes       map[string]interface{} `json:"changes"`
	Result        string                 `json:"result"`
	ErrorMessage  string                 `json:"error_message,omitempty"`
	RolloutID     string                 `json:"rollout_id,omitempty"`
}

// ConfigHealthChecker 設定ヘルスチェッカー
type ConfigHealthChecker struct {
	checks           map[string]HealthCheck
	lastCheckResults map[string]*HealthCheckResult
	checkInterval    time.Duration
	timeout          time.Duration
	mutex            sync.RWMutex
}

// HealthCheck ヘルスチェック関数
type HealthCheck func(config *ApplicationConfig) error

// HealthCheckResult ヘルスチェック結果
type HealthCheckResult struct {
	Name      string        `json:"name"`
	Status    string        `json:"status"`
	Error     string        `json:"error,omitempty"`
	Duration  time.Duration `json:"duration"`
	Timestamp time.Time     `json:"timestamp"`
}

// ConfigValidator 設定検証器
type ConfigValidator struct {
	validationRules  []*ValidationRule
	customValidators map[string]ValidatorFunc
}

// ValidatorFunc 検証関数
type ValidatorFunc func(config *ApplicationConfig, params map[string]interface{}) error

// AutoRollbackManager 自動ロールバック管理器
type AutoRollbackManager struct {
	triggers        []*RollbackTrigger
	isEnabled       bool
	rollbackHistory []*RollbackEvent
	checkInterval   time.Duration
	mutex           sync.RWMutex
}

// RollbackEvent ロールバックイベント
type RollbackEvent struct {
	ID           string                 `json:"id"`
	Timestamp    time.Time              `json:"timestamp"`
	Trigger      string                 `json:"trigger"`
	FromVersion  string                 `json:"from_version"`
	ToVersion    string                 `json:"to_version"`
	Reason       string                 `json:"reason"`
	Automatic    bool                   `json:"automatic"`
	Success      bool                   `json:"success"`
	ErrorMessage string                 `json:"error_message,omitempty"`
	Metadata     map[string]interface{} `json:"metadata"`
}

// FileWatcher ファイル監視器
type FileWatcher struct {
	filePath     string
	lastModTime  time.Time
	lastSize     int64
	lastChecksum string
	callback     func(string)
	interval     time.Duration
	stopChan     chan struct{}
}

// ConfigUpdateEvent 設定更新イベント
type ConfigUpdateEvent struct {
	Type      string             `json:"type"`
	Timestamp time.Time          `json:"timestamp"`
	OldConfig *ApplicationConfig `json:"old_config"`
	NewConfig *ApplicationConfig `json:"new_config"`
	Source    string             `json:"source"`
	UserID    string             `json:"user_id"`
	Reason    string             `json:"reason"`
}

// ConfigReloadCallback 設定リロードコールバック
type ConfigReloadCallback func(*ApplicationConfig) error

// NewZeroDowntimeConfigSystem 新しいゼロダウンタイム設定システムを作成
func NewZeroDowntimeConfigSystem(configFile string) *ZeroDowntimeConfigSystem {
	system := &ZeroDowntimeConfigSystem{
		configManager: &HotReloadConfigManager{
			configFile:      configFile,
			backupDir:       "config_backups",
			watchers:        make(map[string]*FileWatcher),
			reloadCallbacks: make([]ConfigReloadCallback, 0),
		},
		versionController: &ConfigVersionController{
			versions:    make([]*ConfigVersion, 0),
			maxVersions: 50,
		},
		rolloutManager: &CanaryRolloutManager{
			rolloutStrategy: &RolloutStrategy{
				Name: "default_strategy",
				Stages: []*RolloutStage{
					{Name: "canary", TrafficPercent: 10, Duration: 5 * time.Minute, AutoPromote: false},
					{Name: "beta", TrafficPercent: 50, Duration: 10 * time.Minute, AutoPromote: false},
					{Name: "production", TrafficPercent: 100, Duration: 0, AutoPromote: true},
				},
				MaxDuration:         30 * time.Minute,
				HealthCheckInterval: 30 * time.Second,
			},
			trafficSplitter: &TrafficSplitter{},
			progressMonitor: &RolloutProgressMonitor{
				checkInterval: 30 * time.Second,
				metrics:       &RolloutMetrics{},
			},
		},
		auditLogger: &ConfigAuditLogger{
			logFile:      "config_audit.log",
			auditEntries: make([]*AuditEntry, 0),
			maxEntries:   10000,
		},
		healthChecker: &ConfigHealthChecker{
			checks:           make(map[string]HealthCheck),
			lastCheckResults: make(map[string]*HealthCheckResult),
			checkInterval:    1 * time.Minute,
			timeout:          30 * time.Second,
		},
		validator: &ConfigValidator{
			customValidators: make(map[string]ValidatorFunc),
		},
		rollbackManager: &AutoRollbackManager{
			isEnabled:       true,
			checkInterval:   30 * time.Second,
			rollbackHistory: make([]*RollbackEvent, 0),
		},
		stopChan:   make(chan struct{}),
		updateChan: make(chan *ConfigUpdateEvent, 100),
	}

	// HTTPサーバー設定
	mux := http.NewServeMux()
	system.httpServer = &http.Server{
		Addr:    ":9999",
		Handler: mux,
	}

	// API エンドポイント設定
	mux.HandleFunc("/api/config", system.configAPIHandler)
	mux.HandleFunc("/api/config/reload", system.reloadAPIHandler)
	mux.HandleFunc("/api/config/rollback", system.rollbackAPIHandler)
	mux.HandleFunc("/api/config/versions", system.versionsAPIHandler)
	mux.HandleFunc("/api/config/audit", system.auditAPIHandler)
	mux.HandleFunc("/api/config/health", system.healthAPIHandler)
	mux.HandleFunc("/api/rollout/status", system.rolloutStatusAPIHandler)

	// デフォルトのヘルスチェック追加
	system.addDefaultHealthChecks()

	// デフォルトの検証ルール追加
	system.addDefaultValidationRules()

	// デフォルトのロールバックトリガー追加
	system.addDefaultRollbackTriggers()

	return system
}

// Start システム開始
func (zcs *ZeroDowntimeConfigSystem) Start(ctx context.Context) error {
	zcs.isRunning = true
	zcs.startTime = time.Now()

	fmt.Println("ゼロダウンタイム設定更新システム開始")

	// バックアップディレクトリ作成
	if err := os.MkdirAll(zcs.configManager.backupDir, 0755); err != nil {
		return fmt.Errorf("バックアップディレクトリ作成エラー: %w", err)
	}

	// 初期設定読み込み
	if err := zcs.loadInitialConfig(); err != nil {
		return fmt.Errorf("初期設定読み込みエラー: %w", err)
	}

	// ファイル監視開始
	if err := zcs.startFileWatching(ctx); err != nil {
		return fmt.Errorf("ファイル監視開始エラー: %w", err)
	}

	// 設定更新処理開始
	zcs.wg.Add(1)
	go zcs.configUpdateLoop(ctx)

	// ヘルスチェック開始
	zcs.wg.Add(1)
	go zcs.healthCheckLoop(ctx)

	// ロールアウト監視開始
	zcs.wg.Add(1)
	go zcs.rolloutMonitoringLoop(ctx)

	// 自動ロールバック監視開始
	zcs.wg.Add(1)
	go zcs.autoRollbackLoop(ctx)

	// HTTPサーバー開始
	go func() {
		fmt.Printf("設定管理APIサーバー開始: %s\n", zcs.httpServer.Addr)
		if err := zcs.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("APIサーバーエラー: %v\n", err)
		}
	}()

	return nil
}

// Stop システム停止
func (zcs *ZeroDowntimeConfigSystem) Stop() error {
	fmt.Println("ゼロダウンタイム設定更新システム停止中...")

	zcs.isRunning = false
	close(zcs.stopChan)

	// HTTPサーバー停止
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := zcs.httpServer.Shutdown(ctx); err != nil {
		fmt.Printf("HTTPサーバー停止エラー: %v\n", err)
	}

	// ファイル監視停止
	zcs.stopFileWatching()

	// 全ゴルーチン停止待機
	zcs.wg.Wait()

	fmt.Println("ゼロダウンタイム設定更新システム停止完了")
	return nil
}

// loadInitialConfig 初期設定読み込み
func (zcs *ZeroDowntimeConfigSystem) loadInitialConfig() error {
	configData, err := os.ReadFile(zcs.configManager.configFile)
	if err != nil {
		// 設定ファイルが存在しない場合はデフォルト設定を作成
		if os.IsNotExist(err) {
			return zcs.createDefaultConfig()
		}
		return err
	}

	var config ApplicationConfig
	if err := json.Unmarshal(configData, &config); err != nil {
		return fmt.Errorf("設定ファイル解析エラー: %w", err)
	}

	// チェックサム計算
	config.Checksum = zcs.calculateChecksum(configData)
	config.Timestamp = time.Now()

	// 設定検証
	if err := zcs.validateConfig(&config); err != nil {
		return fmt.Errorf("設定検証エラー: %w", err)
	}

	zcs.configManager.mutex.Lock()
	zcs.configManager.currentConfig = &config
	zcs.configManager.mutex.Unlock()

	// バージョン記録
	zcs.versionController.addVersion(&config, "initial_load", "system")

	// 監査ログ記録
	zcs.auditLogger.logAction("initial_load", &config, "system", "システム起動時の初期設定読み込み", "success", "")

	fmt.Printf("初期設定読み込み完了: バージョン %s\n", config.Version)
	return nil
}

// createDefaultConfig デフォルト設定作成
func (zcs *ZeroDowntimeConfigSystem) createDefaultConfig() error {
	defaultConfig := &ApplicationConfig{
		Version:   "1.0.0",
		Timestamp: time.Now(),
		Server: &ServerConfig{
			Port:           8080,
			ReadTimeout:    30 * time.Second,
			WriteTimeout:   30 * time.Second,
			MaxConnections: 1000,
			EnableTLS:      false,
		},
		Database: &DatabaseConfig{
			Host:           "localhost",
			Port:           5432,
			Database:       "app_db",
			Username:       "app_user",
			MaxConnections: 20,
			ConnectTimeout: 10 * time.Second,
			QueryTimeout:   30 * time.Second,
			SSLMode:        "disable",
		},
		Cache: &CacheConfig{
			Type:            "redis",
			Host:            "localhost",
			Port:            6379,
			TTL:             1 * time.Hour,
			MaxSize:         100000,
			CleanupInterval: 10 * time.Minute,
		},
		Logging: &LoggingConfig{
			Level:      "info",
			Format:     "json",
			OutputFile: "app.log",
			MaxSize:    100 * 1024 * 1024, // 100MB
			MaxBackups: 5,
			Compress:   true,
		},
		Features: map[string]bool{
			"feature_a": true,
			"feature_b": false,
			"feature_c": true,
		},
		CustomSettings: map[string]interface{}{
			"max_retry_count": 3,
			"batch_size":      1000,
			"enable_metrics":  true,
		},
	}

	configData, err := json.MarshalIndent(defaultConfig, "", "  ")
	if err != nil {
		return err
	}

	if err := os.WriteFile(zcs.configManager.configFile, configData, 0644); err != nil {
		return err
	}

	defaultConfig.Checksum = zcs.calculateChecksum(configData)

	zcs.configManager.mutex.Lock()
	zcs.configManager.currentConfig = defaultConfig
	zcs.configManager.mutex.Unlock()

	fmt.Println("デフォルト設定ファイルを作成しました")
	return nil
}

// calculateChecksum チェックサム計算
func (zcs *ZeroDowntimeConfigSystem) calculateChecksum(data []byte) string {
	hash := sha256.Sum256(data)
	return fmt.Sprintf("%x", hash)
}

// validateConfig 設定検証
func (zcs *ZeroDowntimeConfigSystem) validateConfig(config *ApplicationConfig) error {
	// 基本的な検証
	if config.Server.Port <= 0 || config.Server.Port > 65535 {
		return fmt.Errorf("無効なサーバーポート: %d", config.Server.Port)
	}

	if config.Database.Host == "" {
		return fmt.Errorf("データベースホストが指定されていません")
	}

	if config.Database.Port <= 0 || config.Database.Port > 65535 {
		return fmt.Errorf("無効なデータベースポート: %d", config.Database.Port)
	}

	if config.Cache.Host == "" {
		return fmt.Errorf("キャッシュホストが指定されていません")
	}

	// カスタム検証ルール実行
	for _, rule := range zcs.validator.validationRules {
		if validator, exists := zcs.validator.customValidators[rule.Type]; exists {
			if err := validator(config, rule.Parameters); err != nil {
				if rule.Required || rule.ErrorLevel == "error" {
					return fmt.Errorf("検証ルール '%s' エラー: %w", rule.Name, err)
				}
				fmt.Printf("検証警告 '%s': %v\n", rule.Name, err)
			}
		}
	}

	return nil
}

// startFileWatching ファイル監視開始
func (zcs *ZeroDowntimeConfigSystem) startFileWatching(ctx context.Context) error {
	watcher := &FileWatcher{
		filePath: zcs.configManager.configFile,
		interval: 1 * time.Second,
		stopChan: make(chan struct{}),
		callback: func(filePath string) {
			event := &ConfigUpdateEvent{
				Type:      "file_change",
				Timestamp: time.Now(),
				Source:    "file_watcher",
				UserID:    "system",
				Reason:    "設定ファイル変更検出",
			}

			select {
			case zcs.updateChan <- event:
			default:
				fmt.Println("設定更新チャンネルが満杯です")
			}
		},
	}

	// 初期ファイル情報取得
	if stat, err := os.Stat(watcher.filePath); err == nil {
		watcher.lastModTime = stat.ModTime()
		watcher.lastSize = stat.Size()
	}

	zcs.configManager.watchers["config"] = watcher

	// 監視ループ開始
	go zcs.fileWatchLoop(ctx, watcher)

	fmt.Printf("ファイル監視開始: %s\n", watcher.filePath)
	return nil
}

// fileWatchLoop ファイル監視ループ
func (zcs *ZeroDowntimeConfigSystem) fileWatchLoop(ctx context.Context, watcher *FileWatcher) {
	ticker := time.NewTicker(watcher.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if zcs.checkFileChange(watcher) {
				watcher.callback(watcher.filePath)
			}

		case <-ctx.Done():
			return
		case <-watcher.stopChan:
			return
		case <-zcs.stopChan:
			return
		}
	}
}

// checkFileChange ファイル変更チェック
func (zcs *ZeroDowntimeConfigSystem) checkFileChange(watcher *FileWatcher) bool {
	stat, err := os.Stat(watcher.filePath)
	if err != nil {
		return false
	}

	changed := false

	if !stat.ModTime().Equal(watcher.lastModTime) {
		changed = true
		watcher.lastModTime = stat.ModTime()
	}

	if stat.Size() != watcher.lastSize {
		changed = true
		watcher.lastSize = stat.Size()
	}

	// チェックサム確認（より確実な変更検出）
	if changed {
		data, err := os.ReadFile(watcher.filePath)
		if err == nil {
			checksum := zcs.calculateChecksum(data)
			if checksum != watcher.lastChecksum {
				watcher.lastChecksum = checksum
				return true
			}
		}
	}

	return false
}

// stopFileWatching ファイル監視停止
func (zcs *ZeroDowntimeConfigSystem) stopFileWatching() {
	for name, watcher := range zcs.configManager.watchers {
		close(watcher.stopChan)
		fmt.Printf("ファイル監視停止: %s\n", name)
	}
}

// configUpdateLoop 設定更新処理ループ
func (zcs *ZeroDowntimeConfigSystem) configUpdateLoop(ctx context.Context) {
	defer zcs.wg.Done()

	for {
		select {
		case event := <-zcs.updateChan:
			if err := zcs.processConfigUpdate(event); err != nil {
				fmt.Printf("設定更新処理エラー: %v\n", err)
				zcs.auditLogger.logAction("update_failed", nil, event.UserID, event.Reason, "error", err.Error())
			}

		case <-ctx.Done():
			return
		case <-zcs.stopChan:
			return
		}
	}
}

// processConfigUpdate 設定更新処理
func (zcs *ZeroDowntimeConfigSystem) processConfigUpdate(event *ConfigUpdateEvent) error {
	fmt.Printf("設定更新処理開始: %s\n", event.Type)

	// 新しい設定読み込み
	newConfig, err := zcs.loadConfigFromFile()
	if err != nil {
		return fmt.Errorf("設定ファイル読み込みエラー: %w", err)
	}

	// 現在の設定を取得
	zcs.configManager.mutex.RLock()
	oldConfig := zcs.configManager.currentConfig
	zcs.configManager.mutex.RUnlock()

	event.OldConfig = oldConfig
	event.NewConfig = newConfig

	// 設定変更チェック
	if oldConfig != nil && oldConfig.Checksum == newConfig.Checksum {
		fmt.Println("設定に変更がありません")
		return nil
	}

	// 設定検証
	if err := zcs.validateConfig(newConfig); err != nil {
		return fmt.Errorf("設定検証エラー: %w", err)
	}

	// バックアップ作成
	_, err = zcs.createConfigBackup(oldConfig)
	if err != nil {
		fmt.Printf("バックアップ作成警告: %v\n", err)
	}

	// カナリアリリース開始
	if err := zcs.startCanaryRollout(newConfig, event); err != nil {
		return fmt.Errorf("カナリアリリース開始エラー: %w", err)
	}

	// 設定更新カウンター増加
	atomic.AddInt64(&zcs.updateCount, 1)
	zcs.lastConfigUpdate = time.Now()

	// 監査ログ記録
	zcs.auditLogger.logAction("config_update", newConfig, event.UserID, event.Reason, "success", "")

	fmt.Printf("設定更新処理完了: バージョン %s\n", newConfig.Version)
	return nil
}

// loadConfigFromFile ファイルから設定読み込み
func (zcs *ZeroDowntimeConfigSystem) loadConfigFromFile() (*ApplicationConfig, error) {
	data, err := os.ReadFile(zcs.configManager.configFile)
	if err != nil {
		return nil, err
	}

	var config ApplicationConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, err
	}

	config.Checksum = zcs.calculateChecksum(data)
	config.Timestamp = time.Now()

	return &config, nil
}

// createConfigBackup 設定バックアップ作成
func (zcs *ZeroDowntimeConfigSystem) createConfigBackup(config *ApplicationConfig) (string, error) {
	if config == nil {
		return "", nil
	}

	timestamp := time.Now().Format("20060102_150405")
	backupPath := filepath.Join(zcs.configManager.backupDir, fmt.Sprintf("config_%s_%s.json", config.Version, timestamp))

	data, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return "", err
	}

	if err := os.WriteFile(backupPath, data, 0644); err != nil {
		return "", err
	}

	fmt.Printf("設定バックアップ作成: %s\n", backupPath)
	return backupPath, nil
}

// startCanaryRollout カナリアリリース開始
func (zcs *ZeroDowntimeConfigSystem) startCanaryRollout(newConfig *ApplicationConfig, event *ConfigUpdateEvent) error {
	zcs.rolloutManager.mutex.Lock()
	defer zcs.rolloutManager.mutex.Unlock()

	rolloutID := fmt.Sprintf("rollout_%d", time.Now().Unix())

	// イベント情報をログ出力
	fmt.Printf("カナリアリリース開始: ID=%s, 変更者=%s, 理由=%s, ソース=%s\n",
		rolloutID, event.UserID, event.Reason, event.Source)

	execution := &RolloutExecution{
		ID:           rolloutID,
		Strategy:     zcs.rolloutManager.rolloutStrategy,
		StartTime:    time.Now(),
		CurrentStage: 0,
		Status:       "in_progress",
		Metrics:      &RolloutMetrics{LastUpdated: time.Now()},
	}

	zcs.rolloutManager.currentRollout = execution

	// トラフィック分散器初期化
	zcs.rolloutManager.trafficSplitter.currentPercent = 0
	zcs.rolloutManager.trafficSplitter.targetPercent = execution.Strategy.Stages[0].TrafficPercent
	zcs.rolloutManager.trafficSplitter.isActive = true

	// 段階的適用開始
	if err := zcs.applyConfigGradually(newConfig, execution); err != nil {
		execution.Status = "failed"
		return err
	}

	fmt.Printf("カナリアリリース開始: %s\n", rolloutID)
	return nil
}

// applyConfigGradually 段階的設定適用
func (zcs *ZeroDowntimeConfigSystem) applyConfigGradually(newConfig *ApplicationConfig, execution *RolloutExecution) error {
	// 第1段階: カナリア環境に適用
	stage := execution.Strategy.Stages[execution.CurrentStage]
	fmt.Printf("ロールアウト段階 %d: %s (%d%% トラフィック)\n", execution.CurrentStage+1, stage.Name, stage.TrafficPercent)

	// トラフィック分散設定
	zcs.rolloutManager.trafficSplitter.mutex.Lock()
	zcs.rolloutManager.trafficSplitter.targetPercent = stage.TrafficPercent
	zcs.rolloutManager.trafficSplitter.mutex.Unlock()

	// 設定の一部適用（トラフィック比率に応じて）
	if err := zcs.applyConfigPartially(newConfig, stage.TrafficPercent); err != nil {
		return fmt.Errorf("部分設定適用エラー: %w", err)
	}

	// バージョン記録
	zcs.versionController.addVersion(newConfig, fmt.Sprintf("canary_rollout_%s", stage.Name), "system")

	return nil
}

// applyConfigPartially 部分的設定適用
func (zcs *ZeroDowntimeConfigSystem) applyConfigPartially(newConfig *ApplicationConfig, trafficPercent int) error {
	// トラフィック比率に応じた段階的適用の実装
	// 実際の実装では、ロードバランサーやプロキシとの連携が必要

	if trafficPercent >= 100 {
		// 全トラフィックに適用
		zcs.configManager.mutex.Lock()
		zcs.configManager.currentConfig = newConfig
		zcs.configManager.mutex.Unlock()

		// リロードコールバック実行
		for _, callback := range zcs.configManager.reloadCallbacks {
			if err := callback(newConfig); err != nil {
				fmt.Printf("リロードコールバックエラー: %v\n", err)
			}
		}

		fmt.Println("設定を全体に適用しました")
	} else {
		// 部分的適用（簡略化）
		zcs.configManager.mutex.Lock()
		zcs.configManager.pendingConfig = newConfig
		zcs.configManager.mutex.Unlock()

		fmt.Printf("設定を %d%% のトラフィックに適用しました\n", trafficPercent)
	}

	return nil
}

// healthCheckLoop ヘルスチェックループ
func (zcs *ZeroDowntimeConfigSystem) healthCheckLoop(ctx context.Context) {
	defer zcs.wg.Done()

	ticker := time.NewTicker(zcs.healthChecker.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			zcs.performHealthChecks()

		case <-ctx.Done():
			return
		case <-zcs.stopChan:
			return
		}
	}
}

// performHealthChecks ヘルスチェック実行
func (zcs *ZeroDowntimeConfigSystem) performHealthChecks() {
	zcs.healthChecker.mutex.Lock()
	defer zcs.healthChecker.mutex.Unlock()

	zcs.configManager.mutex.RLock()
	currentConfig := zcs.configManager.currentConfig
	zcs.configManager.mutex.RUnlock()

	if currentConfig == nil {
		return
	}

	allHealthy := true

	for name, check := range zcs.healthChecker.checks {
		startTime := time.Now()

		_, cancel := context.WithTimeout(context.Background(), zcs.healthChecker.timeout)

		result := &HealthCheckResult{
			Name:      name,
			Timestamp: startTime,
		}

		err := check(currentConfig)
		result.Duration = time.Since(startTime)

		if err != nil {
			result.Status = "unhealthy"
			result.Error = err.Error()
			allHealthy = false
			fmt.Printf("ヘルスチェック失敗 [%s]: %v\n", name, err)
		} else {
			result.Status = "healthy"
		}

		zcs.healthChecker.lastCheckResults[name] = result
		cancel()
	}

	if !allHealthy {
		// ヘルスチェック失敗時の処理
		zcs.handleHealthCheckFailure()
	}
}

// handleHealthCheckFailure ヘルスチェック失敗処理
func (zcs *ZeroDowntimeConfigSystem) handleHealthCheckFailure() {
	// 進行中のロールアウトがある場合は停止を検討
	zcs.rolloutManager.mutex.RLock()
	currentRollout := zcs.rolloutManager.currentRollout
	zcs.rolloutManager.mutex.RUnlock()

	if currentRollout != nil && currentRollout.Status == "in_progress" {
		fmt.Println("ヘルスチェック失敗により、ロールアウトを一時停止します")

		// 自動ロールバックトリガーをチェック
		zcs.checkRollbackTriggers()
	}
}

// rolloutMonitoringLoop ロールアウト監視ループ
func (zcs *ZeroDowntimeConfigSystem) rolloutMonitoringLoop(ctx context.Context) {
	defer zcs.wg.Done()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			zcs.monitorRolloutProgress()

		case <-ctx.Done():
			return
		case <-zcs.stopChan:
			return
		}
	}
}

// monitorRolloutProgress ロールアウト進捗監視
func (zcs *ZeroDowntimeConfigSystem) monitorRolloutProgress() {
	zcs.rolloutManager.mutex.RLock()
	currentRollout := zcs.rolloutManager.currentRollout
	zcs.rolloutManager.mutex.RUnlock()

	if currentRollout == nil || currentRollout.Status != "in_progress" {
		return
	}

	// メトリクス更新
	zcs.updateRolloutMetrics(currentRollout)

	// 成功基準チェック
	if zcs.checkSuccessCriteria(currentRollout) {
		// 次の段階に進む
		if currentRollout.CurrentStage < len(currentRollout.Strategy.Stages)-1 {
			zcs.promoteToNextStage(currentRollout)
		} else {
			// ロールアウト完了
			zcs.completeRollout(currentRollout)
		}
	}

	// タイムアウトチェック
	if time.Since(currentRollout.StartTime) > currentRollout.Strategy.MaxDuration {
		fmt.Println("ロールアウトがタイムアウトしました")
		zcs.triggerAutoRollback("timeout", "ロールアウトがタイムアウトしました")
	}
}

// updateRolloutMetrics ロールアウトメトリクス更新
func (zcs *ZeroDowntimeConfigSystem) updateRolloutMetrics(rollout *RolloutExecution) {
	// 簡略化のため、模擬メトリクスを生成
	rollout.Metrics.TotalRequests = atomic.LoadInt64(&zcs.rolloutManager.trafficSplitter.requestCounter)
	rollout.Metrics.SuccessfulRequests = rollout.Metrics.TotalRequests * 95 / 100 // 95% 成功率を仮定
	rollout.Metrics.FailedRequests = rollout.Metrics.TotalRequests - rollout.Metrics.SuccessfulRequests

	if rollout.Metrics.TotalRequests > 0 {
		rollout.Metrics.ErrorRate = float64(rollout.Metrics.FailedRequests) / float64(rollout.Metrics.TotalRequests) * 100
	}

	rollout.Metrics.AverageResponseTime = 150 * time.Millisecond // 仮の値
	rollout.Metrics.ThroughputRPS = 100.0                        // 仮の値
	rollout.Metrics.LastUpdated = time.Now()
}

// checkSuccessCriteria 成功基準チェック
func (zcs *ZeroDowntimeConfigSystem) checkSuccessCriteria(rollout *RolloutExecution) bool {
	stage := rollout.Strategy.Stages[rollout.CurrentStage]

	if stage.SuccessCriteria == nil {
		return true // 成功基準が設定されていない場合は成功とみなす
	}

	criteria := stage.SuccessCriteria
	metrics := rollout.Metrics

	// エラー率チェック
	if metrics.ErrorRate > criteria.MaxErrorRate {
		fmt.Printf("エラー率が基準を超過: %.2f%% > %.2f%%\n", metrics.ErrorRate, criteria.MaxErrorRate)
		return false
	}

	// レスポンス時間チェック
	if metrics.AverageResponseTime > criteria.MaxResponseTime {
		fmt.Printf("レスポンス時間が基準を超過: %v > %v\n", metrics.AverageResponseTime, criteria.MaxResponseTime)
		return false
	}

	// スループットチェック
	if metrics.ThroughputRPS < criteria.MinThroughput {
		fmt.Printf("スループットが基準を下回る: %.2f < %.2f\n", metrics.ThroughputRPS, criteria.MinThroughput)
		return false
	}

	fmt.Printf("段階 %s の成功基準を満たしています\n", stage.Name)
	return true
}

// promoteToNextStage 次段階への昇格
func (zcs *ZeroDowntimeConfigSystem) promoteToNextStage(rollout *RolloutExecution) {
	zcs.rolloutManager.mutex.Lock()
	defer zcs.rolloutManager.mutex.Unlock()

	rollout.CurrentStage++
	nextStage := rollout.Strategy.Stages[rollout.CurrentStage]

	fmt.Printf("次段階に昇格: %s (%d%% トラフィック)\n", nextStage.Name, nextStage.TrafficPercent)

	// トラフィック分散更新
	zcs.rolloutManager.trafficSplitter.targetPercent = nextStage.TrafficPercent

	// 設定の適用範囲拡大
	zcs.configManager.mutex.RLock()
	pendingConfig := zcs.configManager.pendingConfig
	zcs.configManager.mutex.RUnlock()

	if pendingConfig != nil {
		if err := zcs.applyConfigPartially(pendingConfig, nextStage.TrafficPercent); err != nil {
			fmt.Printf("設定部分適用エラー: %v\n", err)
		}
	}
}

// completeRollout ロールアウト完了
func (zcs *ZeroDowntimeConfigSystem) completeRollout(rollout *RolloutExecution) {
	zcs.rolloutManager.mutex.Lock()
	defer zcs.rolloutManager.mutex.Unlock()

	rollout.Status = "completed"

	// 全トラフィックに適用
	zcs.configManager.mutex.Lock()
	if zcs.configManager.pendingConfig != nil {
		zcs.configManager.currentConfig = zcs.configManager.pendingConfig
		zcs.configManager.pendingConfig = nil
	}
	zcs.configManager.mutex.Unlock()

	// トラフィック分散終了
	zcs.rolloutManager.trafficSplitter.isActive = false
	zcs.rolloutManager.trafficSplitter.currentPercent = 100

	fmt.Printf("ロールアウト完了: %s\n", rollout.ID)

	// 監査ログ記録
	zcs.auditLogger.logAction("rollout_completed", zcs.configManager.currentConfig, "system", "カナリアリリース完了", "success", "")
}

// autoRollbackLoop 自動ロールバックループ
func (zcs *ZeroDowntimeConfigSystem) autoRollbackLoop(ctx context.Context) {
	defer zcs.wg.Done()

	ticker := time.NewTicker(zcs.rollbackManager.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if zcs.rollbackManager.isEnabled {
				zcs.checkRollbackTriggers()
			}

		case <-ctx.Done():
			return
		case <-zcs.stopChan:
			return
		}
	}
}

// checkRollbackTriggers ロールバックトリガーチェック
func (zcs *ZeroDowntimeConfigSystem) checkRollbackTriggers() {
	zcs.rollbackManager.mutex.RLock()
	triggers := zcs.rollbackManager.triggers
	zcs.rollbackManager.mutex.RUnlock()

	for _, trigger := range triggers {
		if !trigger.Enabled {
			continue
		}

		if zcs.evaluateRollbackTrigger(trigger) {
			reason := fmt.Sprintf("トリガー '%s' が発火しました", trigger.Name)
			zcs.triggerAutoRollback(trigger.Name, reason)
			break // 一度に一つのトリガーのみ処理
		}
	}
}

// evaluateRollbackTrigger ロールバックトリガー評価
func (zcs *ZeroDowntimeConfigSystem) evaluateRollbackTrigger(trigger *RollbackTrigger) bool {
	switch trigger.Type {
	case "error_rate":
		zcs.rolloutManager.mutex.RLock()
		currentRollout := zcs.rolloutManager.currentRollout
		zcs.rolloutManager.mutex.RUnlock()

		if currentRollout != nil && currentRollout.Metrics.ErrorRate > trigger.Threshold {
			return true
		}

	case "response_time":
		zcs.rolloutManager.mutex.RLock()
		currentRollout := zcs.rolloutManager.currentRollout
		zcs.rolloutManager.mutex.RUnlock()

		if currentRollout != nil {
			thresholdMs := trigger.Threshold
			actualMs := float64(currentRollout.Metrics.AverageResponseTime.Milliseconds())
			if actualMs > thresholdMs {
				return true
			}
		}

	case "health_check_failure":
		zcs.healthChecker.mutex.RLock()
		results := zcs.healthChecker.lastCheckResults
		zcs.healthChecker.mutex.RUnlock()

		failureCount := 0
		for _, result := range results {
			if result.Status == "unhealthy" {
				failureCount++
			}
		}

		failureRate := float64(failureCount) / float64(len(results)) * 100
		if failureRate > trigger.Threshold {
			return true
		}
	}

	return false
}

// triggerAutoRollback 自動ロールバック実行
func (zcs *ZeroDowntimeConfigSystem) triggerAutoRollback(triggerName, reason string) {
	fmt.Printf("自動ロールバック実行: %s - %s\n", triggerName, reason)

	// 現在のロールアウト停止
	zcs.rolloutManager.mutex.Lock()
	if zcs.rolloutManager.currentRollout != nil {
		zcs.rolloutManager.currentRollout.Status = "rolling_back"
	}
	zcs.rolloutManager.mutex.Unlock()

	// 前バージョンに復元
	if err := zcs.rollbackToPreviousVersion(triggerName, reason); err != nil {
		fmt.Printf("自動ロールバックエラー: %v\n", err)

		// ロールバック失敗イベント記録
		event := &RollbackEvent{
			ID:           fmt.Sprintf("rollback_%d", time.Now().Unix()),
			Timestamp:    time.Now(),
			Trigger:      triggerName,
			Reason:       reason,
			Automatic:    true,
			Success:      false,
			ErrorMessage: err.Error(),
		}

		zcs.rollbackManager.mutex.Lock()
		zcs.rollbackManager.rollbackHistory = append(zcs.rollbackManager.rollbackHistory, event)
		zcs.rollbackManager.mutex.Unlock()

		return
	}

	// ロールバック成功イベント記録
	event := &RollbackEvent{
		ID:        fmt.Sprintf("rollback_%d", time.Now().Unix()),
		Timestamp: time.Now(),
		Trigger:   triggerName,
		Reason:    reason,
		Automatic: true,
		Success:   true,
	}

	zcs.rollbackManager.mutex.Lock()
	zcs.rollbackManager.rollbackHistory = append(zcs.rollbackManager.rollbackHistory, event)
	zcs.rollbackManager.mutex.Unlock()

	// 監査ログ記録
	zcs.auditLogger.logAction("auto_rollback", zcs.configManager.currentConfig, "system", reason, "success", "")

	fmt.Println("自動ロールバック完了")
}

// rollbackToPreviousVersion 前バージョンへのロールバック
func (zcs *ZeroDowntimeConfigSystem) rollbackToPreviousVersion(triggerName, reason string) error {
	// ロールバック開始ログ
	fmt.Printf("ロールバック実行開始: トリガー=%s, 理由=%s\n", triggerName, reason)

	// 前バージョン取得
	zcs.versionController.mutex.RLock()
	versions := zcs.versionController.versions
	zcs.versionController.mutex.RUnlock()

	if len(versions) < 2 {
		return fmt.Errorf("ロールバック可能なバージョンがありません")
	}

	// 最新版の一つ前のバージョンを取得
	previousVersion := versions[len(versions)-2]

	// 設定復元
	zcs.configManager.mutex.Lock()
	zcs.configManager.currentConfig = previousVersion.Config
	zcs.configManager.pendingConfig = nil
	zcs.configManager.mutex.Unlock()

	// トラフィック分散リセット
	zcs.rolloutManager.trafficSplitter.mutex.Lock()
	zcs.rolloutManager.trafficSplitter.isActive = false
	zcs.rolloutManager.trafficSplitter.currentPercent = 100
	zcs.rolloutManager.trafficSplitter.targetPercent = 100
	zcs.rolloutManager.trafficSplitter.mutex.Unlock()

	// リロードコールバック実行
	for _, callback := range zcs.configManager.reloadCallbacks {
		if err := callback(previousVersion.Config); err != nil {
			fmt.Printf("ロールバック時のコールバックエラー: %v\n", err)
		}
	}

	// ロールバック完了の監査ログ出力
	fmt.Printf("ロールバック完了: バージョン %s にロールバック (トリガー: %s, 理由: %s)\n",
		previousVersion.Version, triggerName, reason)

	// 監査エントリを記録
	if zcs.auditLogger != nil {
		zcs.auditLogger.logAction(
			"config_rollback",
			previousVersion.Config,
			"system_auto_rollback",
			fmt.Sprintf("トリガー: %s, 理由: %s", triggerName, reason),
			"success",
			"",
		)
	}

	return nil
}

// デフォルト設定メソッド群

// addDefaultHealthChecks デフォルトヘルスチェック追加
func (zcs *ZeroDowntimeConfigSystem) addDefaultHealthChecks() {
	zcs.healthChecker.checks["config_validation"] = func(config *ApplicationConfig) error {
		return zcs.validateConfig(config)
	}

	zcs.healthChecker.checks["server_port"] = func(config *ApplicationConfig) error {
		if config.Server.Port <= 0 || config.Server.Port > 65535 {
			return fmt.Errorf("無効なサーバーポート: %d", config.Server.Port)
		}
		return nil
	}

	zcs.healthChecker.checks["database_config"] = func(config *ApplicationConfig) error {
		if config.Database.Host == "" {
			return fmt.Errorf("データベースホストが未設定")
		}
		return nil
	}
}

// addDefaultValidationRules デフォルト検証ルール追加
func (zcs *ZeroDowntimeConfigSystem) addDefaultValidationRules() {
	zcs.validator.validationRules = []*ValidationRule{
		{
			Name:       "port_range",
			Type:       "port_validation",
			Required:   true,
			ErrorLevel: "error",
			Parameters: map[string]interface{}{
				"min_port": 1,
				"max_port": 65535,
			},
		},
		{
			Name:       "timeout_values",
			Type:       "timeout_validation",
			Required:   true,
			ErrorLevel: "warning",
			Parameters: map[string]interface{}{
				"min_timeout": "1s",
				"max_timeout": "300s",
			},
		},
	}

	// カスタム検証関数登録
	zcs.validator.customValidators["port_validation"] = func(config *ApplicationConfig, params map[string]interface{}) error {
		minPort := int(params["min_port"].(float64))
		maxPort := int(params["max_port"].(float64))

		if config.Server.Port < minPort || config.Server.Port > maxPort {
			return fmt.Errorf("サーバーポートが範囲外: %d (範囲: %d-%d)", config.Server.Port, minPort, maxPort)
		}

		if config.Database.Port < minPort || config.Database.Port > maxPort {
			return fmt.Errorf("データベースポートが範囲外: %d (範囲: %d-%d)", config.Database.Port, minPort, maxPort)
		}

		return nil
	}

	zcs.validator.customValidators["timeout_validation"] = func(config *ApplicationConfig, params map[string]interface{}) error {
		// タイムアウト値の妥当性チェック（簡略化）
		if config.Server.ReadTimeout < 1*time.Second {
			return fmt.Errorf("読み取りタイムアウトが短すぎます: %v", config.Server.ReadTimeout)
		}
		return nil
	}
}

// addDefaultRollbackTriggers デフォルトロールバックトリガー追加
func (zcs *ZeroDowntimeConfigSystem) addDefaultRollbackTriggers() {
	zcs.rollbackManager.triggers = []*RollbackTrigger{
		{
			Name:      "high_error_rate",
			Type:      "error_rate",
			Threshold: 5.0, // 5%
			Duration:  2 * time.Minute,
			Enabled:   true,
			Actions:   []string{"rollback", "notify"},
		},
		{
			Name:      "slow_response",
			Type:      "response_time",
			Threshold: 1000, // 1000ms
			Duration:  3 * time.Minute,
			Enabled:   true,
			Actions:   []string{"rollback", "notify"},
		},
		{
			Name:      "health_check_failures",
			Type:      "health_check_failure",
			Threshold: 50.0, // 50%
			Duration:  1 * time.Minute,
			Enabled:   true,
			Actions:   []string{"rollback", "notify"},
		},
	}
}

// バージョン制御メソッド群

// addVersion バージョン追加
func (vcr *ConfigVersionController) addVersion(config *ApplicationConfig, reason, changedBy string) {
	vcr.mutex.Lock()
	defer vcr.mutex.Unlock()

	version := &ConfigVersion{
		Version:       config.Version,
		Timestamp:     time.Now(),
		Config:        config,
		ChangeReason:  reason,
		ChangedBy:     changedBy,
		RolloutStatus: "applied",
		Checksum:      config.Checksum,
	}

	vcr.versions = append(vcr.versions, version)
	vcr.currentVersion = config.Version

	// 履歴サイズ制限
	if len(vcr.versions) > vcr.maxVersions {
		vcr.versions = vcr.versions[1:]
	}

	atomic.AddInt64(&vcr.versionCounter, 1)
}

// 監査ログメソッド群

// logAction アクション記録
func (cal *ConfigAuditLogger) logAction(action string, config *ApplicationConfig, changedBy, reason, result, errorMsg string) {
	cal.mutex.Lock()
	defer cal.mutex.Unlock()

	entry := &AuditEntry{
		ID:           fmt.Sprintf("audit_%d", time.Now().UnixNano()),
		Timestamp:    time.Now(),
		Action:       action,
		ChangedBy:    changedBy,
		ChangeReason: reason,
		Result:       result,
		ErrorMessage: errorMsg,
	}

	if config != nil {
		entry.ConfigVersion = config.Version
		entry.Changes = map[string]interface{}{
			"version":   config.Version,
			"checksum":  config.Checksum,
			"timestamp": config.Timestamp,
		}
	}

	cal.auditEntries = append(cal.auditEntries, entry)

	// 履歴サイズ制限
	if len(cal.auditEntries) > cal.maxEntries {
		cal.auditEntries = cal.auditEntries[1:]
	}

	// ファイルに記録
	cal.writeToFile(entry)
}

// writeToFile ファイル書き込み
func (cal *ConfigAuditLogger) writeToFile(entry *AuditEntry) {
	data, err := json.Marshal(entry)
	if err != nil {
		fmt.Printf("監査ログ書き込みエラー: %v\n", err)
		return
	}

	f, err := os.OpenFile(cal.logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Printf("監査ログファイルオープンエラー: %v\n", err)
		return
	}
	defer func() {
		if err := f.Close(); err != nil {
			fmt.Printf("監査ログファイルクローズエラー: %v\n", err)
		}
	}()

	if _, err := f.Write(data); err != nil {
		fmt.Printf("監査ログ書き込みエラー: %v\n", err)
		return
	}
	if _, err := f.WriteString("\n"); err != nil {
		fmt.Printf("監査ログ改行書き込みエラー: %v\n", err)
	}
}

// HTTP API ハンドラー群

// configAPIHandler 設定APIハンドラー
func (zcs *ZeroDowntimeConfigSystem) configAPIHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		zcs.configManager.mutex.RLock()
		config := zcs.configManager.currentConfig
		zcs.configManager.mutex.RUnlock()

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(config); err != nil {
			fmt.Printf("設定エンコードエラー: %v\n", err)
		}

	case "POST":
		var newConfig ApplicationConfig
		if err := json.NewDecoder(r.Body).Decode(&newConfig); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		event := &ConfigUpdateEvent{
			Type:      "api_update",
			Timestamp: time.Now(),
			NewConfig: &newConfig,
			Source:    "api",
			UserID:    r.Header.Get("X-User-ID"),
			Reason:    r.Header.Get("X-Change-Reason"),
		}

		select {
		case zcs.updateChan <- event:
			w.WriteHeader(http.StatusAccepted)
			if err := json.NewEncoder(w).Encode(map[string]string{
				"status":  "accepted",
				"message": "設定更新を受け付けました",
			}); err != nil {
				fmt.Printf("受付応答エンコードエラー: %v\n", err)
			}
		default:
			http.Error(w, "設定更新キューが満杯です", http.StatusServiceUnavailable)
		}

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// reloadAPIHandler リロードAPIハンドラー
func (zcs *ZeroDowntimeConfigSystem) reloadAPIHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	event := &ConfigUpdateEvent{
		Type:      "manual_reload",
		Timestamp: time.Now(),
		Source:    "api",
		UserID:    r.Header.Get("X-User-ID"),
		Reason:    "手動リロード要求",
	}

	select {
	case zcs.updateChan <- event:
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string]string{
			"status":  "success",
			"message": "設定リロードを開始しました",
		}); err != nil {
			fmt.Printf("リロード応答エンコードエラー: %v\n", err)
		}
	default:
		http.Error(w, "設定更新キューが満杯です", http.StatusServiceUnavailable)
	}
}

// rollbackAPIHandler ロールバックAPIハンドラー
func (zcs *ZeroDowntimeConfigSystem) rollbackAPIHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	reason := r.Header.Get("X-Rollback-Reason")
	if reason == "" {
		reason = "手動ロールバック要求"
	}

	if err := zcs.rollbackToPreviousVersion("manual", reason); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]string{
		"status":  "success",
		"message": "ロールバックが完了しました",
	}); err != nil {
		fmt.Printf("ロールバック応答エンコードエラー: %v\n", err)
	}
}

// versionsAPIHandler バージョンAPIハンドラー
func (zcs *ZeroDowntimeConfigSystem) versionsAPIHandler(w http.ResponseWriter, r *http.Request) {
	zcs.versionController.mutex.RLock()
	versions := zcs.versionController.versions
	zcs.versionController.mutex.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(versions); err != nil {
		fmt.Printf("バージョン一覧エンコードエラー: %v\n", err)
	}
}

// auditAPIHandler 監査APIハンドラー
func (zcs *ZeroDowntimeConfigSystem) auditAPIHandler(w http.ResponseWriter, r *http.Request) {
	zcs.auditLogger.mutex.RLock()
	entries := zcs.auditLogger.auditEntries
	zcs.auditLogger.mutex.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(entries); err != nil {
		fmt.Printf("監査ログエンコードエラー: %v\n", err)
	}
}

// healthAPIHandler ヘルスAPIハンドラー
func (zcs *ZeroDowntimeConfigSystem) healthAPIHandler(w http.ResponseWriter, r *http.Request) {
	zcs.healthChecker.mutex.RLock()
	results := zcs.healthChecker.lastCheckResults
	zcs.healthChecker.mutex.RUnlock()

	overallStatus := "healthy"
	for _, result := range results {
		if result.Status == "unhealthy" {
			overallStatus = "unhealthy"
			break
		}
	}

	response := map[string]interface{}{
		"overall_status": overallStatus,
		"checks":         results,
		"timestamp":      time.Now(),
	}

	w.Header().Set("Content-Type", "application/json")
	if overallStatus == "unhealthy" {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	if err := json.NewEncoder(w).Encode(response); err != nil {
		fmt.Printf("ヘルスチェック応答エンコードエラー: %v\n", err)
	}
}

// rolloutStatusAPIHandler ロールアウトステータスAPIハンドラー
func (zcs *ZeroDowntimeConfigSystem) rolloutStatusAPIHandler(w http.ResponseWriter, r *http.Request) {
	zcs.rolloutManager.mutex.RLock()
	currentRollout := zcs.rolloutManager.currentRollout
	trafficSplitter := zcs.rolloutManager.trafficSplitter
	zcs.rolloutManager.mutex.RUnlock()

	response := map[string]interface{}{
		"current_rollout":   currentRollout,
		"traffic_splitting": trafficSplitter,
		"timestamp":         time.Now(),
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		fmt.Printf("ロールアウトステータス応答エンコードエラー: %v\n", err)
	}
}

func main() {
	// ゼロダウンタイム設定更新システムを作成
	configSystem := NewZeroDowntimeConfigSystem("app_config.json")

	ctx := context.Background()

	// システム開始
	if err := configSystem.Start(ctx); err != nil {
		panic(err)
	}

	fmt.Println("ゼロダウンタイム設定更新システム実行中")
	fmt.Println("設定管理API: http://localhost:9999")
	fmt.Println("\nAPIエンドポイント:")
	fmt.Println("- GET  /api/config           (現在の設定取得)")
	fmt.Println("- POST /api/config           (設定更新)")
	fmt.Println("- POST /api/config/reload    (手動リロード)")
	fmt.Println("- POST /api/config/rollback  (ロールバック)")
	fmt.Println("- GET  /api/config/versions  (バージョン履歴)")
	fmt.Println("- GET  /api/config/audit     (監査ログ)")
	fmt.Println("- GET  /api/config/health    (ヘルスチェック)")
	fmt.Println("- GET  /api/rollout/status   (ロールアウト状況)")
	fmt.Println("\nCtrl+Cで停止")

	// 30秒間実行
	time.Sleep(30 * time.Second)

	// システム停止
	if err := configSystem.Stop(); err != nil {
		fmt.Printf("停止エラー: %v\n", err)
	}
}
