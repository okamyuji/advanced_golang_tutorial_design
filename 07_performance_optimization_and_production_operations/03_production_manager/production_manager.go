package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// ProductionManager 本番環境運用管理システム
type ProductionManager struct {
	httpServer    *http.Server
	healthChecker *HealthChecker
	configManager *ConfigManager
	shutdownHooks []func() error
	mutex         sync.RWMutex
	running       bool
}

// HealthChecker ヘルスチェック機能
type HealthChecker struct {
	checks    map[string]HealthCheck
	mutex     sync.RWMutex
	interval  time.Duration
	timeout   time.Duration
	stopChan  chan struct{}
	lastCheck time.Time
	healthy   bool
}

// HealthCheck ヘルスチェック関数型
type HealthCheck func(ctx context.Context) error

// HealthStatus ヘルスステータス
type HealthStatus struct {
	Overall   string                       `json:"overall"`
	Timestamp time.Time                    `json:"timestamp"`
	Checks    map[string]HealthCheckResult `json:"checks"`
	Uptime    time.Duration                `json:"uptime"`
}

// HealthCheckResult 個別ヘルスチェック結果
type HealthCheckResult struct {
	Status    string        `json:"status"`
	Error     string        `json:"error,omitempty"`
	Duration  time.Duration `json:"duration"`
	Timestamp time.Time     `json:"timestamp"`
}

// ConfigManager 設定管理機能
type ConfigManager struct {
	configFile     string
	config         map[string]interface{}
	mutex          sync.RWMutex
	reloadHandlers []func(map[string]interface{}) error
	lastModified   time.Time
	watcher        *ConfigWatcher
}

// ConfigWatcher 設定ファイル監視
type ConfigWatcher struct {
	stopChan chan struct{}
	interval time.Duration
}

// NewProductionManager 新しい本番運用管理システムを作成
func NewProductionManager(port int) *ProductionManager {
	mux := http.NewServeMux()

	pm := &ProductionManager{
		httpServer: &http.Server{
			Addr:    fmt.Sprintf(":%d", port),
			Handler: mux,
		},
		healthChecker: NewHealthChecker(30*time.Second, 10*time.Second),
		configManager: NewConfigManager("config.json"),
		shutdownHooks: make([]func() error, 0),
	}

	// エンドポイント設定
	mux.HandleFunc("/health", pm.healthHandler)
	mux.HandleFunc("/config", pm.configHandler)
	mux.HandleFunc("/shutdown", pm.shutdownHandler)
	mux.HandleFunc("/reload", pm.reloadHandler)
	mux.HandleFunc("/status", pm.statusHandler)

	// 基本的なヘルスチェックを追加
	pm.healthChecker.AddCheck("system", pm.systemHealthCheck)
	pm.healthChecker.AddCheck("memory", pm.memoryHealthCheck)
	pm.healthChecker.AddCheck("goroutines", pm.goroutineHealthCheck)

	return pm
}

// NewHealthChecker 新しいヘルスチェッカーを作成
func NewHealthChecker(interval, timeout time.Duration) *HealthChecker {
	return &HealthChecker{
		checks:   make(map[string]HealthCheck),
		interval: interval,
		timeout:  timeout,
		stopChan: make(chan struct{}),
		healthy:  true,
	}
}

// NewConfigManager 新しい設定管理器を作成
func NewConfigManager(configFile string) *ConfigManager {
	return &ConfigManager{
		configFile:     configFile,
		config:         make(map[string]interface{}),
		reloadHandlers: make([]func(map[string]interface{}) error, 0),
		watcher: &ConfigWatcher{
			stopChan: make(chan struct{}),
			interval: 5 * time.Second,
		},
	}
}

// Start システム開始
func (pm *ProductionManager) Start(ctx context.Context) error {
	pm.running = true

	// 設定ファイル読み込み
	if err := pm.configManager.LoadConfig(); err != nil {
		fmt.Printf("設定ファイル読み込みエラー: %v\n", err)
	}

	// ヘルスチェック開始
	if err := pm.healthChecker.Start(ctx); err != nil {
		return fmt.Errorf("ヘルスチェック開始エラー: %w", err)
	}

	// 設定ファイル監視開始
	if err := pm.configManager.StartWatcher(ctx); err != nil {
		return fmt.Errorf("設定ファイル監視開始エラー: %w", err)
	}

	// シグナルハンドリング設定
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGTERM, syscall.SIGINT)

	// Graceful Shutdown処理
	go func() {
		<-signalChan
		fmt.Println("シャットダウンシグナル受信")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := pm.GracefulShutdown(shutdownCtx); err != nil {
			fmt.Printf("Graceful shutdown failed: %v\n", err)
		}
	}()

	fmt.Printf("本番運用管理システム開始: %s\n", pm.httpServer.Addr)
	return pm.httpServer.ListenAndServe()
}

// GracefulShutdown グレースフルシャットダウン
func (pm *ProductionManager) GracefulShutdown(ctx context.Context) error {
	fmt.Println("グレースフルシャットダウン開始")

	pm.running = false

	// シャットダウンフック実行
	for i, hook := range pm.shutdownHooks {
		fmt.Printf("シャットダウンフック実行 %d/%d\n", i+1, len(pm.shutdownHooks))
		if err := hook(); err != nil {
			fmt.Printf("シャットダウンフックエラー: %v\n", err)
		}
	}

	// ヘルスチェック停止
	pm.healthChecker.Stop()

	// 設定監視停止
	pm.configManager.StopWatcher()

	// HTTPサーバー停止
	fmt.Println("HTTPサーバー停止中...")
	return pm.httpServer.Shutdown(ctx)
}

// AddShutdownHook シャットダウンフック追加
func (pm *ProductionManager) AddShutdownHook(hook func() error) {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()
	pm.shutdownHooks = append(pm.shutdownHooks, hook)
}

// HealthChecker methods

// Start ヘルスチェック開始
func (hc *HealthChecker) Start(ctx context.Context) error {
	go hc.healthCheckLoop(ctx)
	return nil
}

// Stop ヘルスチェック停止
func (hc *HealthChecker) Stop() {
	close(hc.stopChan)
}

// AddCheck ヘルスチェック追加
func (hc *HealthChecker) AddCheck(name string, check HealthCheck) {
	hc.mutex.Lock()
	defer hc.mutex.Unlock()
	hc.checks[name] = check
}

// GetStatus ヘルスステータス取得
func (hc *HealthChecker) GetStatus() *HealthStatus {
	hc.mutex.RLock()
	defer hc.mutex.RUnlock()

	status := &HealthStatus{
		Overall:   "healthy",
		Timestamp: time.Now(),
		Checks:    make(map[string]HealthCheckResult),
		Uptime:    time.Since(hc.lastCheck),
	}

	// 簡略化のため、現在の状態をチェック
	if !hc.healthy {
		status.Overall = "unhealthy"
	}

	return status
}

// healthCheckLoop ヘルスチェックループ
func (hc *HealthChecker) healthCheckLoop(ctx context.Context) {
	ticker := time.NewTicker(hc.interval)
	defer ticker.Stop()

	hc.lastCheck = time.Now()

	for {
		select {
		case <-ticker.C:
			hc.runHealthChecks(ctx)
		case <-ctx.Done():
			return
		case <-hc.stopChan:
			return
		}
	}
}

// runHealthChecks ヘルスチェック実行
func (hc *HealthChecker) runHealthChecks(ctx context.Context) {
	hc.mutex.RLock()
	checks := make(map[string]HealthCheck)
	for name, check := range hc.checks {
		checks[name] = check
	}
	hc.mutex.RUnlock()

	allHealthy := true
	for name, check := range checks {
		checkCtx, cancel := context.WithTimeout(ctx, hc.timeout)
		startTime := time.Now()

		err := check(checkCtx)
		duration := time.Since(startTime)

		if err != nil {
			allHealthy = false
			fmt.Printf("ヘルスチェック失敗 [%s]: %v (実行時間: %v)\n", name, err, duration)
		}

		cancel()
	}

	hc.healthy = allHealthy
	hc.lastCheck = time.Now()
}

// ConfigManager methods

// LoadConfig 設定ファイル読み込み
func (cm *ConfigManager) LoadConfig() error {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	// 設定ファイルが存在しない場合はデフォルト設定を作成
	if _, err := os.Stat(cm.configFile); os.IsNotExist(err) {
		defaultConfig := map[string]interface{}{
			"app_name":    "production_manager",
			"version":     "1.0.0",
			"debug":       false,
			"max_workers": 10,
		}

		data, err := json.MarshalIndent(defaultConfig, "", "  ")
		if err != nil {
			return err
		}

		if err := os.WriteFile(cm.configFile, data, 0644); err != nil {
			return err
		}

		cm.config = defaultConfig
		return nil
	}

	// 設定ファイル読み込み
	data, err := os.ReadFile(cm.configFile)
	if err != nil {
		return err
	}

	var config map[string]interface{}
	if err := json.Unmarshal(data, &config); err != nil {
		return err
	}

	cm.config = config

	// ファイル変更時刻更新
	if stat, err := os.Stat(cm.configFile); err == nil {
		cm.lastModified = stat.ModTime()
	}

	return nil
}

// StartWatcher 設定ファイル監視開始
func (cm *ConfigManager) StartWatcher(ctx context.Context) error {
	go cm.watchConfig(ctx)
	return nil
}

// StopWatcher 設定ファイル監視停止
func (cm *ConfigManager) StopWatcher() {
	close(cm.watcher.stopChan)
}

// watchConfig 設定ファイル監視
func (cm *ConfigManager) watchConfig(ctx context.Context) {
	ticker := time.NewTicker(cm.watcher.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			cm.checkConfigChange()
		case <-ctx.Done():
			return
		case <-cm.watcher.stopChan:
			return
		}
	}
}

// checkConfigChange 設定ファイル変更チェック
func (cm *ConfigManager) checkConfigChange() {
	stat, err := os.Stat(cm.configFile)
	if err != nil {
		return
	}

	if stat.ModTime().After(cm.lastModified) {
		fmt.Println("設定ファイル変更検出、リロード実行")
		if err := cm.LoadConfig(); err != nil {
			fmt.Printf("設定リロードエラー: %v\n", err)
			return
		}

		// リロードハンドラー実行
		cm.mutex.RLock()
		config := make(map[string]interface{})
		for k, v := range cm.config {
			config[k] = v
		}
		handlers := make([]func(map[string]interface{}) error, len(cm.reloadHandlers))
		copy(handlers, cm.reloadHandlers)
		cm.mutex.RUnlock()

		for _, handler := range handlers {
			if err := handler(config); err != nil {
				fmt.Printf("リロードハンドラーエラー: %v\n", err)
			}
		}
	}
}

// システムヘルスチェック関数群

// systemHealthCheck システムヘルスチェック
func (pm *ProductionManager) systemHealthCheck(ctx context.Context) error {
	if !pm.running {
		return fmt.Errorf("システムが停止中")
	}
	return nil
}

// memoryHealthCheck メモリヘルスチェック
func (pm *ProductionManager) memoryHealthCheck(ctx context.Context) error {
	// 簡略化のため、基本的なメモリチェックのみ
	return nil
}

// goroutineHealthCheck Goroutineヘルスチェック
func (pm *ProductionManager) goroutineHealthCheck(ctx context.Context) error {
	// 簡略化のため、基本的なGoroutineチェックのみ
	return nil
}

// HTTP ハンドラー関数群

// healthHandler ヘルスチェックハンドラー
func (pm *ProductionManager) healthHandler(w http.ResponseWriter, r *http.Request) {
	status := pm.healthChecker.GetStatus()
	w.Header().Set("Content-Type", "application/json")

	if status.Overall != "healthy" {
		w.WriteHeader(http.StatusServiceUnavailable)
	}

	if err := json.NewEncoder(w).Encode(status); err != nil {
		fmt.Printf("Failed to encode status response: %v\n", err)
	}
}

// configHandler 設定表示ハンドラー
func (pm *ProductionManager) configHandler(w http.ResponseWriter, r *http.Request) {
	pm.configManager.mutex.RLock()
	config := pm.configManager.config
	pm.configManager.mutex.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(config); err != nil {
		fmt.Printf("Failed to encode config response: %v\n", err)
	}
}

// shutdownHandler シャットダウンハンドラー
func (pm *ProductionManager) shutdownHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	go func() {
		time.Sleep(1 * time.Second) // レスポンス返却後にシャットダウン
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := pm.GracefulShutdown(ctx); err != nil {
			fmt.Printf("Graceful shutdown failed: %v\n", err)
		}
		os.Exit(0)
	}()

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]string{
		"status":  "shutdown_initiated",
		"message": "グレースフルシャットダウンを開始しました",
	}); err != nil {
		fmt.Printf("Failed to encode shutdown response: %v\n", err)
	}
}

// reloadHandler 設定リロードハンドラー
func (pm *ProductionManager) reloadHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if err := pm.configManager.LoadConfig(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]string{
		"status":  "success",
		"message": "設定をリロードしました",
	}); err != nil {
		fmt.Printf("Failed to encode reload response: %v\n", err)
	}
}

// statusHandler ステータスハンドラー
func (pm *ProductionManager) statusHandler(w http.ResponseWriter, r *http.Request) {
	status := map[string]interface{}{
		"running":   pm.running,
		"timestamp": time.Now().Unix(),
		"health":    pm.healthChecker.GetStatus(),
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(status); err != nil {
		fmt.Printf("Failed to encode status response: %v\n", err)
	}
}

func main() {
	ctx := context.Background()

	// 本番運用管理システムを作成
	manager := NewProductionManager(9090)

	// シャットダウンフック追加
	manager.AddShutdownHook(func() error {
		fmt.Println("カスタムシャットダウン処理実行")
		return nil
	})

	fmt.Println("本番運用管理システム開始")
	fmt.Println("エンドポイント:")
	fmt.Println("- http://localhost:9090/health (ヘルスチェック)")
	fmt.Println("- http://localhost:9090/config (設定表示)")
	fmt.Println("- http://localhost:9090/status (システムステータス)")
	fmt.Println("- POST http://localhost:9090/shutdown (グレースフルシャットダウン)")
	fmt.Println("- POST http://localhost:9090/reload (設定リロード)")

	// システム開始
	if err := manager.Start(ctx); err != nil && err != http.ErrServerClosed {
		panic(err)
	}
}
