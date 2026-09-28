package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

var (
	errAlreadyRunning = errors.New("本番運用管理システムは実行中です")
	errManagerStopped = errors.New("本番運用管理システムは停止済みです")
)

// ProductionManager 本番環境運用管理システム
type ProductionManager struct {
	httpServer    *http.Server
	healthChecker *HealthChecker
	configManager *ConfigManager
	shutdownHooks []func() error
	mutex         sync.RWMutex
	running       atomic.Bool
	stopped       atomic.Bool

	// シグナルと /shutdown の両方から呼ばれても停止処理を1回だけ行う
	shutdownOnce sync.Once
	shutdownErr  error
	shutdownDone chan struct{}
}

// HealthChecker ヘルスチェック機能
type HealthChecker struct {
	checks    map[string]HealthCheck
	mutex     sync.RWMutex
	interval  time.Duration
	timeout   time.Duration
	stopChan  chan struct{}
	stop      func() // stopChan を1回だけ close する
	startedAt time.Time
	lastCheck time.Time
	healthy   bool
	results   map[string]HealthCheckResult
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
	config         map[string]any
	mutex          sync.RWMutex
	reloadHandlers []func(map[string]any) error
	lastModified   time.Time
	watcher        *ConfigWatcher
}

// ConfigWatcher 設定ファイル監視
type ConfigWatcher struct {
	stopChan chan struct{}
	stop     func() // stopChan を1回だけ close する
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
		shutdownDone:  make(chan struct{}),
	}

	// エンドポイント設定
	mux.HandleFunc("/health", pm.healthHandler)
	mux.HandleFunc("/config", pm.configHandler)
	mux.HandleFunc("POST /shutdown", pm.shutdownHandler)
	mux.HandleFunc("POST /reload", pm.reloadHandler)
	mux.HandleFunc("/status", pm.statusHandler)

	// 基本的なヘルスチェックを追加
	pm.healthChecker.AddCheck("system", pm.systemHealthCheck)
	pm.healthChecker.AddCheck("memory", pm.memoryHealthCheck)
	pm.healthChecker.AddCheck("goroutines", pm.goroutineHealthCheck)

	return pm
}

// NewHealthChecker 新しいヘルスチェッカーを作成
func NewHealthChecker(interval, timeout time.Duration) *HealthChecker {
	stopChan := make(chan struct{})
	return &HealthChecker{
		checks:   make(map[string]HealthCheck),
		interval: interval,
		timeout:  timeout,
		stopChan: stopChan,
		stop:     sync.OnceFunc(func() { close(stopChan) }),
		healthy:  true,
		results:  make(map[string]HealthCheckResult),
	}
}

// NewConfigManager 新しい設定管理器を作成
func NewConfigManager(configFile string) *ConfigManager {
	stopChan := make(chan struct{})
	return &ConfigManager{
		configFile:     configFile,
		config:         make(map[string]any),
		reloadHandlers: make([]func(map[string]any) error, 0),
		watcher: &ConfigWatcher{
			stopChan: stopChan,
			stop:     sync.OnceFunc(func() { close(stopChan) }),
			interval: 5 * time.Second,
		},
	}
}

// Start システム開始。SIGTERM/SIGINT を受けるか ctx が終わるとグレースフルシャットダウンし、
// 停止処理が終わってから http.ErrServerClosed を返す
func (pm *ProductionManager) Start(ctx context.Context) error {
	// http.Server は Shutdown 後に再利用できないので、停止後の再開は受け付けない
	if pm.stopped.Load() {
		return errManagerStopped
	}
	if !pm.running.CompareAndSwap(false, true) {
		return errAlreadyRunning
	}

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
	sigCtx, stopSignals := signal.NotifyContext(ctx, syscall.SIGTERM, syscall.SIGINT)
	defer stopSignals()

	// Graceful Shutdown処理。サーバーが別の理由で止まった場合も、後片付けのためここを通る
	go func() {
		<-sigCtx.Done()
		fmt.Println("シャットダウン開始")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := pm.GracefulShutdown(shutdownCtx); err != nil {
			fmt.Printf("Graceful shutdown failed: %v\n", err)
		}
	}()

	fmt.Printf("本番運用管理システム開始: %s\n", pm.httpServer.Addr)
	err := pm.httpServer.ListenAndServe()

	// ListenAndServe は Shutdown の開始直後に返るので、停止処理の完了を待ってから返す
	stopSignals()
	<-pm.shutdownDone
	return err
}

// GracefulShutdown グレースフルシャットダウン。並行して何度呼ばれても停止処理は1回だけ行い、
// 全員がその完了を待って同じ結果を受け取る
func (pm *ProductionManager) GracefulShutdown(ctx context.Context) error {
	pm.shutdownOnce.Do(func() {
		pm.shutdownErr = pm.shutdown(ctx)
		close(pm.shutdownDone)
	})
	return pm.shutdownErr
}

// shutdown 停止処理の本体
func (pm *ProductionManager) shutdown(ctx context.Context) error {
	fmt.Println("グレースフルシャットダウン開始")

	pm.stopped.Store(true)
	pm.running.Store(false)

	// シャットダウンフック実行
	pm.mutex.RLock()
	hooks := slices.Clone(pm.shutdownHooks)
	pm.mutex.RUnlock()
	for i, hook := range hooks {
		fmt.Printf("シャットダウンフック実行 %d/%d\n", i+1, len(hooks))
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
	hc.mutex.Lock()
	hc.startedAt = time.Now()
	hc.lastCheck = hc.startedAt
	hc.mutex.Unlock()

	go hc.healthCheckLoop(ctx)
	return nil
}

// Stop ヘルスチェック停止。二度目以降の呼び出しは何もしない
func (hc *HealthChecker) Stop() {
	hc.stop()
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
		Checks:    maps.Clone(hc.results),
	}
	if !hc.startedAt.IsZero() {
		status.Uptime = time.Since(hc.startedAt)
	}

	if !hc.healthy {
		status.Overall = "unhealthy"
	}

	return status
}

// healthCheckLoop ヘルスチェックループ
func (hc *HealthChecker) healthCheckLoop(ctx context.Context) {
	tick := time.Tick(hc.interval)

	for {
		select {
		case <-tick:
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
	checks := maps.Clone(hc.checks)
	hc.mutex.RUnlock()

	allHealthy := true
	results := make(map[string]HealthCheckResult, len(checks))
	for name, check := range checks {
		checkCtx, cancel := context.WithTimeout(ctx, hc.timeout)
		startTime := time.Now()

		err := check(checkCtx)
		duration := time.Since(startTime)
		cancel()

		result := HealthCheckResult{Status: "healthy", Duration: duration, Timestamp: startTime}
		if err != nil {
			allHealthy = false
			result.Status = "unhealthy"
			result.Error = err.Error()
			fmt.Printf("ヘルスチェック失敗 [%s]: %v (実行時間: %v)\n", name, err, duration)
		}
		results[name] = result
	}

	hc.mutex.Lock()
	hc.healthy = allHealthy
	hc.results = results
	hc.lastCheck = time.Now()
	hc.mutex.Unlock()
}

// ConfigManager methods

// LoadConfig 設定ファイル読み込み
func (cm *ConfigManager) LoadConfig() error {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	// 設定ファイルが存在しない場合はデフォルト設定を作成
	if _, err := os.Stat(cm.configFile); os.IsNotExist(err) {
		defaultConfig := map[string]any{
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
		// 作成した時刻を覚えておかないと、次の監視で自分の書き込みを変更と誤認する
		if stat, err := os.Stat(cm.configFile); err == nil {
			cm.lastModified = stat.ModTime()
		}
		return nil
	}

	// 設定ファイル読み込み
	data, err := os.ReadFile(cm.configFile)
	if err != nil {
		return err
	}

	var config map[string]any
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

// StopWatcher 設定ファイル監視停止。二度目以降の呼び出しは何もしない
func (cm *ConfigManager) StopWatcher() {
	cm.watcher.stop()
}

// watchConfig 設定ファイル監視
func (cm *ConfigManager) watchConfig(ctx context.Context) {
	tick := time.Tick(cm.watcher.interval)

	for {
		select {
		case <-tick:
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

	cm.mutex.RLock()
	lastModified := cm.lastModified
	cm.mutex.RUnlock()

	if stat.ModTime().After(lastModified) {
		fmt.Println("設定ファイル変更検出、リロード実行")
		if err := cm.LoadConfig(); err != nil {
			fmt.Printf("設定リロードエラー: %v\n", err)
			return
		}

		// リロードハンドラー実行
		cm.mutex.RLock()
		config := maps.Clone(cm.config)
		handlers := slices.Clone(cm.reloadHandlers)
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
	if !pm.running.Load() {
		return errors.New("システムが停止中")
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
// Shutdown は処理中のリクエストが終わるのを待つので、この応答は返してから停止する。
// 停止が終わると Start が返り、main が終了する
func (pm *ProductionManager) shutdownHandler(w http.ResponseWriter, r *http.Request) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := pm.GracefulShutdown(ctx); err != nil {
			fmt.Printf("Graceful shutdown failed: %v\n", err)
		}
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
	status := map[string]any{
		"running":   pm.running.Load(),
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
	if err := manager.Start(ctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		panic(err)
	}
}
