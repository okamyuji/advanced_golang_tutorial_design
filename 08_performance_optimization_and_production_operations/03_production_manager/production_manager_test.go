package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

func TestNewProductionManager(t *testing.T) {
	manager := NewProductionManager(9091)

	if manager == nil {
		t.Fatal("NewProductionManager returned nil")
	}

	if manager.httpServer == nil {
		t.Error("httpServer not initialized")
	}

	if manager.healthChecker == nil {
		t.Error("healthChecker not initialized")
	}

	if manager.configManager == nil {
		t.Error("configManager not initialized")
	}

	if manager.shutdownHooks == nil {
		t.Error("shutdownHooks not initialized")
	}
}

func TestNewHealthChecker(t *testing.T) {
	checker := NewHealthChecker(30*time.Second, 10*time.Second)

	if checker == nil {
		t.Fatal("NewHealthChecker returned nil")
	}

	if checker.interval != 30*time.Second {
		t.Errorf("Expected interval 30s, got %v", checker.interval)
	}

	if checker.timeout != 10*time.Second {
		t.Errorf("Expected timeout 10s, got %v", checker.timeout)
	}

	if checker.checks == nil {
		t.Error("checks map not initialized")
	}

	if !checker.healthy {
		t.Error("Expected initial healthy state to be true")
	}
}

func TestNewConfigManager(t *testing.T) {
	manager := NewConfigManager("test_config.json")

	if manager == nil {
		t.Fatal("NewConfigManager returned nil")
	}

	if manager.configFile != "test_config.json" {
		t.Errorf("Expected configFile 'test_config.json', got '%s'", manager.configFile)
	}

	if manager.config == nil {
		t.Error("config map not initialized")
	}

	if manager.reloadHandlers == nil {
		t.Error("reloadHandlers not initialized")
	}

	if manager.watcher == nil {
		t.Error("watcher not initialized")
	}
}

func TestHealthCheckerAddCheck(t *testing.T) {
	checker := NewHealthChecker(30*time.Second, 10*time.Second)

	testCheck := func(ctx context.Context) error {
		return nil
	}

	checker.AddCheck("test_check", testCheck)

	if len(checker.checks) != 1 {
		t.Errorf("Expected 1 check, got %d", len(checker.checks))
	}

	if _, ok := checker.checks["test_check"]; !ok {
		t.Error("test_check not found in checks")
	}
}

func TestHealthCheckerGetStatus(t *testing.T) {
	checker := NewHealthChecker(30*time.Second, 10*time.Second)
	checker.lastCheck = time.Now()

	status := checker.GetStatus()
	if status == nil {
		t.Fatal("GetStatus returned nil")
	}

	if status.Overall != "healthy" {
		t.Errorf("Expected overall status 'healthy', got '%s'", status.Overall)
	}

	if status.Timestamp.IsZero() {
		t.Error("Status timestamp not set")
	}

	if status.Checks == nil {
		t.Error("Checks map not initialized")
	}

	// 異常状態のテスト
	checker.healthy = false
	status = checker.GetStatus()
	if status.Overall != "unhealthy" {
		t.Errorf("Expected overall status 'unhealthy', got '%s'", status.Overall)
	}
}

func TestConfigManagerLoadConfig(t *testing.T) {
	configFile := filepath.Join(t.TempDir(), "test_config.json")

	manager := NewConfigManager(configFile)

	// 設定ファイルが存在しない場合のテスト（デフォルト設定作成）
	err := manager.LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	// デフォルト設定が作成されていることを確認
	if len(manager.config) == 0 {
		t.Error("Default config not created")
	}

	expectedFields := []string{"app_name", "version", "debug", "max_workers"}
	for _, field := range expectedFields {
		if _, ok := manager.config[field]; !ok {
			t.Errorf("Default config missing field: %s", field)
		}
	}

	// ファイルが作成されていることを確認
	if _, err := os.Stat(configFile); os.IsNotExist(err) {
		t.Error("Config file was not created")
	}

	// 既存ファイルからの読み込みテスト
	err = manager.LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig from existing file failed: %v", err)
	}
}

func TestConfigManagerInvalidFile(t *testing.T) {
	configFile := filepath.Join(t.TempDir(), "invalid_config.json")

	// 無効なJSONファイルを作成
	invalidJSON := `{"invalid": json}`
	err := os.WriteFile(configFile, []byte(invalidJSON), 0644)
	if err != nil {
		t.Fatalf("Failed to create invalid config file: %v", err)
	}

	manager := NewConfigManager(configFile)

	// 無効なJSONの読み込みでエラーが発生することを確認
	err = manager.LoadConfig()
	if err == nil {
		t.Error("Expected error when loading invalid JSON")
	}
}

func TestAddShutdownHook(t *testing.T) {
	manager := NewProductionManager(9092)

	hookCalled := false
	testHook := func() error {
		hookCalled = true
		return nil
	}

	manager.AddShutdownHook(testHook)

	if len(manager.shutdownHooks) != 1 {
		t.Errorf("Expected 1 shutdown hook, got %d", len(manager.shutdownHooks))
	}

	// フック実行テスト
	err := manager.shutdownHooks[0]()
	if err != nil {
		t.Fatalf("Shutdown hook execution failed: %v", err)
	}

	if !hookCalled {
		t.Error("Shutdown hook was not called")
	}
}

func TestSystemHealthCheck(t *testing.T) {
	manager := NewProductionManager(9093)
	ctx := t.Context()

	// システムが停止中の場合
	manager.running.Store(false)
	err := manager.systemHealthCheck(ctx)
	if err == nil {
		t.Error("Expected error when system not running")
	}

	// システムが実行中の場合
	manager.running.Store(true)
	err = manager.systemHealthCheck(ctx)
	if err != nil {
		t.Errorf("Unexpected error when system running: %v", err)
	}
}

func TestMemoryHealthCheck(t *testing.T) {
	manager := NewProductionManager(9094)
	ctx := t.Context()

	// メモリヘルスチェック実行（エラーが発生しないことを確認）
	err := manager.memoryHealthCheck(ctx)
	if err != nil {
		t.Errorf("Unexpected error in memory health check: %v", err)
	}
}

func TestGoroutineHealthCheck(t *testing.T) {
	manager := NewProductionManager(9095)
	ctx := t.Context()

	// Goroutineヘルスチェック実行（エラーが発生しないことを確認）
	err := manager.goroutineHealthCheck(ctx)
	if err != nil {
		t.Errorf("Unexpected error in goroutine health check: %v", err)
	}
}

func TestHealthHandler(t *testing.T) {
	manager := NewProductionManager(9096)
	manager.running.Store(true)
	manager.healthChecker.healthy = true

	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()

	manager.healthHandler(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	var response HealthStatus
	err := json.NewDecoder(w.Body).Decode(&response)
	if err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if response.Overall != "healthy" {
		t.Errorf("Expected overall 'healthy', got '%s'", response.Overall)
	}

	// 異常状態のテスト
	manager.healthChecker.healthy = false
	w = httptest.NewRecorder()
	manager.healthHandler(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("Expected status 503, got %d", w.Code)
	}
}

func TestConfigHandler(t *testing.T) {
	manager := NewProductionManager(9097)

	// テスト用設定を追加
	manager.configManager.config["test_key"] = "test_value"

	req := httptest.NewRequest("GET", "/config", nil)
	w := httptest.NewRecorder()

	manager.configHandler(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	var response map[string]any
	err := json.NewDecoder(w.Body).Decode(&response)
	if err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if testValue, ok := response["test_key"].(string); !ok || testValue != "test_value" {
		t.Errorf("Expected test_key 'test_value', got %v", response["test_key"])
	}
}

func TestShutdownHandler(t *testing.T) {
	manager := newTestManager(t)
	mux := manager.httpServer.Handler

	// POSTメソッドでのテスト
	req := httptest.NewRequest("POST", "/shutdown", nil)
	w := httptest.NewRecorder()

	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	var response map[string]string
	err := json.NewDecoder(w.Body).Decode(&response)
	if err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if response["status"] != "shutdown_initiated" {
		t.Errorf("Expected status 'shutdown_initiated', got '%s'", response["status"])
	}

	// GETメソッドでのテスト（エラーが返されることを確認）
	req = httptest.NewRequest("GET", "/shutdown", nil)
	w = httptest.NewRecorder()

	mux.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected status 405, got %d", w.Code)
	}

	// 受け付けた停止処理の完了を待つ
	if err := manager.GracefulShutdown(t.Context()); err != nil {
		t.Fatalf("GracefulShutdown failed: %v", err)
	}
	if manager.running.Load() {
		t.Error("Expected running to be false after shutdown")
	}
}

func TestReloadHandler(t *testing.T) {
	configFile := filepath.Join(t.TempDir(), "test_reload_config.json")

	manager := NewProductionManager(9099)
	manager.configManager.configFile = configFile

	// 設定ファイルを作成
	err := manager.configManager.LoadConfig()
	if err != nil {
		t.Fatalf("Failed to create config file: %v", err)
	}

	// POSTメソッドでのテスト
	req := httptest.NewRequest("POST", "/reload", nil)
	w := httptest.NewRecorder()

	manager.httpServer.Handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	var response map[string]string
	err = json.NewDecoder(w.Body).Decode(&response)
	if err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if response["status"] != "success" {
		t.Errorf("Expected status 'success', got '%s'", response["status"])
	}

	// GETメソッドでのテスト（エラーが返されることを確認）
	req = httptest.NewRequest("GET", "/reload", nil)
	w = httptest.NewRecorder()

	manager.httpServer.Handler.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected status 405, got %d", w.Code)
	}
}

func TestStatusHandler(t *testing.T) {
	manager := NewProductionManager(9100)
	manager.running.Store(true)

	req := httptest.NewRequest("GET", "/status", nil)
	w := httptest.NewRecorder()

	manager.statusHandler(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	var response map[string]any
	err := json.NewDecoder(w.Body).Decode(&response)
	if err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if running, ok := response["running"].(bool); !ok || !running {
		t.Errorf("Expected running true, got %v", response["running"])
	}

	if _, ok := response["timestamp"]; !ok {
		t.Error("Status response missing timestamp")
	}

	if _, ok := response["health"]; !ok {
		t.Error("Status response missing health")
	}
}

func TestCheckConfigChange(t *testing.T) {
	configFile := filepath.Join(t.TempDir(), "test_change_config.json")

	manager := NewConfigManager(configFile)

	// 初期設定ファイル作成
	err := manager.LoadConfig()
	if err != nil {
		t.Fatalf("Failed to create initial config: %v", err)
	}

	originalModTime := manager.lastModified

	// ファイルを変更
	updatedConfig := map[string]any{
		"app_name": "updated_app",
		"version":  "2.0.0",
	}

	data, err := json.MarshalIndent(updatedConfig, "", "  ")
	if err != nil {
		t.Fatalf("Failed to marshal updated config: %v", err)
	}

	err = os.WriteFile(configFile, data, 0644)
	if err != nil {
		t.Fatalf("Failed to write updated config: %v", err)
	}
	// ファイルシステムの時刻の粒度に依存しないよう、更新時刻を明示的に進める
	later := originalModTime.Add(time.Second)
	if err := os.Chtimes(configFile, later, later); err != nil {
		t.Fatalf("Chtimes failed: %v", err)
	}

	// 変更検出テスト
	manager.checkConfigChange()

	// 設定が更新されていることを確認
	if manager.lastModified.Equal(originalModTime) {
		t.Error("lastModified was not updated")
	}

	if appName, ok := manager.config["app_name"].(string); !ok || appName != "updated_app" {
		t.Errorf("Expected app_name 'updated_app', got %v", manager.config["app_name"])
	}
}

// TestConfigWatcherIntegration 監視ループが設定ファイルの書き換えを拾って再読み込みすることを確かめる
func TestConfigWatcherIntegration(t *testing.T) {
	configFile := filepath.Join(t.TempDir(), "test_watcher_config.json")

	synctest.Test(t, func(t *testing.T) {
		manager := NewConfigManager(configFile)
		manager.watcher.interval = 100 * time.Millisecond

		// 初期設定ファイル作成
		if err := manager.LoadConfig(); err != nil {
			t.Fatalf("Failed to create initial config: %v", err)
		}

		data, err := json.Marshal(map[string]any{"app_name": "watched_app"})
		if err != nil {
			t.Fatalf("Marshal failed: %v", err)
		}
		if err := os.WriteFile(configFile, data, 0o644); err != nil {
			t.Fatalf("WriteFile failed: %v", err)
		}
		later := manager.lastModified.Add(time.Second)
		if err := os.Chtimes(configFile, later, later); err != nil {
			t.Fatalf("Chtimes failed: %v", err)
		}

		if err := manager.StartWatcher(t.Context()); err != nil {
			t.Fatalf("Failed to start watcher: %v", err)
		}

		// 監視間隔を1回分進め、監視ループが処理を終えるまで待つ
		time.Sleep(150 * time.Millisecond)
		synctest.Wait()

		manager.mutex.RLock()
		appName := manager.config["app_name"]
		manager.mutex.RUnlock()
		if appName != "watched_app" {
			t.Errorf("Expected app_name 'watched_app', got %v", appName)
		}

		manager.StopWatcher()
		manager.StopWatcher()
	})
}

func BenchmarkHealthCheck(b *testing.B) {
	manager := NewProductionManager(9101)
	manager.running.Store(true)
	ctx := b.Context()

	for b.Loop() {
		err := manager.systemHealthCheck(ctx)
		if err != nil {
			b.Fatalf("Health check failed: %v", err)
		}
	}
}

func BenchmarkConfigLoad(b *testing.B) {
	configFile := filepath.Join(b.TempDir(), "bench_config.json")

	manager := NewConfigManager(configFile)

	// 初期設定ファイル作成
	err := manager.LoadConfig()
	if err != nil {
		b.Fatalf("Failed to create initial config: %v", err)
	}

	for b.Loop() {
		err := manager.LoadConfig()
		if err != nil {
			b.Fatalf("LoadConfig failed: %v", err)
		}
	}
}

// newTestManager 設定ファイルを一時ディレクトリに置き、空きポートで待ち受けるマネージャーを作る
func newTestManager(t *testing.T) *ProductionManager {
	t.Helper()
	manager := NewProductionManager(0)
	manager.httpServer.Addr = "127.0.0.1:0"
	manager.configManager.configFile = filepath.Join(t.TempDir(), "config.json")
	return manager
}

// TestGracefulShutdownConcurrent シグナルと /shutdown が重なるなど、停止処理が並行して呼ばれても panic しないことを確かめる
func TestGracefulShutdownConcurrent(t *testing.T) {
	for range 50 {
		manager := newTestManager(t)
		if err := manager.healthChecker.Start(t.Context()); err != nil {
			t.Fatalf("healthChecker.Start failed: %v", err)
		}
		if err := manager.configManager.StartWatcher(t.Context()); err != nil {
			t.Fatalf("StartWatcher failed: %v", err)
		}

		var wg sync.WaitGroup
		for range 2 {
			wg.Go(func() {
				if err := manager.GracefulShutdown(t.Context()); err != nil {
					t.Errorf("GracefulShutdown failed: %v", err)
				}
			})
		}
		wg.Go(func() {
			manager.statusHandler(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/status", nil))
		})
		wg.Wait()
	}
}

// TestStartReturnsWhenContextCanceled ctx が終わると Start がグレースフルシャットダウンして返り、停止後の再開はエラーになることを確かめる
func TestStartReturnsWhenContextCanceled(t *testing.T) {
	manager := newTestManager(t)
	ctx, cancel := context.WithCancel(t.Context())

	errc := make(chan error, 1)
	go func() { errc <- manager.Start(ctx) }()
	cancel()

	select {
	case err := <-errc:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Fatalf("Start returned unexpected error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ctx が終わっても Start が返らない")
	}

	if err := manager.Start(t.Context()); err == nil || errors.Is(err, http.ErrServerClosed) {
		t.Fatalf("停止後の Start が明示的なエラーになっていない: %v", err)
	}
}

// TestShutdownHandlerRunsHooksWithoutExit /shutdown はプロセスを終了させず、シャットダウンフックを実行して停止処理を終えることを確かめる
func TestShutdownHandlerRunsHooksWithoutExit(t *testing.T) {
	manager := newTestManager(t)
	hookCalled := make(chan struct{})
	manager.AddShutdownHook(func() error {
		close(hookCalled)
		return nil
	})

	w := httptest.NewRecorder()
	manager.shutdownHandler(w, httptest.NewRequest(http.MethodPost, "/shutdown", nil))

	select {
	case <-hookCalled:
	case <-time.After(5 * time.Second):
		t.Fatal("シャットダウンフックが呼ばれない")
	}
	// os.Exit を呼ぶ実装だと、ここで待つ間にテストプロセスが終了する
	if err := manager.GracefulShutdown(t.Context()); err != nil {
		t.Fatalf("GracefulShutdown failed: %v", err)
	}
}

// TestHealthStatusConcurrentWithChecks ヘルスチェックの実行と状態取得を並行させる
func TestHealthStatusConcurrentWithChecks(t *testing.T) {
	checker := NewHealthChecker(time.Second, time.Second)
	checker.AddCheck("fail", func(context.Context) error { return errors.New("失敗") })

	var wg sync.WaitGroup
	wg.Go(func() {
		for range 20 {
			checker.runHealthChecks(t.Context())
		}
	})
	wg.Go(func() {
		for range 20 {
			checker.GetStatus()
		}
	})
	wg.Wait()

	status := checker.GetStatus()
	if status.Overall != "unhealthy" {
		t.Errorf("Expected unhealthy, got %s", status.Overall)
	}
	if result, ok := status.Checks["fail"]; !ok || result.Status != "unhealthy" {
		t.Errorf("個別チェックの結果が返っていない: %+v", status.Checks)
	}
}

// TestConfigChangeConcurrentWithReload 設定ファイルの監視と /reload による読み込みを並行させる
func TestConfigChangeConcurrentWithReload(t *testing.T) {
	manager := NewConfigManager(filepath.Join(t.TempDir(), "config.json"))
	if err := manager.LoadConfig(); err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	var wg sync.WaitGroup
	wg.Go(func() {
		for range 20 {
			manager.checkConfigChange()
		}
	})
	wg.Go(func() {
		for range 20 {
			if err := manager.LoadConfig(); err != nil {
				t.Errorf("LoadConfig failed: %v", err)
			}
		}
	})
	wg.Wait()
}
