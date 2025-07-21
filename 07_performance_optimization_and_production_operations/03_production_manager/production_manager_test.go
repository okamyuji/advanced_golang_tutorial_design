package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
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
	configFile := "test_config.json"
	defer func() {
		if err := os.Remove(configFile); err != nil {
			t.Logf("Failed to remove test config file: %v", err)
		}
	}() // テスト後にファイル削除

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
	configFile := "invalid_config.json"
	defer func() {
		if err := os.Remove(configFile); err != nil {
			t.Logf("Failed to remove invalid config file: %v", err)
		}
	}()

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
	ctx := context.Background()

	// システムが停止中の場合
	manager.running = false
	err := manager.systemHealthCheck(ctx)
	if err == nil {
		t.Error("Expected error when system not running")
	}

	// システムが実行中の場合
	manager.running = true
	err = manager.systemHealthCheck(ctx)
	if err != nil {
		t.Errorf("Unexpected error when system running: %v", err)
	}
}

func TestMemoryHealthCheck(t *testing.T) {
	manager := NewProductionManager(9094)
	ctx := context.Background()

	// メモリヘルスチェック実行（エラーが発生しないことを確認）
	err := manager.memoryHealthCheck(ctx)
	if err != nil {
		t.Errorf("Unexpected error in memory health check: %v", err)
	}
}

func TestGoroutineHealthCheck(t *testing.T) {
	manager := NewProductionManager(9095)
	ctx := context.Background()

	// Goroutineヘルスチェック実行（エラーが発生しないことを確認）
	err := manager.goroutineHealthCheck(ctx)
	if err != nil {
		t.Errorf("Unexpected error in goroutine health check: %v", err)
	}
}

func TestHealthHandler(t *testing.T) {
	manager := NewProductionManager(9096)
	manager.running = true
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

	var response map[string]interface{}
	err := json.NewDecoder(w.Body).Decode(&response)
	if err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if testValue, ok := response["test_key"].(string); !ok || testValue != "test_value" {
		t.Errorf("Expected test_key 'test_value', got %v", response["test_key"])
	}
}

func TestShutdownHandler(t *testing.T) {
	manager := NewProductionManager(9098)

	// POSTメソッドでのテスト
	req := httptest.NewRequest("POST", "/shutdown", nil)
	w := httptest.NewRecorder()

	manager.shutdownHandler(w, req)

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

	manager.shutdownHandler(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected status 405, got %d", w.Code)
	}
}

func TestReloadHandler(t *testing.T) {
	configFile := "test_reload_config.json"
	defer func() {
		if err := os.Remove(configFile); err != nil {
			t.Logf("Failed to remove reload config file: %v", err)
		}
	}()

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

	manager.reloadHandler(w, req)

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

	manager.reloadHandler(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected status 405, got %d", w.Code)
	}
}

func TestStatusHandler(t *testing.T) {
	manager := NewProductionManager(9100)
	manager.running = true

	req := httptest.NewRequest("GET", "/status", nil)
	w := httptest.NewRecorder()

	manager.statusHandler(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	var response map[string]interface{}
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
	configFile := "test_change_config.json"
	defer func() {
		if err := os.Remove(configFile); err != nil {
			t.Logf("Failed to remove change config file: %v", err)
		}
	}()

	manager := NewConfigManager(configFile)

	// 初期設定ファイル作成
	err := manager.LoadConfig()
	if err != nil {
		t.Fatalf("Failed to create initial config: %v", err)
	}

	originalModTime := manager.lastModified

	// ファイルを変更
	time.Sleep(10 * time.Millisecond) // ファイルシステムの時間精度を考慮
	updatedConfig := map[string]interface{}{
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

func TestConfigWatcherIntegration(t *testing.T) {
	configFile := "test_watcher_config.json"
	defer func() {
		if err := os.Remove(configFile); err != nil {
			t.Logf("Failed to remove watcher config file: %v", err)
		}
	}()

	manager := NewConfigManager(configFile)
	manager.watcher.interval = 100 * time.Millisecond // 短い間隔でテスト

	// 初期設定ファイル作成
	err := manager.LoadConfig()
	if err != nil {
		t.Fatalf("Failed to create initial config: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// ウォッチャー開始
	err = manager.StartWatcher(ctx)
	if err != nil {
		t.Fatalf("Failed to start watcher: %v", err)
	}

	// 少し待機してからウォッチャー停止
	time.Sleep(200 * time.Millisecond)
	manager.StopWatcher()

	// エラーが発生しないことを確認
}

func BenchmarkHealthCheck(b *testing.B) {
	manager := NewProductionManager(9101)
	manager.running = true
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := manager.systemHealthCheck(ctx)
		if err != nil {
			b.Fatalf("Health check failed: %v", err)
		}
	}
}

func BenchmarkConfigLoad(b *testing.B) {
	configFile := "bench_config.json"
	defer func() {
		if err := os.Remove(configFile); err != nil {
			b.Logf("Failed to remove bench config file: %v", err)
		}
	}()

	manager := NewConfigManager(configFile)

	// 初期設定ファイル作成
	err := manager.LoadConfig()
	if err != nil {
		b.Fatalf("Failed to create initial config: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := manager.LoadConfig()
		if err != nil {
			b.Fatalf("LoadConfig failed: %v", err)
		}
	}
}
