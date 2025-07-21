package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"testing"
	"time"
)

func TestNewProfilingSystem(t *testing.T) {
	profiler := NewProfilingSystem(8081, 30*time.Second, 24*time.Hour)

	if profiler == nil {
		t.Fatal("NewProfilingSystem returned nil")
	}

	if profiler.httpServer == nil {
		t.Error("httpServer not initialized")
	}

	if profiler.profileInterval != 30*time.Second {
		t.Errorf("Expected profileInterval 30s, got %v", profiler.profileInterval)
	}

	if profiler.dataRetention != 24*time.Hour {
		t.Errorf("Expected dataRetention 24h, got %v", profiler.dataRetention)
	}

	if profiler.profiles == nil {
		t.Error("profiles map not initialized")
	}

	if profiler.stopChan == nil {
		t.Error("stopChan not initialized")
	}
}

func TestProfilingSystemStartStop(t *testing.T) {
	profiler := NewProfilingSystem(8082, 1*time.Second, 1*time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// システム開始
	err := profiler.StartProfiling(ctx)
	if err != nil {
		t.Fatalf("StartProfiling failed: %v", err)
	}

	if !profiler.running {
		t.Error("Expected running to be true after StartProfiling")
	}

	// 少し待機してプロファイル収集を確認
	time.Sleep(2 * time.Second)

	// システム停止
	err = profiler.Stop()
	if err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	if profiler.running {
		t.Error("Expected running to be false after Stop")
	}
}

func TestCollectProfiles(t *testing.T) {
	profiler := NewProfilingSystem(8083, 30*time.Second, 24*time.Hour)

	// 初期状態確認
	if len(profiler.profiles) != 0 {
		t.Error("Expected empty profiles initially")
	}

	// プロファイル収集実行
	profiler.collectProfiles()

	// プロファイルが追加されたことを確認
	if len(profiler.profiles) != 1 {
		t.Errorf("Expected 1 profile, got %d", len(profiler.profiles))
	}

	// プロファイルデータの内容確認
	for _, profile := range profiler.profiles {
		if profile.Timestamp.IsZero() {
			t.Error("Profile timestamp not set")
		}

		if profile.GoroutineInfo == nil {
			t.Error("GoroutineInfo not set")
		}

		if profile.MemStats == nil {
			t.Error("MemStats not set")
		}

		if profile.Metrics == nil {
			t.Error("Metrics not set")
		}

		// Goroutine情報の妥当性確認
		if profile.GoroutineInfo.Count <= 0 {
			t.Error("GoroutineInfo.Count should be positive")
		}

		// メトリクスの妥当性確認
		if _, ok := profile.Metrics["timestamp"]; !ok {
			t.Error("Metrics missing timestamp")
		}

		if _, ok := profile.Metrics["goroutine_count"]; !ok {
			t.Error("Metrics missing goroutine_count")
		}
	}
}

func TestCollectGoroutineInfo(t *testing.T) {
	profiler := NewProfilingSystem(8084, 30*time.Second, 24*time.Hour)

	info := profiler.collectGoroutineInfo()
	if info == nil {
		t.Fatal("collectGoroutineInfo returned nil")
	}

	if info.Count <= 0 {
		t.Error("GoroutineInfo.Count should be positive")
	}

	// 簡略化された推定値の妥当性確認
	expectedTotal := info.Running + info.Waiting + info.Sleeping + info.Blocked + info.Syscall
	if expectedTotal > info.Count {
		t.Errorf("Sum of goroutine states (%d) exceeds total count (%d)", expectedTotal, info.Count)
	}
}

func TestCollectCustomMetrics(t *testing.T) {
	profiler := NewProfilingSystem(8085, 30*time.Second, 24*time.Hour)

	metrics := profiler.collectCustomMetrics()
	if metrics == nil {
		t.Fatal("collectCustomMetrics returned nil")
	}

	expectedFields := []string{"timestamp", "cpu_count", "goroutine_count", "cgo_calls"}
	for _, field := range expectedFields {
		if _, ok := metrics[field]; !ok {
			t.Errorf("Metrics missing field: %s", field)
		}
	}

	// 値の妥当性確認
	if cpuCount, ok := metrics["cpu_count"].(int); !ok || cpuCount <= 0 {
		t.Errorf("Expected positive cpu_count, got %v", metrics["cpu_count"])
	}

	if goroutineCount, ok := metrics["goroutine_count"].(int); !ok || goroutineCount <= 0 {
		t.Errorf("Expected positive goroutine_count, got %v", metrics["goroutine_count"])
	}
}

func TestCleanupOldData(t *testing.T) {
	profiler := NewProfilingSystem(8086, 30*time.Second, 1*time.Second) // 短い保持期間

	// 古いデータを追加
	oldTime := time.Now().Add(-2 * time.Second)
	profiler.profiles["old_profile"] = &ProfileData{
		Timestamp: oldTime,
	}

	// 新しいデータを追加
	newTime := time.Now()
	profiler.profiles["new_profile"] = &ProfileData{
		Timestamp: newTime,
	}

	if len(profiler.profiles) != 2 {
		t.Errorf("Expected 2 profiles before cleanup, got %d", len(profiler.profiles))
	}

	// クリーンアップ実行
	profiler.cleanupOldData()

	// 古いデータが削除されていることを確認
	if len(profiler.profiles) != 1 {
		t.Errorf("Expected 1 profile after cleanup, got %d", len(profiler.profiles))
	}

	if _, ok := profiler.profiles["old_profile"]; ok {
		t.Error("Old profile should be removed")
	}

	if _, ok := profiler.profiles["new_profile"]; !ok {
		t.Error("New profile should be retained")
	}
}

func TestHealthHandler(t *testing.T) {
	profiler := NewProfilingSystem(8087, 30*time.Second, 24*time.Hour)
	profiler.running = true

	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()

	profiler.healthHandler(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	var response map[string]interface{}
	err := json.NewDecoder(w.Body).Decode(&response)
	if err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if status, ok := response["status"].(string); !ok || status != "healthy" {
		t.Errorf("Expected status 'healthy', got %v", response["status"])
	}

	if running, ok := response["running"].(bool); !ok || !running {
		t.Errorf("Expected running true, got %v", response["running"])
	}
}

func TestMetricsHandler(t *testing.T) {
	profiler := NewProfilingSystem(8088, 30*time.Second, 24*time.Hour)

	req := httptest.NewRequest("GET", "/metrics", nil)
	w := httptest.NewRecorder()

	profiler.metricsHandler(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	var response map[string]interface{}
	err := json.NewDecoder(w.Body).Decode(&response)
	if err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	expectedFields := []string{"goroutine_count", "memory_alloc", "memory_sys", "gc_cycles", "timestamp"}
	for _, field := range expectedFields {
		if _, ok := response[field]; !ok {
			t.Errorf("Metrics response missing field: %s", field)
		}
	}
}

func TestProfilesHandler(t *testing.T) {
	profiler := NewProfilingSystem(8089, 30*time.Second, 24*time.Hour)

	// テスト用プロファイルデータを追加
	testProfile := &ProfileData{
		Timestamp: time.Now(),
		GoroutineInfo: &GoroutineInfo{
			Count: 10,
		},
		Metrics: map[string]interface{}{
			"test": "value",
		},
	}
	profiler.profiles["test_profile"] = testProfile

	req := httptest.NewRequest("GET", "/profiles", nil)
	w := httptest.NewRecorder()

	profiler.profilesHandler(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	var response []*ProfileData
	err := json.NewDecoder(w.Body).Decode(&response)
	if err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if len(response) != 1 {
		t.Errorf("Expected 1 profile in response, got %d", len(response))
	}

	if response[0].GoroutineInfo.Count != 10 {
		t.Errorf("Expected GoroutineInfo.Count 10, got %d", response[0].GoroutineInfo.Count)
	}
}

func TestAlertsHandler(t *testing.T) {
	profiler := NewProfilingSystem(8090, 30*time.Second, 24*time.Hour)

	req := httptest.NewRequest("GET", "/alerts", nil)
	w := httptest.NewRecorder()

	profiler.alertsHandler(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	var response []PerformanceAlert
	err := json.NewDecoder(w.Body).Decode(&response)
	if err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	// レスポンスが配列であることを確認（内容は環境によって変わる）
	if response == nil {
		t.Error("Expected array response, got nil")
	}
}

func TestSaveCPUProfile(t *testing.T) {
	profiler := NewProfilingSystem(8091, 30*time.Second, 24*time.Hour)

	filename := "test_cpu_profile.out"
	defer func() {
		if err := os.Remove(filename); err != nil {
			t.Logf("Failed to remove test file: %v", err)
		}
	}() // テスト後にファイル削除

	err := profiler.SaveCPUProfile(filename, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("SaveCPUProfile failed: %v", err)
	}

	// ファイルが作成されていることを確認
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		t.Error("CPU profile file was not created")
	}
}

func TestSaveMemProfile(t *testing.T) {
	profiler := NewProfilingSystem(8092, 30*time.Second, 24*time.Hour)

	filename := "test_mem_profile.out"
	defer func() {
		if err := os.Remove(filename); err != nil {
			t.Logf("Failed to remove test file: %v", err)
		}
	}() // テスト後にファイル削除

	err := profiler.SaveMemProfile(filename)
	if err != nil {
		t.Fatalf("SaveMemProfile failed: %v", err)
	}

	// ファイルが作成されていることを確認
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		t.Error("Memory profile file was not created")
	}
}

func TestCheckPerformanceAlerts(t *testing.T) {
	profiler := NewProfilingSystem(8093, 30*time.Second, 24*time.Hour)

	// アラートが発生しない正常なデータ
	normalData := &ProfileData{
		Timestamp: time.Now(),
		MemStats: &runtime.MemStats{
			Alloc: 50 * 1024 * 1024, // 50MB
		},
		GoroutineInfo: &GoroutineInfo{
			Count: 100,
		},
	}

	// アラート判定実行（エラーが発生しないことを確認）
	profiler.checkPerformanceAlerts(normalData)

	// アラートが発生するデータ
	alertData := &ProfileData{
		Timestamp: time.Now(),
		MemStats: &runtime.MemStats{
			Alloc: 200 * 1024 * 1024, // 200MB（閾値超過）
		},
		GoroutineInfo: &GoroutineInfo{
			Count: 2000, // 閾値超過
		},
	}

	// アラート判定実行（エラーが発生しないことを確認）
	profiler.checkPerformanceAlerts(alertData)
}

func BenchmarkCollectProfiles(b *testing.B) {
	profiler := NewProfilingSystem(8094, 30*time.Second, 24*time.Hour)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		profiler.collectProfiles()
	}
}

func BenchmarkCollectGoroutineInfo(b *testing.B) {
	profiler := NewProfilingSystem(8095, 30*time.Second, 24*time.Hour)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		info := profiler.collectGoroutineInfo()
		if info == nil {
			b.Fatal("collectGoroutineInfo returned nil")
		}
	}
}
