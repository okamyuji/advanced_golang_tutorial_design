package main

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"testing/synctest"
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
	profiler := NewProfilingSystem(0, 1*time.Second, 1*time.Hour)
	ctx := t.Context()

	// システム開始
	err := profiler.StartProfiling(ctx)
	if err != nil {
		t.Fatalf("StartProfiling failed: %v", err)
	}

	if !profiler.running.Load() {
		t.Error("Expected running to be true after StartProfiling")
	}

	// 起動したサーバーに実際に届くことを確認
	resp, err := http.Get("http://" + profiler.listenAddr() + "/health")
	if err != nil {
		t.Fatalf("GET /health failed: %v", err)
	}
	if err := resp.Body.Close(); err != nil {
		t.Errorf("Close failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	// システム停止
	err = profiler.Stop()
	if err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	if profiler.running.Load() {
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

	// 呼び出している goroutine 自身が実行中なので、少なくとも1になる
	if info.Running < 1 {
		t.Errorf("Running = %d, want >= 1", info.Running)
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
	profiler.running.Store(true)

	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()

	profiler.healthHandler(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	var response map[string]any
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

	var response map[string]any
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
		Metrics: map[string]any{
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

	filename := filepath.Join(t.TempDir(), "test_cpu_profile.out")

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

	filename := filepath.Join(t.TempDir(), "test_mem_profile.out")

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

	if alerts := profiler.checkPerformanceAlerts(normalData); len(alerts) != 0 {
		t.Errorf("Expected no alerts, got %+v", alerts)
	}

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

	if alerts := profiler.checkPerformanceAlerts(alertData); len(alerts) != 2 {
		t.Errorf("Expected 2 alerts (memory, goroutine), got %+v", alerts)
	}
}

func BenchmarkCollectProfiles(b *testing.B) {
	profiler := NewProfilingSystem(8094, 30*time.Second, 24*time.Hour)

	for b.Loop() {
		profiler.collectProfiles()
	}
}

func BenchmarkCollectGoroutineInfo(b *testing.B) {
	profiler := NewProfilingSystem(8095, 30*time.Second, 24*time.Hour)

	for b.Loop() {
		info := profiler.collectGoroutineInfo()
		if info == nil {
			b.Fatal("collectGoroutineInfo returned nil")
		}
	}
}

// TestStartStopConcurrentWithHandlers 開始・停止とハンドラーを並行させても panic せず、二重の停止も安全なことを確かめる
func TestStartStopConcurrentWithHandlers(t *testing.T) {
	for range 50 {
		profiler := NewProfilingSystem(0, 10*time.Millisecond, time.Hour)
		if err := profiler.StartProfiling(t.Context()); err != nil {
			t.Fatalf("StartProfiling failed: %v", err)
		}

		var wg sync.WaitGroup
		wg.Go(func() {
			profiler.healthHandler(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/health", nil))
		})
		wg.Go(func() {
			profiler.profilesHandler(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/profiles", nil))
		})
		if err := profiler.Stop(); err != nil {
			t.Fatalf("Stop failed: %v", err)
		}
		wg.Wait()
		if err := profiler.Stop(); err != nil {
			t.Fatalf("二度目の Stop がエラーを返した: %v", err)
		}
	}
}

// TestStartProfilingReportsListenError ポートが使用中なら StartProfiling がエラーを返すことを確かめる
func TestStartProfilingReportsListenError(t *testing.T) {
	// サーバーと同じアドレスで塞ぐ。macOS では ":0" で塞いでも 127.0.0.1 の同じポートを bind できてしまう
	busy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen failed: %v", err)
	}
	defer func() {
		if err := busy.Close(); err != nil {
			t.Errorf("Close failed: %v", err)
		}
	}()
	port := busy.Addr().(*net.TCPAddr).Port

	profiler := NewProfilingSystem(port, time.Hour, time.Hour)
	err = profiler.StartProfiling(t.Context())
	if stopErr := profiler.Stop(); stopErr != nil {
		t.Errorf("Stop failed: %v", stopErr)
	}
	if err == nil {
		t.Fatal("使用中のポートでも StartProfiling がエラーを返さない")
	}
}

// TestProfilesHandlerReturnsLatestTen /profiles が新しい順に最大10件を返すことを確かめる
func TestProfilesHandlerReturnsLatestTen(t *testing.T) {
	profiler := NewProfilingSystem(0, time.Hour, time.Hour)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := range 15 {
		ts := base.Add(time.Duration(i) * time.Second)
		profiler.profiles[ts.Format(time.RFC3339)] = &ProfileData{Timestamp: ts}
	}

	w := httptest.NewRecorder()
	profiler.profilesHandler(w, httptest.NewRequest(http.MethodGet, "/profiles", nil))

	var response []*ProfileData
	if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
		t.Fatalf("Decode failed: %v", err)
	}
	if len(response) != 10 {
		t.Fatalf("Expected 10 profiles, got %d", len(response))
	}
	for i, profile := range response {
		want := base.Add(time.Duration(14-i) * time.Second)
		if !profile.Timestamp.Equal(want) {
			t.Fatalf("response[%d] = %v, want %v", i, profile.Timestamp, want)
		}
	}
}

// TestProfileCollectionLoopCollectsPeriodically 収集ループが間隔ごとにプロファイルを記録することを確かめる
func TestProfileCollectionLoopCollectsPeriodically(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		profiler := NewProfilingSystem(0, time.Second, time.Hour)
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan struct{})
		go func() {
			profiler.profileCollectionLoop(ctx)
			close(done)
		}()

		time.Sleep(3500 * time.Millisecond)
		synctest.Wait()

		profiler.mutex.RLock()
		got := len(profiler.profiles)
		profiler.mutex.RUnlock()
		if got != 3 {
			t.Errorf("Expected 3 profiles after 3.5s, got %d", got)
		}

		cancel()
		<-done
	})
}

// TestCollectGoroutineInfoReflectsStates goroutine の状態別の数が実際の状態を反映することを確かめる
func TestCollectGoroutineInfoReflectsStates(t *testing.T) {
	profiler := NewProfilingSystem(0, time.Hour, time.Hour)

	const blocked = 200
	release := make(chan struct{})
	var wg sync.WaitGroup
	for range blocked {
		wg.Go(func() { <-release })
	}
	defer func() {
		close(release)
		wg.Wait()
	}()
	// 全員がチャネル待ちに入るまで待つ（メトリクスは近似値なので、条件を満たすまで読み直す）
	deadline := time.Now().Add(5 * time.Second)
	info := profiler.collectGoroutineInfo()
	for info.Waiting < blocked && time.Now().Before(deadline) {
		runtime.Gosched()
		info = profiler.collectGoroutineInfo()
	}
	if info.Waiting < blocked {
		t.Errorf("Waiting = %d, want >= %d", info.Waiting, blocked)
	}
	if info.Running > runtime.GOMAXPROCS(0) {
		t.Errorf("Running = %d は GOMAXPROCS(%d) を超えない", info.Running, runtime.GOMAXPROCS(0))
	}
}

// pprof はメモリの中身やスタックを返すので、同じマシンからしか届かないようにする
func TestStartProfiling_ListensOnLoopbackOnly(t *testing.T) {
	profiler := NewProfilingSystem(0, 1*time.Second, 1*time.Hour)
	if err := profiler.StartProfiling(t.Context()); err != nil {
		t.Fatalf("StartProfiling failed: %v", err)
	}
	t.Cleanup(func() {
		if err := profiler.Stop(); err != nil {
			t.Errorf("Stop failed: %v", err)
		}
	})

	host, _, err := net.SplitHostPort(profiler.listenAddr())
	if err != nil {
		t.Fatalf("SplitHostPort failed: %v", err)
	}
	if ip := net.ParseIP(host); ip == nil || !ip.IsLoopback() {
		t.Errorf("待ち受けアドレス = %q, 期待値 ループバックアドレス", host)
	}
}
