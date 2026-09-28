package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// waitFor 条件が満たされるまで待ちます
// Start は実際のポートで HTTP サーバーを起動するので synctest のバブルでは動かせない。固定の Sleep の代わりに条件を確かめながら待つ。
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// MockMetricsCollector テスト用のメトリクス収集器です
type MockMetricsCollector struct {
	name        string
	interval    time.Duration
	metrics     []Metric
	shouldError bool
}

func NewMockMetricsCollector(name string, interval time.Duration) *MockMetricsCollector {
	return &MockMetricsCollector{
		name:     name,
		interval: interval,
		metrics:  make([]Metric, 0),
	}
}

func (mmc *MockMetricsCollector) Name() string {
	return mmc.name
}

func (mmc *MockMetricsCollector) Interval() time.Duration {
	return mmc.interval
}

func (mmc *MockMetricsCollector) Collect(ctx context.Context) ([]Metric, error) {
	if mmc.shouldError {
		return nil, context.DeadlineExceeded
	}

	now := time.Now()
	metrics := []Metric{
		{
			Name:      mmc.name + ".value",
			Value:     float64(len(mmc.metrics)),
			Timestamp: now,
			Tags:      map[string]string{"source": mmc.name},
		},
		{
			Name:      mmc.name + ".counter",
			Value:     float64(time.Now().Unix() % 100),
			Timestamp: now,
		},
	}

	mmc.metrics = append(mmc.metrics, metrics...)
	return metrics, nil
}

func (mmc *MockMetricsCollector) SetShouldError(shouldError bool) {
	mmc.shouldError = shouldError
}

func TestTimeSeriesStore_StoreAndQuery(t *testing.T) {
	store := NewTimeSeriesStore(100, time.Hour)

	now := time.Now()
	metrics := []Metric{
		{
			Name:      "test.metric",
			Value:     42.0,
			Timestamp: now,
			Tags:      map[string]string{"env": "test"},
		},
		{
			Name:      "test.metric",
			Value:     43.0,
			Timestamp: now.Add(time.Minute),
			Tags:      map[string]string{"env": "test"},
		},
	}

	store.Store(metrics)

	// クエリテスト
	start := now.Add(-time.Hour)
	end := now.Add(time.Hour)

	results := store.Query("test.metric", map[string]string{"env": "test"}, start, end)

	if len(results) != 1 {
		t.Errorf("Expected 1 series, got %d", len(results))
	}

	if len(results[0].Points) != 2 {
		t.Errorf("Expected 2 points, got %d", len(results[0].Points))
	}

	if results[0].Points[0].Value != 42.0 {
		t.Errorf("Expected first value 42.0, got %f", results[0].Points[0].Value)
	}

	if results[0].Points[1].Value != 43.0 {
		t.Errorf("Expected second value 43.0, got %f", results[0].Points[1].Value)
	}
}

func TestTimeSeriesStore_GetAllMetrics(t *testing.T) {
	store := NewTimeSeriesStore(100, time.Hour)

	metrics := []Metric{
		{Name: "metric.a", Value: 1.0, Timestamp: time.Now()},
		{Name: "metric.b", Value: 2.0, Timestamp: time.Now()},
		{Name: "metric.a", Value: 3.0, Timestamp: time.Now()},
	}

	store.Store(metrics)

	allMetrics := store.GetAllMetrics()

	if len(allMetrics) != 2 {
		t.Errorf("Expected 2 unique metrics, got %d", len(allMetrics))
	}

	// ソートされているかチェック
	if allMetrics[0] != "metric.a" || allMetrics[1] != "metric.b" {
		t.Errorf("Expected sorted metrics [metric.a, metric.b], got %v", allMetrics)
	}
}

func TestTimeSeriesStore_DataRetention(t *testing.T) {
	store := NewTimeSeriesStore(2, 2*time.Hour)

	now := time.Now()
	metrics := []Metric{
		{Name: "test", Value: 1.0, Timestamp: now.Add(-time.Hour)},   // 古い
		{Name: "test", Value: 2.0, Timestamp: now.Add(-time.Minute)}, // 最近
		{Name: "test", Value: 3.0, Timestamp: now},                   // 最新
	}

	store.Store(metrics)

	// データポイント制限のテスト（最大2個）
	results := store.Query("test", nil, now.Add(-2*time.Hour), now.Add(time.Hour))
	if len(results) != 1 {
		t.Errorf("Expected 1 series, got %d", len(results))
	}

	if len(results[0].Points) != 2 {
		t.Errorf("Expected 2 points due to limit, got %d", len(results[0].Points))
	}

	// 最新の2つが残っているはず
	if results[0].Points[0].Value != 2.0 || results[0].Points[1].Value != 3.0 {
		t.Error("Expected latest 2 points to be retained")
	}
}

func TestMonitoringDashboard_BasicOperation(t *testing.T) {
	dashboard := NewMonitoringDashboard(100*time.Millisecond, time.Hour, 100)

	mockCollector := NewMockMetricsCollector("test_collector", 100*time.Millisecond)
	dashboard.AddCollector(mockCollector)

	err := dashboard.Start(0) // ポート0で自動割り当て
	if err != nil {
		t.Fatalf("Failed to start dashboard: %v", err)
	}
	defer func() {
		if err := dashboard.Stop(); err != nil {
			t.Errorf("Failed to stop dashboard: %v", err)
		}
	}()

	// メトリクスが収集されるまで待機
	waitFor(t, "metrics to be collected", func() bool {
		return len(dashboard.dataStore.GetAllMetrics()) > 0
	})

	want := []string{"test_collector.counter", "test_collector.value"}
	if got := dashboard.dataStore.GetAllMetrics(); len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("Expected metrics %v, got %v", want, got)
	}
}

func TestMonitoringDashboard_CollectorError(t *testing.T) {
	dashboard := NewMonitoringDashboard(50*time.Millisecond, time.Hour, 100)

	mockCollector := NewMockMetricsCollector("error_collector", 50*time.Millisecond)
	mockCollector.SetShouldError(true)
	dashboard.AddCollector(mockCollector)

	err := dashboard.Start(0)
	if err != nil {
		t.Fatalf("Failed to start dashboard: %v", err)
	}
	defer func() {
		if err := dashboard.Stop(); err != nil {
			t.Errorf("Failed to stop dashboard: %v", err)
		}
	}()

	// エラーが発生しても収集ループが続くことを確認
	waitFor(t, "repeated collections", func() bool {
		return dashboard.totalCollections.Load() >= 3
	})

	if !dashboard.isRunning.Load() {
		t.Error("Expected dashboard to keep running after collector errors")
	}
	if got := dashboard.totalMetrics.Load(); got != 0 {
		t.Errorf("Expected no metrics from a failing collector, got %d", got)
	}
}

func TestMonitoringDashboard_AlertEvaluation(t *testing.T) {
	dashboard := NewMonitoringDashboard(50*time.Millisecond, time.Hour, 100)
	dashboard.alertInterval = 50 * time.Millisecond

	// アラートルールを追加
	rule := AlertRule{
		Name:        "test_alert",
		MetricName:  "test.value",
		Condition:   "gt",
		Threshold:   50.0,
		Duration:    time.Minute,
		Severity:    "warning",
		Enabled:     true,
		Description: "Test alert",
	}
	dashboard.AddAlertRule(rule)

	err := dashboard.Start(0)
	if err != nil {
		t.Fatalf("Failed to start dashboard: %v", err)
	}
	defer func() {
		if err := dashboard.Stop(); err != nil {
			t.Errorf("Failed to stop dashboard: %v", err)
		}
	}()

	// 閾値を超えるメトリクスを追加
	metrics := []Metric{
		{
			Name:      "test.value",
			Value:     100.0, // 閾値50を超える
			Timestamp: time.Now(),
		},
	}
	dashboard.dataStore.Store(metrics)

	// アラートが発火するまで待機
	waitFor(t, "alert to fire", func() bool {
		return dashboard.totalAlerts.Load() >= 1
	})

	// 同じルールは 5 分間は再発火しない
	time.Sleep(200 * time.Millisecond)
	if got := dashboard.totalAlerts.Load(); got != 1 {
		t.Errorf("Expected the alert to fire once, got %d", got)
	}
}

func TestMonitoringDashboard_HTTPEndpoints(t *testing.T) {
	dashboard := NewMonitoringDashboard(time.Hour, time.Hour, 100)

	// テストデータを準備
	metrics := []Metric{
		{Name: "http.test", Value: 42.0, Timestamp: time.Now()},
	}
	dashboard.dataStore.Store(metrics)

	rule := AlertRule{
		Name:        "test_rule",
		MetricName:  "http.test",
		Condition:   "gt",
		Threshold:   10.0,
		Enabled:     true,
		Description: "Test rule",
	}
	dashboard.AddAlertRule(rule)

	// HTTPハンドラーをテスト
	mux := http.NewServeMux()
	dashboard.setupRoutes(mux)

	// /api/metrics エンドポイントをテスト
	req := httptest.NewRequest("GET", "/api/metrics", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	var response map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Errorf("Failed to parse response: %v", err)
	}

	if response["count"].(float64) != 1 {
		t.Errorf("Expected 1 metric, got %v", response["count"])
	}

	// /api/health エンドポイントをテスト
	req = httptest.NewRequest("GET", "/api/health", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200 for health, got %d", w.Code)
	}

	// /api/query エンドポイントをテスト
	req = httptest.NewRequest("GET", "/api/query?metric=http.test", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200 for query, got %d", w.Code)
	}

	// /api/alerts エンドポイントをテスト
	req = httptest.NewRequest("GET", "/api/alerts", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200 for alerts, got %d", w.Code)
	}

	// /api/stats エンドポイントをテスト。稼働時間は開始時刻からの経過時間になる
	dashboard.mutex.Lock()
	dashboard.startedAt = time.Now().Add(-time.Hour)
	dashboard.mutex.Unlock()

	req = httptest.NewRequest("GET", "/api/stats", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200 for stats, got %d", w.Code)
	}

	var stats map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &stats); err != nil {
		t.Fatalf("Failed to parse stats: %v", err)
	}
	if uptime := time.Duration(stats["uptime"].(float64)); uptime < time.Hour {
		t.Errorf("Expected uptime of at least 1h, got %v", uptime)
	}
}

func TestMonitoringDashboard_QueryWithTimeRange(t *testing.T) {
	dashboard := NewMonitoringDashboard(time.Hour, time.Hour, 100)

	now := time.Now()
	metrics := []Metric{
		{Name: "time.test", Value: 1.0, Timestamp: now.Add(-2 * time.Hour)},
		{Name: "time.test", Value: 2.0, Timestamp: now.Add(-1 * time.Hour)},
		{Name: "time.test", Value: 3.0, Timestamp: now},
	}
	dashboard.dataStore.Store(metrics)

	mux := http.NewServeMux()
	dashboard.setupRoutes(mux)

	// 時間範囲指定でのクエリ
	start := now.Add(-90 * time.Minute).Format(time.RFC3339)
	end := now.Add(30 * time.Minute).Format(time.RFC3339)

	url := "/api/query?metric=time.test&start=" + start + "&end=" + end
	req := httptest.NewRequest("GET", url, nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	var response map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Errorf("Failed to parse response: %v", err)
	}

	series := response["series"].([]any)
	if len(series) == 0 {
		t.Error("Expected time series data")
	}
}

func TestMonitoringDashboard_StartStop(t *testing.T) {
	dashboard := NewMonitoringDashboard(time.Hour, time.Hour, 100)

	// 最初は停止状態
	err := dashboard.Stop()
	if err == nil {
		t.Error("Expected error when stopping non-running dashboard")
	}

	// 開始
	err = dashboard.Start(0)
	if err != nil {
		t.Fatalf("Failed to start dashboard: %v", err)
	}

	// 重複開始はエラー
	err = dashboard.Start(0)
	if err == nil {
		t.Error("Expected error when starting already running dashboard")
	}

	// 停止
	err = dashboard.Stop()
	if err != nil {
		t.Errorf("Failed to stop dashboard: %v", err)
	}

	// 重複停止はエラー
	err = dashboard.Stop()
	if err == nil {
		t.Error("Expected error when stopping already stopped dashboard")
	}

	// 停止後の再開始はループが動かないのでエラー
	if err := dashboard.Start(0); err == nil {
		t.Error("Expected error when starting a stopped dashboard")
	}
}

func TestMonitoringDashboard_AlertsEndpointDuringFire(t *testing.T) {
	dashboard := NewMonitoringDashboard(time.Hour, time.Hour, 100)
	mux := http.NewServeMux()
	dashboard.setupRoutes(mux)

	// アラート発火と /api/alerts の JSON 化が同時に走っても、activeAlerts の読み書きが競合しない
	var wg sync.WaitGroup
	wg.Go(func() {
		for i := range 100 {
			dashboard.fireAlert(AlertRule{Name: fmt.Sprintf("rule-%d", i), MetricName: "m"}, 1)
		}
	})
	wg.Go(func() {
		for range 100 {
			mux.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/api/alerts", nil))
		}
	})
	wg.Wait()

	if got := dashboard.totalAlerts.Load(); got != 100 {
		t.Errorf("Expected 100 alerts, got %d", got)
	}
}

func TestMonitoringDashboard_ConcurrentStartStop(t *testing.T) {
	for range 50 {
		dashboard := NewMonitoringDashboard(time.Millisecond, time.Hour, 100)
		dashboard.AddCollector(NewMockMetricsCollector("c", time.Millisecond))

		var wg sync.WaitGroup
		wg.Go(func() { _ = dashboard.Start(0) })
		wg.Go(func() { _ = dashboard.Stop() })
		wg.Wait()
		_ = dashboard.Stop()

		if err := dashboard.Start(0); err == nil {
			t.Fatal("Expected error when starting after stop")
		}
		if dashboard.isRunning.Load() {
			t.Fatal("Expected dashboard to stay stopped")
		}
	}
}

func TestSystemMetricsCollector(t *testing.T) {
	collector := NewSystemMetricsCollector(time.Second)

	if collector.Name() != "system_metrics" {
		t.Errorf("Expected name 'system_metrics', got %s", collector.Name())
	}

	if collector.Interval() != time.Second {
		t.Errorf("Expected interval 1s, got %v", collector.Interval())
	}

	metrics, err := collector.Collect(t.Context())

	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	if len(metrics) == 0 {
		t.Error("Expected some metrics to be collected")
	}

	// 特定のメトリクスが含まれているかチェック
	metricNames := make(map[string]bool)
	for _, metric := range metrics {
		metricNames[metric.Name] = true
	}

	expectedMetrics := []string{
		"system.memory.heap_alloc",
		"system.memory.heap_sys",
		"system.memory.heap_objects",
		"system.goroutines",
		"system.cpu.goroutines_per_cpu",
	}

	for _, expected := range expectedMetrics {
		if !metricNames[expected] {
			t.Errorf("Expected metric %s not found", expected)
		}
	}

	// メトリクスの値が合理的かチェック
	for _, metric := range metrics {
		if metric.Value < 0 {
			t.Errorf("Metric %s has negative value: %f", metric.Name, metric.Value)
		}

		if metric.Timestamp.IsZero() {
			t.Errorf("Metric %s has zero timestamp", metric.Name)
		}
	}
}

func TestAlertRuleConditions(t *testing.T) {
	dashboard := NewMonitoringDashboard(time.Hour, time.Hour, 100)

	testCases := []struct {
		condition string
		value     float64
		threshold float64
		expected  bool
	}{
		{"gt", 10.0, 5.0, true},
		{"gt", 5.0, 10.0, false},
		{"gte", 10.0, 10.0, true},
		{"gte", 9.0, 10.0, false},
		{"lt", 5.0, 10.0, true},
		{"lt", 10.0, 5.0, false},
		{"lte", 10.0, 10.0, true},
		{"lte", 11.0, 10.0, false},
		{"eq", 10.0, 10.0, true},
		{"eq", 10.0, 5.0, false},
		{"invalid", 10.0, 5.0, false},
	}

	for _, tc := range testCases {
		result := dashboard.checkCondition(tc.condition, tc.value, tc.threshold)
		if result != tc.expected {
			t.Errorf("Condition %s with value %f and threshold %f: expected %v, got %v",
				tc.condition, tc.value, tc.threshold, tc.expected, result)
		}
	}
}

// ベンチマークテスト
func BenchmarkTimeSeriesStore_Store(b *testing.B) {
	store := NewTimeSeriesStore(1000, time.Hour)

	metrics := []Metric{
		{Name: "bench.metric", Value: 42.0, Timestamp: time.Now()},
	}

	for b.Loop() {
		store.Store(metrics)
	}
}

func BenchmarkTimeSeriesStore_Query(b *testing.B) {
	store := NewTimeSeriesStore(1000, time.Hour)

	// テストデータを準備
	now := time.Now()
	for i := range 100 {
		metrics := []Metric{
			{Name: "bench.query", Value: float64(i), Timestamp: now.Add(time.Duration(i) * time.Second)},
		}
		store.Store(metrics)
	}

	start := now.Add(-time.Hour)
	end := now.Add(time.Hour)

	for b.Loop() {
		store.Query("bench.query", nil, start, end)
	}
}

func BenchmarkSystemMetricsCollector_Collect(b *testing.B) {
	collector := NewSystemMetricsCollector(time.Second)
	ctx := b.Context()

	for b.Loop() {
		if _, err := collector.Collect(ctx); err != nil {
			b.Errorf("Failed to collect metrics: %v", err)
		}
	}
}
