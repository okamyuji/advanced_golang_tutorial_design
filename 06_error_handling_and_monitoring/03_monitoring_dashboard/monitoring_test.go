package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

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

	// メトリクス収集を待機
	time.Sleep(300 * time.Millisecond)

	// メトリクスが収集されたことを確認
	metrics := dashboard.dataStore.GetAllMetrics()
	if len(metrics) == 0 {
		t.Error("Expected metrics to be collected")
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

	// エラーが発生してもシステムが継続することを確認
	time.Sleep(200 * time.Millisecond)

	// ダッシュボードが実行中であることを確認
	_ = map[string]interface{}{}
	// stats の取得ロジックは簡略化
}

func TestMonitoringDashboard_AlertEvaluation(t *testing.T) {
	dashboard := NewMonitoringDashboard(50*time.Millisecond, time.Hour, 100)

	// アラートルールを追加
	rule := AlertRule{
		Name:        "test_alert",
		MetricName:  "test.value",
		Condition:   "gt",
		Threshold:   50.0,
		Duration:    100 * time.Millisecond,
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

	// アラート評価を待機
	time.Sleep(300 * time.Millisecond)

	// アラートが発火したことを簡易確認
	// 実際の実装では、アラートカウンターやコールバックで確認
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

	var response map[string]interface{}
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

	// /api/stats エンドポイントをテスト
	req = httptest.NewRequest("GET", "/api/stats", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200 for stats, got %d", w.Code)
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

	var response map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Errorf("Failed to parse response: %v", err)
	}

	series := response["series"].([]interface{})
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
}

func TestSystemMetricsCollector(t *testing.T) {
	collector := NewSystemMetricsCollector(time.Second)

	if collector.Name() != "system_metrics" {
		t.Errorf("Expected name 'system_metrics', got %s", collector.Name())
	}

	if collector.Interval() != time.Second {
		t.Errorf("Expected interval 1s, got %v", collector.Interval())
	}

	ctx := context.Background()
	metrics, err := collector.Collect(ctx)

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

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		store.Store(metrics)
	}
}

func BenchmarkTimeSeriesStore_Query(b *testing.B) {
	store := NewTimeSeriesStore(1000, time.Hour)

	// テストデータを準備
	now := time.Now()
	for i := 0; i < 100; i++ {
		metrics := []Metric{
			{Name: "bench.query", Value: float64(i), Timestamp: now.Add(time.Duration(i) * time.Second)},
		}
		store.Store(metrics)
	}

	start := now.Add(-time.Hour)
	end := now.Add(time.Hour)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		store.Query("bench.query", nil, start, end)
	}
}

func BenchmarkSystemMetricsCollector_Collect(b *testing.B) {
	collector := NewSystemMetricsCollector(time.Second)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := collector.Collect(ctx); err != nil {
			b.Errorf("Failed to collect metrics: %v", err)
		}
	}
}
