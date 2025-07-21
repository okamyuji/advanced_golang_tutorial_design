package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Metric 単一のメトリクスデータポイントを表します
type Metric struct {
	Name      string                 `json:"name"`
	Value     float64                `json:"value"`
	Timestamp time.Time              `json:"timestamp"`
	Tags      map[string]string      `json:"tags,omitempty"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
}

// TimeSeriesPoint 時系列データポイントを表します
type TimeSeriesPoint struct {
	Timestamp time.Time `json:"timestamp"`
	Value     float64   `json:"value"`
}

// TimeSeries 時系列データを表します
type TimeSeries struct {
	Name       string                 `json:"name"`
	Points     []TimeSeriesPoint      `json:"points"`
	Tags       map[string]string      `json:"tags,omitempty"`
	Metadata   map[string]interface{} `json:"metadata,omitempty"`
	LastUpdate time.Time              `json:"last_update"`
}

// MetricsCollector メトリクス収集インターフェースです
type MetricsCollector interface {
	Collect(ctx context.Context) ([]Metric, error)
	Name() string
	Interval() time.Duration
}

// AlertRule アラートルールを表します
type AlertRule struct {
	Name        string                 `json:"name"`
	MetricName  string                 `json:"metric_name"`
	Condition   string                 `json:"condition"` // "gt", "lt", "eq", "gte", "lte"
	Threshold   float64                `json:"threshold"`
	Duration    time.Duration          `json:"duration"`
	Severity    string                 `json:"severity"`
	Enabled     bool                   `json:"enabled"`
	LastFired   time.Time              `json:"last_fired"`
	Description string                 `json:"description"`
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
}

// TimeSeriesStore 時系列データを格納します
type TimeSeriesStore struct {
	series          map[string]*TimeSeries
	maxDataPoints   int
	retentionPeriod time.Duration
	mutex           sync.RWMutex
}

// NewTimeSeriesStore 新しい時系列ストアを作成します
func NewTimeSeriesStore(maxDataPoints int, retentionPeriod time.Duration) *TimeSeriesStore {
	return &TimeSeriesStore{
		series:          make(map[string]*TimeSeries),
		maxDataPoints:   maxDataPoints,
		retentionPeriod: retentionPeriod,
	}
}

// Store メトリクスを格納します
func (tss *TimeSeriesStore) Store(metrics []Metric) {
	tss.mutex.Lock()
	defer tss.mutex.Unlock()

	// メトリクスをシリーズ別にグループ化
	seriesMap := make(map[string][]Metric)
	for _, metric := range metrics {
		seriesKey := tss.getSeriesKey(metric.Name, metric.Tags)
		seriesMap[seriesKey] = append(seriesMap[seriesKey], metric)
	}

	// 各シリーズに対してデータを追加
	for seriesKey, seriesMetrics := range seriesMap {
		if tss.series[seriesKey] == nil {
			tss.series[seriesKey] = &TimeSeries{
				Name:     seriesMetrics[0].Name,
				Points:   make([]TimeSeriesPoint, 0),
				Tags:     seriesMetrics[0].Tags,
				Metadata: seriesMetrics[0].Metadata,
			}
		}

		series := tss.series[seriesKey]

		// 新しいポイントを追加
		for _, metric := range seriesMetrics {
			series.Points = append(series.Points, TimeSeriesPoint{
				Timestamp: metric.Timestamp,
				Value:     metric.Value,
			})
		}

		// 時間順にソート
		sort.Slice(series.Points, func(i, j int) bool {
			return series.Points[i].Timestamp.Before(series.Points[j].Timestamp)
		})

		// データポイント数制限（最新のポイントを保持）
		if len(series.Points) > tss.maxDataPoints {
			series.Points = series.Points[len(series.Points)-tss.maxDataPoints:]
		}

		series.LastUpdate = time.Now()
	}

	// 古いデータを削除
	tss.cleanupOldData()
}

// Query 時系列データを検索します
func (tss *TimeSeriesStore) Query(metricName string, tags map[string]string, start, end time.Time) []TimeSeries {
	tss.mutex.RLock()
	defer tss.mutex.RUnlock()

	var result []TimeSeries

	for _, series := range tss.series {
		if series.Name != metricName {
			continue
		}

		// タグマッチング
		if !tss.matchTags(series.Tags, tags) {
			continue
		}

		// 時間範囲でフィルタ
		filteredPoints := make([]TimeSeriesPoint, 0)
		for _, point := range series.Points {
			if (point.Timestamp.After(start) || point.Timestamp.Equal(start)) &&
				(point.Timestamp.Before(end) || point.Timestamp.Equal(end)) {
				filteredPoints = append(filteredPoints, point)
			}
		}

		if len(filteredPoints) > 0 {
			result = append(result, TimeSeries{
				Name:       series.Name,
				Points:     filteredPoints,
				Tags:       series.Tags,
				Metadata:   series.Metadata,
				LastUpdate: series.LastUpdate,
			})
		}
	}

	return result
}

// GetAllMetrics 全メトリクス名を取得します
func (tss *TimeSeriesStore) GetAllMetrics() []string {
	tss.mutex.RLock()
	defer tss.mutex.RUnlock()

	metricSet := make(map[string]bool)
	for _, series := range tss.series {
		metricSet[series.Name] = true
	}

	var metrics []string
	for metric := range metricSet {
		metrics = append(metrics, metric)
	}

	sort.Strings(metrics)
	return metrics
}

// getSeriesKey シリーズキーを生成します
func (tss *TimeSeriesStore) getSeriesKey(name string, tags map[string]string) string {
	key := name
	if tags != nil {
		var tagPairs []string
		for k, v := range tags {
			tagPairs = append(tagPairs, fmt.Sprintf("%s=%s", k, v))
		}
		sort.Strings(tagPairs)
		for _, pair := range tagPairs {
			key += "|" + pair
		}
	}
	return key
}

// matchTags タグがマッチするかチェックします
func (tss *TimeSeriesStore) matchTags(seriesTags, queryTags map[string]string) bool {
	if queryTags == nil {
		return true
	}

	for k, v := range queryTags {
		if seriesTags[k] != v {
			return false
		}
	}
	return true
}

// cleanupOldData 古いデータを削除します
func (tss *TimeSeriesStore) cleanupOldData() {
	cutoff := time.Now().Add(-tss.retentionPeriod)

	for _, series := range tss.series {
		var validPoints []TimeSeriesPoint
		for _, point := range series.Points {
			if point.Timestamp.After(cutoff) {
				validPoints = append(validPoints, point)
			}
		}
		series.Points = validPoints
	}
}

// MonitoringDashboard リアルタイム監視ダッシュボードです
type MonitoringDashboard struct {
	metricsCollectors map[string]MetricsCollector
	alertRules        []AlertRule
	dataStore         *TimeSeriesStore
	webServer         *http.Server

	updateInterval  time.Duration
	retentionPeriod time.Duration
	maxDataPoints   int

	ctx              context.Context
	cancel           context.CancelFunc
	isRunning        int64
	totalCollections int64
	totalMetrics     int64
	totalAlerts      int64

	mutex        sync.RWMutex
	activeAlerts map[string]time.Time
}

// NewMonitoringDashboard 新しい監視ダッシュボードを作成します
func NewMonitoringDashboard(updateInterval, retentionPeriod time.Duration, maxDataPoints int) *MonitoringDashboard {
	ctx, cancel := context.WithCancel(context.Background())

	return &MonitoringDashboard{
		metricsCollectors: make(map[string]MetricsCollector),
		alertRules:        make([]AlertRule, 0),
		dataStore:         NewTimeSeriesStore(maxDataPoints, retentionPeriod),
		updateInterval:    updateInterval,
		retentionPeriod:   retentionPeriod,
		maxDataPoints:     maxDataPoints,
		ctx:               ctx,
		cancel:            cancel,
		activeAlerts:      make(map[string]time.Time),
	}
}

// AddCollector メトリクスコレクターを追加します
func (md *MonitoringDashboard) AddCollector(collector MetricsCollector) {
	md.metricsCollectors[collector.Name()] = collector
}

// AddAlertRule アラートルールを追加します
func (md *MonitoringDashboard) AddAlertRule(rule AlertRule) {
	md.mutex.Lock()
	defer md.mutex.Unlock()
	md.alertRules = append(md.alertRules, rule)
}

// Start 監視ダッシュボードを開始します
func (md *MonitoringDashboard) Start(port int) error {
	if !atomic.CompareAndSwapInt64(&md.isRunning, 0, 1) {
		return fmt.Errorf("monitoring dashboard is already running")
	}

	log.Println("Starting Monitoring Dashboard...")

	// HTTPサーバーを設定
	mux := http.NewServeMux()
	md.setupRoutes(mux)

	md.webServer = &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}

	// データ収集ループを開始
	go md.collectAndDistribute()

	// アラート評価ループを開始
	go md.evaluateAlerts()

	// HTTPサーバーを開始
	go func() {
		log.Printf("HTTP server starting on port %d", port)
		if err := md.webServer.ListenAndServe(); err != http.ErrServerClosed {
			log.Printf("HTTP server error: %v", err)
		}
	}()

	log.Println("Monitoring Dashboard started successfully")
	return nil
}

// Stop 監視ダッシュボードを停止します
func (md *MonitoringDashboard) Stop() error {
	if !atomic.CompareAndSwapInt64(&md.isRunning, 1, 0) {
		return fmt.Errorf("monitoring dashboard is not running")
	}

	log.Println("Stopping Monitoring Dashboard...")

	// コンテキストをキャンセル
	md.cancel()

	// HTTPサーバーを停止
	if md.webServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := md.webServer.Shutdown(ctx); err != nil {
			log.Printf("Error shutting down HTTP server: %v", err)
		}
	}

	log.Println("Monitoring Dashboard stopped")
	return nil
}

// collectAndDistribute メトリクス収集と配信を行います
func (md *MonitoringDashboard) collectAndDistribute() {
	ticker := time.NewTicker(md.updateInterval)
	defer ticker.Stop()

	for {
		select {
		case <-md.ctx.Done():
			return
		case <-ticker.C:
			md.performCollection()
		}
	}
}

// performCollection メトリクス収集を実行します
func (md *MonitoringDashboard) performCollection() {
	atomic.AddInt64(&md.totalCollections, 1)

	var wg sync.WaitGroup
	metricsChan := make(chan []Metric, len(md.metricsCollectors))

	for name, collector := range md.metricsCollectors {
		wg.Add(1)
		go func(name string, collector MetricsCollector) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					log.Printf("Metrics collector %s panicked: %v", name, r)
				}
			}()

			ctx, cancel := context.WithTimeout(md.ctx, 30*time.Second)
			defer cancel()

			metrics, err := collector.Collect(ctx)
			if err != nil {
				log.Printf("Failed to collect metrics from %s: %v", name, err)
				return
			}

			metricsChan <- metrics
		}(name, collector)
	}

	go func() {
		wg.Wait()
		close(metricsChan)
	}()

	md.aggregateAndStore(metricsChan)
}

// aggregateAndStore メトリクスを集約して保存します
func (md *MonitoringDashboard) aggregateAndStore(metricsChan chan []Metric) {
	var allMetrics []Metric

	for metrics := range metricsChan {
		allMetrics = append(allMetrics, metrics...)
	}

	if len(allMetrics) > 0 {
		atomic.AddInt64(&md.totalMetrics, int64(len(allMetrics)))
		md.dataStore.Store(allMetrics)
	}
}

// evaluateAlerts アラートルールを評価します
func (md *MonitoringDashboard) evaluateAlerts() {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-md.ctx.Done():
			return
		case <-ticker.C:
			md.checkAlertRules()
		}
	}
}

// checkAlertRules アラートルールをチェックします
func (md *MonitoringDashboard) checkAlertRules() {
	md.mutex.RLock()
	rules := make([]AlertRule, len(md.alertRules))
	copy(rules, md.alertRules)
	md.mutex.RUnlock()

	for _, rule := range rules {
		if !rule.Enabled {
			continue
		}

		md.evaluateRule(rule)
	}
}

// evaluateRule 単一のアラートルールを評価します
func (md *MonitoringDashboard) evaluateRule(rule AlertRule) {
	// 最近のメトリクスデータを取得
	end := time.Now()
	start := end.Add(-rule.Duration)

	series := md.dataStore.Query(rule.MetricName, nil, start, end)
	if len(series) == 0 {
		return
	}

	// 最新の値を取得
	latestValue := md.getLatestValue(series)
	if math.IsNaN(latestValue) {
		return
	}

	// 条件をチェック
	shouldAlert := md.checkCondition(rule.Condition, latestValue, rule.Threshold)

	if shouldAlert {
		md.fireAlert(rule, latestValue)
	}
}

// getLatestValue 最新の値を取得します
func (md *MonitoringDashboard) getLatestValue(series []TimeSeries) float64 {
	if len(series) == 0 {
		return math.NaN()
	}

	var latestTime time.Time
	var latestValue float64

	for _, s := range series {
		for _, point := range s.Points {
			if point.Timestamp.After(latestTime) {
				latestTime = point.Timestamp
				latestValue = point.Value
			}
		}
	}

	return latestValue
}

// checkCondition 条件をチェックします
func (md *MonitoringDashboard) checkCondition(condition string, value, threshold float64) bool {
	switch condition {
	case "gt":
		return value > threshold
	case "gte":
		return value >= threshold
	case "lt":
		return value < threshold
	case "lte":
		return value <= threshold
	case "eq":
		return value == threshold
	default:
		return false
	}
}

// fireAlert アラートを発火します
func (md *MonitoringDashboard) fireAlert(rule AlertRule, value float64) {
	// 重複アラートを防ぐ
	alertKey := fmt.Sprintf("%s_%s", rule.Name, rule.MetricName)
	if lastFired, exists := md.activeAlerts[alertKey]; exists {
		if time.Since(lastFired) < 5*time.Minute {
			return
		}
	}

	md.activeAlerts[alertKey] = time.Now()
	atomic.AddInt64(&md.totalAlerts, 1)

	log.Printf("ALERT: %s - %s (value: %.2f, threshold: %.2f)",
		rule.Severity, rule.Description, value, rule.Threshold)
}

// setupRoutes HTTPルートを設定します
func (md *MonitoringDashboard) setupRoutes(mux *http.ServeMux) {
	// メトリクス一覧API
	mux.HandleFunc("/api/metrics", md.handleMetrics)

	// 時系列データAPI
	mux.HandleFunc("/api/query", md.handleQuery)

	// アラートルールAPI
	mux.HandleFunc("/api/alerts", md.handleAlerts)

	// ダッシュボード統計API
	mux.HandleFunc("/api/stats", md.handleStats)

	// ヘルスチェックAPI
	mux.HandleFunc("/api/health", md.handleHealth)

	// 静的ダッシュボードページ
	mux.HandleFunc("/", md.handleDashboard)
}

// handleMetrics メトリクス一覧を返します
func (md *MonitoringDashboard) handleMetrics(w http.ResponseWriter, r *http.Request) {
	metrics := md.dataStore.GetAllMetrics()

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]interface{}{
		"metrics": metrics,
		"count":   len(metrics),
	}); err != nil {
		log.Printf("Failed to encode metrics: %v", err)
	}
}

// handleQuery 時系列データクエリを処理します
func (md *MonitoringDashboard) handleQuery(w http.ResponseWriter, r *http.Request) {
	metricName := r.URL.Query().Get("metric")
	if metricName == "" {
		http.Error(w, "metric parameter is required", http.StatusBadRequest)
		return
	}

	// 時間範囲のパース
	end := time.Now()
	start := end.Add(-time.Hour) // デフォルト1時間

	if startStr := r.URL.Query().Get("start"); startStr != "" {
		if parsed, err := time.Parse(time.RFC3339, startStr); err == nil {
			start = parsed
		}
	}

	if endStr := r.URL.Query().Get("end"); endStr != "" {
		if parsed, err := time.Parse(time.RFC3339, endStr); err == nil {
			end = parsed
		}
	}

	series := md.dataStore.Query(metricName, nil, start, end)

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]interface{}{
		"metric": metricName,
		"start":  start,
		"end":    end,
		"series": series,
		"count":  len(series),
	}); err != nil {
		log.Printf("Failed to encode query result: %v", err)
	}
}

// handleAlerts アラート情報を返します
func (md *MonitoringDashboard) handleAlerts(w http.ResponseWriter, r *http.Request) {
	md.mutex.RLock()
	rules := make([]AlertRule, len(md.alertRules))
	copy(rules, md.alertRules)
	md.mutex.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]interface{}{
		"rules":         rules,
		"active_alerts": md.activeAlerts,
		"total_alerts":  atomic.LoadInt64(&md.totalAlerts),
	}); err != nil {
		log.Printf("Failed to encode alerts: %v", err)
	}
}

// handleStats ダッシュボード統計を返します
func (md *MonitoringDashboard) handleStats(w http.ResponseWriter, r *http.Request) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	stats := map[string]interface{}{
		"is_running":        atomic.LoadInt64(&md.isRunning) == 1,
		"total_collections": atomic.LoadInt64(&md.totalCollections),
		"total_metrics":     atomic.LoadInt64(&md.totalMetrics),
		"total_alerts":      atomic.LoadInt64(&md.totalAlerts),
		"collectors":        len(md.metricsCollectors),
		"alert_rules":       len(md.alertRules),
		"uptime":            time.Since(time.Now()), // 簡略化
		"memory_usage":      m.HeapAlloc,
		"goroutines":        runtime.NumGoroutine(),
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(stats); err != nil {
		log.Printf("Failed to encode stats: %v", err)
	}
}

// handleHealth ヘルスチェックを処理します
func (md *MonitoringDashboard) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(map[string]string{
		"status": "healthy",
		"time":   time.Now().Format(time.RFC3339),
	}); err != nil {
		log.Printf("Failed to encode health response: %v", err)
	}
}

// handleDashboard ダッシュボードページを表示します
func (md *MonitoringDashboard) handleDashboard(w http.ResponseWriter, r *http.Request) {
	html := `
<!DOCTYPE html>
<html>
<head>
    <title>Monitoring Dashboard</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        .metric-card { border: 1px solid #ddd; padding: 15px; margin: 10px 0; border-radius: 5px; }
        .alert { background-color: #ffebee; border-left: 4px solid #f44336; }
        .healthy { background-color: #e8f5e8; border-left: 4px solid #4caf50; }
        .stats { display: flex; flex-wrap: wrap; }
        .stat-item { margin: 10px; padding: 10px; background: #f5f5f5; border-radius: 3px; }
    </style>
</head>
<body>
    <h1>Monitoring Dashboard</h1>
    
    <h2>System Statistics</h2>
    <div id="stats" class="stats"></div>
    
    <h2>Available Metrics</h2>
    <div id="metrics"></div>
    
    <h2>Alert Rules</h2>
    <div id="alerts"></div>
    
    <script>
        function fetchAndDisplay(endpoint, elementId, formatter) {
            fetch('/api/' + endpoint)
                .then(response => response.json())
                .then(data => {
                    document.getElementById(elementId).innerHTML = formatter(data);
                })
                .catch(error => {
                    document.getElementById(elementId).innerHTML = 'Error: ' + error;
                });
        }
        
        function formatStats(data) {
            return Object.entries(data).map(([key, value]) => 
                '<div class="stat-item"><strong>' + key + ':</strong> ' + value + '</div>'
            ).join('');
        }
        
        function formatMetrics(data) {
            return data.metrics.map(metric => 
                '<div class="metric-card healthy">' + metric + '</div>'
            ).join('');
        }
        
        function formatAlerts(data) {
            return data.rules.map(rule => 
                '<div class="metric-card ' + (rule.enabled ? 'alert' : 'healthy') + '">' +
                '<strong>' + rule.name + '</strong><br>' +
                'Metric: ' + rule.metric_name + '<br>' +
                'Condition: ' + rule.condition + ' ' + rule.threshold + '<br>' +
                'Severity: ' + rule.severity + '<br>' +
                'Enabled: ' + rule.enabled +
                '</div>'
            ).join('');
        }
        
        // 初期ロード
        fetchAndDisplay('stats', 'stats', formatStats);
        fetchAndDisplay('metrics', 'metrics', formatMetrics);
        fetchAndDisplay('alerts', 'alerts', formatAlerts);
        
        // 10秒ごとに更新
        setInterval(() => {
            fetchAndDisplay('stats', 'stats', formatStats);
        }, 10000);
    </script>
</body>
</html>
`
	w.Header().Set("Content-Type", "text/html")
	if _, err := w.Write([]byte(html)); err != nil {
		log.Printf("Failed to write HTML response: %v", err)
	}
}

// SystemMetricsCollector システムメトリクスを収集します
type SystemMetricsCollector struct {
	interval time.Duration
}

func NewSystemMetricsCollector(interval time.Duration) *SystemMetricsCollector {
	return &SystemMetricsCollector{interval: interval}
}

func (smc *SystemMetricsCollector) Name() string {
	return "system_metrics"
}

func (smc *SystemMetricsCollector) Interval() time.Duration {
	return smc.interval
}

func (smc *SystemMetricsCollector) Collect(ctx context.Context) ([]Metric, error) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	now := time.Now()

	metrics := []Metric{
		{
			Name:      "system.memory.heap_alloc",
			Value:     float64(m.HeapAlloc),
			Timestamp: now,
			Tags:      map[string]string{"unit": "bytes"},
		},
		{
			Name:      "system.memory.heap_sys",
			Value:     float64(m.HeapSys),
			Timestamp: now,
			Tags:      map[string]string{"unit": "bytes"},
		},
		{
			Name:      "system.memory.heap_objects",
			Value:     float64(m.HeapObjects),
			Timestamp: now,
			Tags:      map[string]string{"unit": "count"},
		},
		{
			Name:      "system.goroutines",
			Value:     float64(runtime.NumGoroutine()),
			Timestamp: now,
			Tags:      map[string]string{"unit": "count"},
		},
		{
			Name:      "system.cpu.goroutines_per_cpu",
			Value:     float64(runtime.NumGoroutine()) / float64(runtime.NumCPU()),
			Timestamp: now,
			Tags:      map[string]string{"unit": "ratio"},
		},
	}

	return metrics, nil
}

// 使用例とテスト用のmain関数
func main() {
	// 監視ダッシュボードを作成
	dashboard := NewMonitoringDashboard(
		5*time.Second, // 更新間隔
		24*time.Hour,  // データ保持期間
		1000,          // 最大データポイント数
	)

	// システムメトリクスコレクターを追加
	systemCollector := NewSystemMetricsCollector(5 * time.Second)
	dashboard.AddCollector(systemCollector)

	// アラートルールを追加
	dashboard.AddAlertRule(AlertRule{
		Name:        "high_memory_usage",
		MetricName:  "system.memory.heap_alloc",
		Condition:   "gt",
		Threshold:   100 * 1024 * 1024, // 100MB
		Duration:    1 * time.Minute,
		Severity:    "warning",
		Enabled:     true,
		Description: "Memory usage is too high",
	})

	dashboard.AddAlertRule(AlertRule{
		Name:        "too_many_goroutines",
		MetricName:  "system.goroutines",
		Condition:   "gt",
		Threshold:   100,
		Duration:    30 * time.Second,
		Severity:    "critical",
		Enabled:     true,
		Description: "Too many goroutines running",
	})

	// ダッシュボード開始
	if err := dashboard.Start(8080); err != nil {
		log.Fatalf("Failed to start dashboard: %v", err)
	}

	log.Println("=== Monitoring Dashboard Demo ===")
	log.Println("Dashboard: http://localhost:8080")
	log.Println("API endpoints:")
	log.Println("  - GET /api/metrics")
	log.Println("  - GET /api/query?metric=system.memory.heap_alloc")
	log.Println("  - GET /api/alerts")
	log.Println("  - GET /api/stats")
	log.Println("  - GET /api/health")
	log.Println("Press Ctrl+C to stop")

	// 負荷生成（テスト用）
	go func() {
		for {
			// メモリ使用量を増加させる
			data := make([]byte, 1024*1024) // 1MB
			_ = data
			time.Sleep(10 * time.Second)
		}
	}()

	// 1分間実行
	time.Sleep(60 * time.Second)

	if err := dashboard.Stop(); err != nil {
		log.Printf("Error stopping dashboard: %v", err)
	}

	log.Println("Demo completed")
}
