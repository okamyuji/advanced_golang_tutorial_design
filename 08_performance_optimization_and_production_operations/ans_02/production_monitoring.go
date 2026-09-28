// 復習問題2: 本番環境監視システム
// 問題: 本番環境での並行プログラム監視システムを実装してください：
// - リアルタイムメトリクス収集（CPU、メモリ、Goroutine数）
// - 異常値検出とアラート機能
// - プロファイルデータの自動収集と分析
// - ダッシュボード機能によるリアルタイム表示

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

var (
	errAlreadyRunning = errors.New("監視システムは実行中です")
	errStopped        = errors.New("監視システムは停止済みです")
)

// ProductionMonitoringSystem 本番環境監視システム
type ProductionMonitoringSystem struct {
	// メトリクス収集
	metricsCollector *MetricsCollector
	anomalyDetector  *AnomalyDetector
	alertManager     *AlertManager
	profileAnalyzer  *ProfileAnalyzer
	dashboardServer  *DashboardServer

	// システム状態。stateMu は Start/Stop の多重呼び出しを排他する
	stateMu            sync.Mutex
	running            bool
	stopped            bool
	startTime          time.Time
	monitoringInterval time.Duration
	lastNumGC          uint32 // 前回の計測時点の GC 回数（収集ループだけが読み書きする）

	// 停止制御
	stopChan chan struct{}
	wg       sync.WaitGroup
}

// MetricsCollector メトリクス収集器
type MetricsCollector struct {
	currentMetrics  *SystemMetrics
	historicalData  []*SystemMetrics
	maxHistorySize  int
	mutex           sync.RWMutex
	collectionCount atomic.Int64
}

// SystemMetrics システムメトリクス
type SystemMetrics struct {
	Timestamp          time.Time `json:"timestamp"`
	CPUUsage           float64   `json:"cpu_usage"`
	MemoryUsage        uint64    `json:"memory_usage"`
	MemoryPercent      float64   `json:"memory_percent"`
	GoroutineCount     int       `json:"goroutine_count"`
	GCPauseTime        uint64    `json:"gc_pause_time"`
	HeapAlloc          uint64    `json:"heap_alloc"`
	HeapSys            uint64    `json:"heap_sys"`
	NumGC              uint32    `json:"num_gc"`
	NetworkConnections int       `json:"network_connections"`
	DiskIORate         float64   `json:"disk_io_rate"`
	LoadAverage        float64   `json:"load_average"`
}

// AnomalyDetector 異常値検出器
type AnomalyDetector struct {
	thresholds       *AlertThresholds
	detectionHistory []AnomalyEvent
	statisticalModel *StatisticalModel
	isLearning       bool
	learningPeriod   time.Duration
	mutex            sync.RWMutex
}

// AlertThresholds アラート閾値
type AlertThresholds struct {
	CPUUsageWarning     float64       `json:"cpu_usage_warning"`
	CPUUsageCritical    float64       `json:"cpu_usage_critical"`
	MemoryUsageWarning  float64       `json:"memory_usage_warning"`
	MemoryUsageCritical float64       `json:"memory_usage_critical"`
	GoroutineCountMax   int           `json:"goroutine_count_max"`
	GCPauseTimeMax      uint64        `json:"gc_pause_time_max"`
	ResponseTimeMax     time.Duration `json:"response_time_max"`
}

// AnomalyEvent 異常イベント
type AnomalyEvent struct {
	EventID    string         `json:"event_id"`
	Timestamp  time.Time      `json:"timestamp"`
	Type       string         `json:"type"`
	Severity   string         `json:"severity"`
	Message    string         `json:"message"`
	Metrics    *SystemMetrics `json:"metrics"`
	Context    map[string]any `json:"context"`
	Resolved   bool           `json:"resolved"`
	ResolvedAt *time.Time     `json:"resolved_at,omitempty"`
}

// StatisticalModel 統計モデル
type StatisticalModel struct {
	CPUMean         float64
	CPUStdDev       float64
	MemoryMean      float64
	MemoryStdDev    float64
	GoroutineMean   float64
	GoroutineStdDev float64
	SampleCount     int64
	LastUpdate      time.Time
}

// AlertManager アラート管理器
type AlertManager struct {
	activeAlerts      map[string]*Alert
	alertHistory      []*Alert
	notificationQueue chan *Alert
	escalationRules   *EscalationRules
	mutex             sync.RWMutex
}

// Alert アラート
type Alert struct {
	ID             string         `json:"id"`
	Timestamp      time.Time      `json:"timestamp"`
	Type           string         `json:"type"`
	Severity       string         `json:"severity"`
	Title          string         `json:"title"`
	Description    string         `json:"description"`
	Source         string         `json:"source"`
	Tags           []string       `json:"tags"`
	Metadata       map[string]any `json:"metadata"`
	Status         string         `json:"status"`
	Acknowledged   bool           `json:"acknowledged"`
	AcknowledgedBy string         `json:"acknowledged_by,omitempty"`
	AcknowledgedAt *time.Time     `json:"acknowledged_at,omitempty"`
	ResolvedAt     *time.Time     `json:"resolved_at,omitempty"`
}

// EscalationRules エスカレーションルール
type EscalationRules struct {
	Level1Duration time.Duration
	Level2Duration time.Duration
	Level3Duration time.Duration
	NotifyChannels []string
}

// ProfileAnalyzer プロファイル分析器
type ProfileAnalyzer struct {
	analysisResults     []*AnalysisResult
	autoAnalysisEnabled bool
	analysisInterval    time.Duration
	lastAnalysis        time.Time
	mutex               sync.RWMutex
}

// AnalysisResult 分析結果
type AnalysisResult struct {
	Timestamp       time.Time      `json:"timestamp"`
	AnalysisType    string         `json:"analysis_type"`
	Summary         string         `json:"summary"`
	Findings        []string       `json:"findings"`
	Recommendations []string       `json:"recommendations"`
	Severity        string         `json:"severity"`
	Data            map[string]any `json:"data"`
}

// DashboardServer ダッシュボードサーバー
type DashboardServer struct {
	httpServer    *http.Server
	wsConnections map[string]*WebSocketConnection
	updateChannel chan *DashboardUpdate
	mutex         sync.RWMutex
	system        *ProductionMonitoringSystem // API が返す状態の取得元
}

// WebSocketConnection WebSocket接続
type WebSocketConnection struct {
	ID           string
	LastActivity time.Time
	SendChannel  chan []byte
}

// DashboardUpdate ダッシュボード更新
type DashboardUpdate struct {
	Type      string    `json:"type"`
	Timestamp time.Time `json:"timestamp"`
	Data      any       `json:"data"`
}

// NewProductionMonitoringSystem 新しい本番環境監視システムを作成
func NewProductionMonitoringSystem() *ProductionMonitoringSystem {
	pms := &ProductionMonitoringSystem{
		metricsCollector:   NewMetricsCollector(1000),
		anomalyDetector:    NewAnomalyDetector(),
		alertManager:       NewAlertManager(),
		profileAnalyzer:    NewProfileAnalyzer(),
		dashboardServer:    NewDashboardServer(8080),
		monitoringInterval: 5 * time.Second,
		stopChan:           make(chan struct{}),
	}
	pms.dashboardServer.system = pms

	return pms
}

// NewMetricsCollector 新しいメトリクス収集器を作成
func NewMetricsCollector(maxHistory int) *MetricsCollector {
	return &MetricsCollector{
		historicalData: make([]*SystemMetrics, 0, maxHistory),
		maxHistorySize: maxHistory,
	}
}

// NewAnomalyDetector 新しい異常値検出器を作成
func NewAnomalyDetector() *AnomalyDetector {
	return &AnomalyDetector{
		thresholds: &AlertThresholds{
			CPUUsageWarning:     70.0,
			CPUUsageCritical:    90.0,
			MemoryUsageWarning:  80.0,
			MemoryUsageCritical: 95.0,
			GoroutineCountMax:   10000,
			GCPauseTimeMax:      100000000, // 100ms in nanoseconds
			ResponseTimeMax:     5 * time.Second,
		},
		detectionHistory: make([]AnomalyEvent, 0, 1000),
		statisticalModel: &StatisticalModel{},
		isLearning:       true,
		learningPeriod:   24 * time.Hour,
	}
}

// NewAlertManager 新しいアラート管理器を作成
func NewAlertManager() *AlertManager {
	return &AlertManager{
		activeAlerts:      make(map[string]*Alert),
		alertHistory:      make([]*Alert, 0, 1000),
		notificationQueue: make(chan *Alert, 100),
		escalationRules: &EscalationRules{
			Level1Duration: 5 * time.Minute,
			Level2Duration: 15 * time.Minute,
			Level3Duration: 30 * time.Minute,
			NotifyChannels: []string{"email", "slack", "pagerduty"},
		},
	}
}

// NewProfileAnalyzer 新しいプロファイル分析器を作成
func NewProfileAnalyzer() *ProfileAnalyzer {
	return &ProfileAnalyzer{
		analysisResults:     make([]*AnalysisResult, 0, 100),
		autoAnalysisEnabled: true,
		analysisInterval:    1 * time.Hour,
	}
}

// NewDashboardServer 新しいダッシュボードサーバーを作成
func NewDashboardServer(port int) *DashboardServer {
	mux := http.NewServeMux()

	ds := &DashboardServer{
		httpServer: &http.Server{
			Addr:    fmt.Sprintf(":%d", port),
			Handler: mux,
		},
		wsConnections: make(map[string]*WebSocketConnection),
		updateChannel: make(chan *DashboardUpdate, 1000),
	}

	// HTTPエンドポイント設定
	mux.HandleFunc("GET /", ds.dashboardHandler)
	mux.HandleFunc("GET /api/metrics", ds.metricsAPIHandler)
	mux.HandleFunc("GET /api/alerts", ds.alertsAPIHandler)
	mux.HandleFunc("GET /api/health", ds.healthAPIHandler)
	mux.HandleFunc("GET /api/analysis", ds.analysisAPIHandler)

	return ds
}

// Start 監視システム開始
func (pms *ProductionMonitoringSystem) Start(ctx context.Context) error {
	pms.stateMu.Lock()
	defer pms.stateMu.Unlock()
	// stopChan と http.Server は停止後に再利用できないので、停止後の再開は受け付けない
	if pms.stopped {
		return errStopped
	}
	if pms.running {
		return errAlreadyRunning
	}

	// ポートの確保はここで行い、失敗を呼び出し側に返す
	listener, err := net.Listen("tcp", pms.dashboardServer.httpServer.Addr)
	if err != nil {
		return fmt.Errorf("ダッシュボードサーバー起動エラー: %w", err)
	}
	pms.running = true
	pms.startTime = time.Now()
	// ポート0を指定した場合に確定したアドレスを残す
	pms.dashboardServer.httpServer.Addr = listener.Addr().String()

	fmt.Println("本番環境監視システム開始")

	// メトリクス収集開始
	pms.wg.Go(func() { pms.metricsCollectionLoop(ctx) })

	// 異常検出開始
	pms.wg.Go(func() { pms.anomalyDetectionLoop(ctx) })

	// アラート処理開始
	pms.wg.Go(func() { pms.alertProcessingLoop(ctx) })

	// プロファイル分析開始
	pms.wg.Go(func() { pms.profileAnalysisLoop(ctx) })

	// ダッシュボードサーバー開始
	fmt.Printf("ダッシュボードサーバー開始: %s\n", listener.Addr())
	pms.wg.Go(func() {
		if err := pms.dashboardServer.httpServer.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fmt.Printf("ダッシュボードサーバーエラー: %v\n", err)
		}
	})

	// ダッシュボード更新処理開始
	pms.wg.Go(func() { pms.dashboardUpdateLoop(ctx) })

	return nil
}

// Stop 監視システム停止。二度目以降の呼び出しや開始前の呼び出しは何もしない
func (pms *ProductionMonitoringSystem) Stop() error {
	pms.stateMu.Lock()
	defer pms.stateMu.Unlock()
	if !pms.running {
		return nil
	}
	pms.running = false
	pms.stopped = true

	fmt.Println("本番環境監視システム停止中...")
	close(pms.stopChan)

	// HTTPサーバー停止
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := pms.dashboardServer.httpServer.Shutdown(ctx); err != nil {
		log.Printf("Failed to shutdown HTTP server: %v", err)
	}

	// 全ゴルーチン停止待機
	pms.wg.Wait()

	fmt.Println("本番環境監視システム停止完了")
	return nil
}

// metricsCollectionLoop メトリクス収集ループ
func (pms *ProductionMonitoringSystem) metricsCollectionLoop(ctx context.Context) {
	tick := time.Tick(pms.monitoringInterval)

	for {
		select {
		case <-tick:
			metrics := pms.collectSystemMetrics()
			pms.metricsCollector.AddMetrics(metrics)

			// ダッシュボード更新通知
			update := &DashboardUpdate{
				Type:      "metrics",
				Timestamp: time.Now(),
				Data:      metrics,
			}
			select {
			case pms.dashboardServer.updateChannel <- update:
			default:
				// チャンネルが満杯の場合はスキップ
			}

		case <-ctx.Done():
			return
		case <-pms.stopChan:
			return
		}
	}
}

// collectSystemMetrics システムメトリクス収集
func (pms *ProductionMonitoringSystem) collectSystemMetrics() *SystemMetrics {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	return &SystemMetrics{
		Timestamp:          time.Now(),
		CPUUsage:           pms.calculateCPUUsage(),
		MemoryUsage:        memStats.Alloc,
		MemoryPercent:      float64(memStats.Alloc) / float64(memStats.Sys) * 100,
		GoroutineCount:     runtime.NumGoroutine(),
		GCPauseTime:        pms.newGCPauseMax(&memStats),
		HeapAlloc:          memStats.HeapAlloc,
		HeapSys:            memStats.HeapSys,
		NumGC:              memStats.NumGC,
		NetworkConnections: pms.getNetworkConnections(),
		DiskIORate:         pms.getDiskIORate(),
		LoadAverage:        pms.getLoadAverage(),
	}
}

// newGCPauseMax 前回の計測以降に起きた GC の停止時間の最大値を返す。
// PauseNs は直近256回分の履歴なので、新しい GC がなければ 0 を返し、同じ停止を数え直さない
func (pms *ProductionMonitoringSystem) newGCPauseMax(memStats *runtime.MemStats) uint64 {
	newGCs := min(memStats.NumGC-pms.lastNumGC, uint32(len(memStats.PauseNs)))
	pms.lastNumGC = memStats.NumGC

	var pause uint64
	for i := range newGCs {
		pause = max(pause, memStats.PauseNs[(memStats.NumGC-i+255)%256])
	}
	return pause
}

// calculateCPUUsage CPU使用率計算（簡略化）
func (pms *ProductionMonitoringSystem) calculateCPUUsage() float64 {
	// 実際の実装では適切なCPU使用率測定を行う
	// ここでは簡略化してランダム値を返す
	return float64(runtime.NumGoroutine()) / 100.0
}

// getNetworkConnections ネットワーク接続数取得（簡略化）
func (pms *ProductionMonitoringSystem) getNetworkConnections() int {
	// 簡略化のため固定値を返す
	return 50
}

// getDiskIORate ディスクI/O率取得（簡略化）
func (pms *ProductionMonitoringSystem) getDiskIORate() float64 {
	// 簡略化のため固定値を返す
	return 15.5
}

// getLoadAverage ロードアベレージ取得（簡略化）
func (pms *ProductionMonitoringSystem) getLoadAverage() float64 {
	// 簡略化のため計算値を返す
	return float64(runtime.NumCPU()) * 0.7
}

// AddMetrics メトリクス追加
func (mc *MetricsCollector) AddMetrics(metrics *SystemMetrics) {
	mc.mutex.Lock()
	defer mc.mutex.Unlock()

	mc.currentMetrics = metrics
	mc.historicalData = append(mc.historicalData, metrics)

	// 履歴サイズ制限
	if len(mc.historicalData) > mc.maxHistorySize {
		mc.historicalData = mc.historicalData[1:]
	}

	mc.collectionCount.Add(1)
}

// GetCurrentMetrics 現在のメトリクス取得
func (mc *MetricsCollector) GetCurrentMetrics() *SystemMetrics {
	mc.mutex.RLock()
	defer mc.mutex.RUnlock()
	return mc.currentMetrics
}

// GetHistoricalData 履歴データ取得
func (mc *MetricsCollector) GetHistoricalData(count int) []*SystemMetrics {
	mc.mutex.RLock()
	defer mc.mutex.RUnlock()

	if count <= 0 || count > len(mc.historicalData) {
		count = len(mc.historicalData)
	}

	start := len(mc.historicalData) - count
	result := make([]*SystemMetrics, count)
	copy(result, mc.historicalData[start:])

	return result
}

// anomalyDetectionLoop 異常検出ループ
func (pms *ProductionMonitoringSystem) anomalyDetectionLoop(ctx context.Context) {
	tick := time.Tick(10 * time.Second)

	for {
		select {
		case <-tick:
			metrics := pms.metricsCollector.GetCurrentMetrics()
			if metrics != nil {
				pms.detectAnomalies(metrics)
			}

		case <-ctx.Done():
			return
		case <-pms.stopChan:
			return
		}
	}
}

// detectAnomalies 異常検出
func (pms *ProductionMonitoringSystem) detectAnomalies(metrics *SystemMetrics) {
	pms.anomalyDetector.mutex.Lock()
	defer pms.anomalyDetector.mutex.Unlock()

	// 閾値ベース検出
	pms.detectThresholdAnomalies(metrics)

	// 統計ベース検出
	if !pms.anomalyDetector.isLearning {
		pms.detectStatisticalAnomalies(metrics)
	}

	// 統計モデル更新
	pms.updateStatisticalModel(metrics)
}

// detectThresholdAnomalies 閾値ベース異常検出
func (pms *ProductionMonitoringSystem) detectThresholdAnomalies(metrics *SystemMetrics) {
	thresholds := pms.anomalyDetector.thresholds

	// CPU使用率チェック
	if metrics.CPUUsage > thresholds.CPUUsageCritical {
		pms.createAlert("cpu_critical", "CRITICAL", "CPU使用率が危険レベルに達しました", metrics)
	} else if metrics.CPUUsage > thresholds.CPUUsageWarning {
		pms.createAlert("cpu_warning", "WARNING", "CPU使用率が警告レベルに達しました", metrics)
	}

	// メモリ使用率チェック
	if metrics.MemoryPercent > thresholds.MemoryUsageCritical {
		pms.createAlert("memory_critical", "CRITICAL", "メモリ使用率が危険レベルに達しました", metrics)
	} else if metrics.MemoryPercent > thresholds.MemoryUsageWarning {
		pms.createAlert("memory_warning", "WARNING", "メモリ使用率が警告レベルに達しました", metrics)
	}

	// Goroutine数チェック
	if metrics.GoroutineCount > thresholds.GoroutineCountMax {
		pms.createAlert("goroutine_excess", "WARNING", "Goroutine数が上限を超えました", metrics)
	}

	// GC停止時間チェック
	if metrics.GCPauseTime > thresholds.GCPauseTimeMax {
		pms.createAlert("gc_pause_long", "WARNING", "GC停止時間が長すぎます", metrics)
	}
}

// detectStatisticalAnomalies 統計ベース異常検出
func (pms *ProductionMonitoringSystem) detectStatisticalAnomalies(metrics *SystemMetrics) {
	model := pms.anomalyDetector.statisticalModel

	// CPU使用率の統計的異常検出
	if zScoreExceeds(metrics.CPUUsage, model.CPUMean, model.CPUStdDev) {
		pms.createAlert("cpu_statistical", "WARNING", "CPU使用率に統計的異常を検出", metrics)
	}

	// メモリ使用率の統計的異常検出
	if zScoreExceeds(metrics.MemoryPercent, model.MemoryMean, model.MemoryStdDev) {
		pms.createAlert("memory_statistical", "WARNING", "メモリ使用率に統計的異常を検出", metrics)
	}

	// Goroutine数の統計的異常検出
	if zScoreExceeds(float64(metrics.GoroutineCount), model.GoroutineMean, model.GoroutineStdDev) {
		pms.createAlert("goroutine_statistical", "WARNING", "Goroutine数に統計的異常を検出", metrics)
	}
}

// zScoreExceeds 平均から標準偏差の3倍を超えて離れているかを返します。
// 学習期間中に値が変わらなかった指標は標準偏差が0で、わずかな変化でもz値が無限大になるので判定しない
func zScoreExceeds(value, mean, stdDev float64) bool {
	if stdDev == 0 {
		return false
	}
	return math.Abs((value-mean)/stdDev) > 3.0
}

// updateStatisticalModel 統計モデル更新
func (pms *ProductionMonitoringSystem) updateStatisticalModel(metrics *SystemMetrics) {
	model := pms.anomalyDetector.statisticalModel
	n := float64(model.SampleCount)

	if n == 0 {
		// 初回データ
		model.CPUMean = metrics.CPUUsage
		model.MemoryMean = metrics.MemoryPercent
		model.GoroutineMean = float64(metrics.GoroutineCount)
		model.CPUStdDev = 0
		model.MemoryStdDev = 0
		model.GoroutineStdDev = 0
	} else {
		// オンライン更新（Welford's algorithm）
		cpuDelta := metrics.CPUUsage - model.CPUMean
		model.CPUMean += cpuDelta / (n + 1)
		cpuDelta2 := metrics.CPUUsage - model.CPUMean
		model.CPUStdDev = math.Sqrt((model.CPUStdDev*model.CPUStdDev*n + cpuDelta*cpuDelta2) / (n + 1))

		memoryDelta := metrics.MemoryPercent - model.MemoryMean
		model.MemoryMean += memoryDelta / (n + 1)
		memoryDelta2 := metrics.MemoryPercent - model.MemoryMean
		model.MemoryStdDev = math.Sqrt((model.MemoryStdDev*model.MemoryStdDev*n + memoryDelta*memoryDelta2) / (n + 1))

		goroutineDelta := float64(metrics.GoroutineCount) - model.GoroutineMean
		model.GoroutineMean += goroutineDelta / (n + 1)
		goroutineDelta2 := float64(metrics.GoroutineCount) - model.GoroutineMean
		model.GoroutineStdDev = math.Sqrt((model.GoroutineStdDev*model.GoroutineStdDev*n + goroutineDelta*goroutineDelta2) / (n + 1))
	}

	model.SampleCount++
	model.LastUpdate = time.Now()

	// 学習期間完了チェック
	if pms.anomalyDetector.isLearning && time.Since(pms.startTime) > pms.anomalyDetector.learningPeriod {
		pms.anomalyDetector.isLearning = false
		fmt.Println("統計モデル学習完了、異常検出開始")
	}
}

// createAlert アラート作成
func (pms *ProductionMonitoringSystem) createAlert(alertType, severity, description string, metrics *SystemMetrics) {
	alert := &Alert{
		ID:          fmt.Sprintf("%s_%d", alertType, time.Now().Unix()),
		Timestamp:   time.Now(),
		Type:        alertType,
		Severity:    severity,
		Title:       fmt.Sprintf("System Alert: %s", alertType),
		Description: description,
		Source:      "monitoring_system",
		Tags:        []string{"production", "monitoring", alertType},
		Metadata: map[string]any{
			"cpu_usage":       metrics.CPUUsage,
			"memory_percent":  metrics.MemoryPercent,
			"goroutine_count": metrics.GoroutineCount,
		},
		Status: "active",
	}

	// アラートキューに送信
	select {
	case pms.alertManager.notificationQueue <- alert:
	default:
		fmt.Printf("アラートキューが満杯: %s\n", alert.ID)
	}
}

// alertProcessingLoop アラート処理ループ
func (pms *ProductionMonitoringSystem) alertProcessingLoop(ctx context.Context) {
	for {
		select {
		case alert := <-pms.alertManager.notificationQueue:
			pms.processAlert(alert)

		case <-ctx.Done():
			return
		case <-pms.stopChan:
			return
		}
	}
}

// processAlert アラート処理
func (pms *ProductionMonitoringSystem) processAlert(alert *Alert) {
	pms.alertManager.mutex.Lock()
	defer pms.alertManager.mutex.Unlock()

	// 重複チェック（種類ごとに1件。登録と検索は同じキーを使う）
	if existingAlert, exists := pms.alertManager.activeAlerts[alert.Type]; exists {
		// 既存アラートの更新
		existingAlert.Timestamp = alert.Timestamp
		existingAlert.Metadata = alert.Metadata
		return
	}

	// 新しいアラートとして追加
	pms.alertManager.activeAlerts[alert.Type] = alert
	pms.alertManager.alertHistory = append(pms.alertManager.alertHistory, alert)

	// 通知送信
	pms.sendNotification(alert)

	// ダッシュボード更新。登録したアラートは後で更新されるので、配信側には写しを渡す
	sent := *alert
	update := &DashboardUpdate{
		Type:      "alert",
		Timestamp: time.Now(),
		Data:      &sent,
	}

	select {
	case pms.dashboardServer.updateChannel <- update:
	default:
		// チャンネルが満杯の場合はスキップ
	}

	fmt.Printf("アラート発生: [%s] %s - %s\n", alert.Severity, alert.Type, alert.Description)
}

// sendNotification 通知送信
func (pms *ProductionMonitoringSystem) sendNotification(alert *Alert) {
	// 実際の実装では、メール、Slack、PagerDutyなどに通知を送信
	fmt.Printf("通知送信: %s\n", alert.Title)
}

// profileAnalysisLoop プロファイル分析ループ
func (pms *ProductionMonitoringSystem) profileAnalysisLoop(ctx context.Context) {
	tick := time.Tick(pms.profileAnalyzer.analysisInterval)

	for {
		select {
		case <-tick:
			if pms.profileAnalyzer.autoAnalysisEnabled {
				pms.performAutomaticAnalysis()
			}

		case <-ctx.Done():
			return
		case <-pms.stopChan:
			return
		}
	}
}

// performAutomaticAnalysis 自動分析実行
func (pms *ProductionMonitoringSystem) performAutomaticAnalysis() {
	pms.profileAnalyzer.mutex.Lock()
	defer pms.profileAnalyzer.mutex.Unlock()

	historicalData := pms.metricsCollector.GetHistoricalData(100)
	if len(historicalData) < 10 {
		return // データ不足
	}

	// トレンド分析
	trendAnalysis := pms.analyzeTrends(historicalData)

	// パフォーマンス分析
	performanceAnalysis := pms.analyzePerformance(historicalData)

	// 容量計画分析
	capacityAnalysis := pms.analyzeCapacity(historicalData)

	// 分析結果をまとめる
	result := &AnalysisResult{
		Timestamp:    time.Now(),
		AnalysisType: "automatic_system_analysis",
		Summary:      "定期的なシステム分析を完了しました",
		Findings: []string{
			trendAnalysis,
			performanceAnalysis,
			capacityAnalysis,
		},
		Recommendations: pms.generateRecommendations(historicalData),
		Severity:        "INFO",
		Data: map[string]any{
			"analysis_period": pms.profileAnalyzer.analysisInterval.String(),
			"data_points":     len(historicalData),
		},
	}

	pms.profileAnalyzer.analysisResults = append(pms.profileAnalyzer.analysisResults, result)
	pms.profileAnalyzer.lastAnalysis = time.Now()

	fmt.Printf("自動分析完了: %s\n", result.Summary)
}

// analyzeTrends トレンド分析
func (pms *ProductionMonitoringSystem) analyzeTrends(data []*SystemMetrics) string {
	if len(data) < 2 {
		return "トレンド分析にはデータが不足しています"
	}

	// CPU使用率トレンド
	cpuStart := data[0].CPUUsage
	cpuEnd := data[len(data)-1].CPUUsage
	cpuTrend := cpuEnd - cpuStart

	// メモリ使用率トレンド
	memStart := data[0].MemoryPercent
	memEnd := data[len(data)-1].MemoryPercent
	memTrend := memEnd - memStart

	return fmt.Sprintf("CPU使用率変化: %.2f%%, メモリ使用率変化: %.2f%%", cpuTrend, memTrend)
}

// analyzePerformance パフォーマンス分析
func (pms *ProductionMonitoringSystem) analyzePerformance(data []*SystemMetrics) string {
	if len(data) == 0 {
		return "パフォーマンス分析にはデータが不足しています"
	}

	var totalGCPause uint64
	var maxGCPause uint64
	var gcCount int

	for _, metrics := range data {
		if metrics.GCPauseTime > 0 {
			totalGCPause += metrics.GCPauseTime
			maxGCPause = max(maxGCPause, metrics.GCPauseTime)
			gcCount++
		}
	}

	var avgGCPause float64
	if gcCount > 0 {
		avgGCPause = float64(totalGCPause) / float64(gcCount) / 1000000 // convert to ms
	}

	return fmt.Sprintf("平均GC停止時間: %.2fms, 最大GC停止時間: %.2fms", avgGCPause, float64(maxGCPause)/1000000)
}

// analyzeCapacity 容量分析
func (pms *ProductionMonitoringSystem) analyzeCapacity(data []*SystemMetrics) string {
	if len(data) == 0 {
		return "容量分析にはデータが不足しています"
	}

	var maxMemory uint64
	var maxGoroutines int

	for _, metrics := range data {
		maxMemory = max(maxMemory, metrics.MemoryUsage)
		maxGoroutines = max(maxGoroutines, metrics.GoroutineCount)
	}

	return fmt.Sprintf("最大メモリ使用量: %d MB, 最大Goroutine数: %d", maxMemory/(1024*1024), maxGoroutines)
}

// generateRecommendations 推奨事項生成
func (pms *ProductionMonitoringSystem) generateRecommendations(data []*SystemMetrics) []string {
	recommendations := []string{}

	if len(data) == 0 {
		return recommendations
	}

	latest := data[len(data)-1]

	if latest.CPUUsage > 80 {
		recommendations = append(recommendations, "CPU使用率が高いため、スケールアウトを検討してください")
	}

	if latest.MemoryPercent > 85 {
		recommendations = append(recommendations, "メモリ使用率が高いため、メモリ増設またはメモリリーク調査を実施してください")
	}

	if latest.GoroutineCount > 5000 {
		recommendations = append(recommendations, "Goroutine数が多いため、並行処理の最適化を検討してください")
	}

	if latest.GCPauseTime > 50000000 { // 50ms
		recommendations = append(recommendations, "GC停止時間が長いため、ヒープサイズ調整またはGCチューニングを実施してください")
	}

	return recommendations
}

// dashboardUpdateLoop ダッシュボード更新ループ
func (pms *ProductionMonitoringSystem) dashboardUpdateLoop(ctx context.Context) {
	for {
		select {
		case update := <-pms.dashboardServer.updateChannel:
			pms.broadcastUpdate(update)

		case <-ctx.Done():
			return
		case <-pms.stopChan:
			return
		}
	}
}

// broadcastUpdate 更新ブロードキャスト
func (pms *ProductionMonitoringSystem) broadcastUpdate(update *DashboardUpdate) {
	pms.dashboardServer.mutex.RLock()
	defer pms.dashboardServer.mutex.RUnlock()

	data, err := json.Marshal(update)
	if err != nil {
		fmt.Printf("ダッシュボード更新のマーシャルエラー: %v\n", err)
		return
	}

	// 全WebSocket接続にブロードキャスト
	for _, conn := range pms.dashboardServer.wsConnections {
		select {
		case conn.SendChannel <- data:
		default:
			// 送信チャンネルが満杯の場合はスキップ
		}
	}
}

// uptime 開始からの経過時間。開始前は0。
// startTime は Start がサーバーを起動する前に書き込むので、ハンドラーからはロックなしで読める。
// Stop は stateMu を持ったまま Shutdown で処理中のリクエストを待つため、ここで stateMu を取ってはいけない
func (pms *ProductionMonitoringSystem) uptime() time.Duration {
	if pms.startTime.IsZero() {
		return 0
	}
	return time.Since(pms.startTime)
}

// HTTP ハンドラー関数群

// dashboardHandler ダッシュボードハンドラー
func (ds *DashboardServer) dashboardHandler(w http.ResponseWriter, r *http.Request) {
	// 簡略化のためHTMLを直接返す
	html := `
<!DOCTYPE html>
<html>
<head>
    <title>Production Monitoring Dashboard</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        .metric { margin: 10px 0; padding: 10px; border: 1px solid #ddd; }
        .alert { background-color: #ffcccc; }
        .normal { background-color: #ccffcc; }
    </style>
</head>
<body>
    <h1>Production Monitoring Dashboard</h1>
    <div id="metrics"></div>
    <div id="alerts"></div>
    <script>
        // WebSocketやAPIを使ったリアルタイム更新の実装
        setInterval(function() {
            fetch('/api/metrics').then(r => r.json()).then(data => {
                document.getElementById('metrics').innerHTML = 
                    '<h2>Current Metrics</h2>' + JSON.stringify(data, null, 2);
            });
            fetch('/api/alerts').then(r => r.json()).then(data => {
                document.getElementById('alerts').innerHTML = 
                    '<h2>Active Alerts</h2>' + JSON.stringify(data, null, 2);
            });
        }, 5000);
    </script>
</body>
</html>`

	w.Header().Set("Content-Type", "text/html")
	if _, err := w.Write([]byte(html)); err != nil {
		log.Printf("Failed to write dashboard HTML: %v", err)
	}
}

// metricsAPIHandler メトリクスAPIハンドラー
func (ds *DashboardServer) metricsAPIHandler(w http.ResponseWriter, r *http.Request) {
	metrics := ds.system.metricsCollector.GetCurrentMetrics()
	var response any = metrics
	if metrics == nil {
		response = map[string]any{
			"timestamp": time.Now(),
			"status":    "collecting",
		}
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Printf("Failed to encode metrics response: %v", err)
	}
}

// alertsAPIHandler アラートAPIハンドラー
func (ds *DashboardServer) alertsAPIHandler(w http.ResponseWriter, r *http.Request) {
	// アラート処理ループが更新するので、ロックの中で値として写し取る
	am := ds.system.alertManager
	am.mutex.RLock()
	alerts := make([]Alert, 0, len(am.activeAlerts))
	for _, alert := range am.activeAlerts {
		alerts = append(alerts, *alert)
	}
	am.mutex.RUnlock()

	response := map[string]any{
		"active_alerts": len(alerts),
		"alerts":        alerts,
		"timestamp":     time.Now(),
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Printf("Failed to encode alerts response: %v", err)
	}
}

// healthAPIHandler ヘルスAPIハンドラー
func (ds *DashboardServer) healthAPIHandler(w http.ResponseWriter, r *http.Request) {
	response := map[string]any{
		"status":    "healthy",
		"timestamp": time.Now(),
		"uptime":    ds.system.uptime().String(),
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Printf("Failed to encode health response: %v", err)
	}
}

// analysisAPIHandler 分析APIハンドラー
func (ds *DashboardServer) analysisAPIHandler(w http.ResponseWriter, r *http.Request) {
	pa := ds.system.profileAnalyzer
	pa.mutex.RLock()
	var latest *AnalysisResult
	if n := len(pa.analysisResults); n > 0 {
		latest = pa.analysisResults[n-1]
	}
	pa.mutex.RUnlock()

	status := "pending"
	if latest != nil {
		status = "completed"
	}
	response := map[string]any{
		"latest_analysis": latest,
		"status":          status,
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Printf("Failed to encode analysis response: %v", err)
	}
}

func main() {
	// 本番環境監視システムを作成
	monitoringSystem := NewProductionMonitoringSystem()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// システム開始
	if err := monitoringSystem.Start(ctx); err != nil {
		panic(err)
	}

	fmt.Println("本番環境監視システム実行中")
	fmt.Println("ダッシュボード: http://localhost:8080")
	fmt.Println("APIエンドポイント:")
	fmt.Println("- http://localhost:8080/api/metrics")
	fmt.Println("- http://localhost:8080/api/alerts")
	fmt.Println("- http://localhost:8080/api/health")
	fmt.Println("- http://localhost:8080/api/analysis")
	fmt.Println("\nCtrl+Cで停止")

	// 30秒間、または Ctrl+C を受けるまで実行
	select {
	case <-ctx.Done():
	case <-time.After(30 * time.Second):
	}

	// システム停止
	if err := monitoringSystem.Stop(); err != nil {
		fmt.Printf("停止エラー: %v\n", err)
	}
}
