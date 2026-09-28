package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

// HealthStatus 健全性チェックの結果を表します
type HealthStatus struct {
	Name      string         `json:"name"`
	Status    string         `json:"status"` // "healthy", "warning", "critical"
	Message   string         `json:"message"`
	Timestamp time.Time      `json:"timestamp"`
	Details   map[string]any `json:"details"`
	Score     float64        `json:"score"` // 0.0-1.0
}

// HealthChecker 健全性チェックインターフェースです
type HealthChecker interface {
	Check(ctx context.Context) HealthStatus
	Name() string
	Critical() bool
}

// HealingAction 復旧アクションのインターフェースです
type HealingAction interface {
	Execute(ctx context.Context, issue HealthIssue) error
	Name() string
	CanHandle(issueType string) bool
	EstimatedDuration() time.Duration
}

// HealthIssue 検出された健全性の問題を表します
type HealthIssue struct {
	Type        string         `json:"type"`
	Severity    Severity       `json:"severity"`
	Description string         `json:"description"`
	Timestamp   time.Time      `json:"timestamp"`
	Checker     string         `json:"checker"`
	Details     map[string]any `json:"details"`
	Predictive  bool           `json:"predictive"`
}

// Severity 問題の重要度を表します
type Severity int

const (
	SeverityLow Severity = iota
	SeverityMedium
	SeverityHigh
	SeverityCritical
)

func (s Severity) String() string {
	switch s {
	case SeverityLow:
		return "LOW"
	case SeverityMedium:
		return "MEDIUM"
	case SeverityHigh:
		return "HIGH"
	case SeverityCritical:
		return "CRITICAL"
	default:
		return "UNKNOWN"
	}
}

// AlertLevel アラートレベルを表します
type AlertLevel int

const (
	AlertLevelInfo AlertLevel = iota
	AlertLevelWarning
	AlertLevelError
	AlertLevelCritical
)

func (a AlertLevel) String() string {
	switch a {
	case AlertLevelInfo:
		return "INFO"
	case AlertLevelWarning:
		return "WARNING"
	case AlertLevelError:
		return "ERROR"
	case AlertLevelCritical:
		return "CRITICAL"
	default:
		return "UNKNOWN"
	}
}

// Alert アラート情報を表します
type Alert struct {
	Level     AlertLevel     `json:"level"`
	Message   string         `json:"message"`
	Timestamp time.Time      `json:"timestamp"`
	Source    string         `json:"source"`
	Details   map[string]any `json:"details"`
}

// AlertManager アラート管理を行います
type AlertManager struct {
	alerts    []Alert
	mutex     sync.RWMutex
	maxAlerts int
	callbacks []func(Alert)
}

// NewAlertManager 新しいアラートマネージャーを作成します
func NewAlertManager(maxAlerts int) *AlertManager {
	return &AlertManager{
		alerts:    make([]Alert, 0),
		maxAlerts: maxAlerts,
		callbacks: make([]func(Alert), 0),
	}
}

// Send アラートを送信します
func (am *AlertManager) Send(alert Alert) {
	alert.Timestamp = time.Now()

	am.mutex.Lock()
	am.alerts = append(am.alerts, alert)

	// 最大数を超えた場合は古いアラートを削除
	if len(am.alerts) > am.maxAlerts {
		am.alerts = am.alerts[len(am.alerts)-am.maxAlerts:]
	}
	callbacks := am.callbacks
	am.mutex.Unlock()

	// コールバック実行
	for _, callback := range callbacks {
		go callback(alert)
	}
}

// AddCallback アラートコールバックを追加します
func (am *AlertManager) AddCallback(callback func(Alert)) {
	am.mutex.Lock()
	defer am.mutex.Unlock()
	// Send が保持中のスライスと配列を共有しないように、常に新しいスライスを作る
	am.callbacks = append(slices.Clip(am.callbacks), callback)
}

// GetAlerts 最近のアラートを取得します
func (am *AlertManager) GetAlerts(limit int) []Alert {
	am.mutex.RLock()
	defer am.mutex.RUnlock()

	if limit <= 0 || limit > len(am.alerts) {
		limit = len(am.alerts)
	}

	result := slices.Clone(am.alerts[len(am.alerts)-limit:])

	// 新しい順にソート
	slices.SortFunc(result, func(a, b Alert) int {
		return b.Timestamp.Compare(a.Timestamp)
	})

	return result
}

// SystemMetrics システムメトリクスを表します
type SystemMetrics struct {
	CPUUsage       float64   `json:"cpu_usage"`
	MemoryUsage    float64   `json:"memory_usage"`
	GoroutineCount int       `json:"goroutine_count"`
	HeapSize       uint64    `json:"heap_size"`
	LastUpdated    time.Time `json:"last_updated"`
}

// AutoHealingSystem 自己修復システムです
type AutoHealingSystem struct {
	healthCheckers []HealthChecker
	healingActions map[string][]HealingAction
	alertManager   *AlertManager
	metrics        *SystemMetrics
	historySize    int
	healthHistory  []HealthStatus
	issueHistory   []HealthIssue
	actionHistory  []HealingActionResult

	ctx             context.Context
	cancel          context.CancelFunc
	interval        time.Duration
	retryAttempts   int
	escalationDelay time.Duration

	isRunning              atomic.Bool
	totalChecks            atomic.Int64
	totalIssues            atomic.Int64
	totalActions           atomic.Int64
	totalSuccessfulActions atomic.Int64

	// lifecycle Start と Stop を直列化し、wg.Wait と wg.Go が同時に走らないようにします
	lifecycle sync.Mutex
	// wg 監視ループと、そこから起動した復旧処理の goroutine を数えます
	wg sync.WaitGroup
	// healingInFlight 復旧処理が実行中の障害の種類。監視の周期ごとに同じ障害の復旧を重ねて起動しないために使う
	healingInFlight sync.Map

	mutex sync.RWMutex
}

// HealingActionResult 復旧アクション結果を表します
type HealingActionResult struct {
	Action    string        `json:"action"`
	Issue     HealthIssue   `json:"issue"`
	Success   bool          `json:"success"`
	Error     string        `json:"error,omitempty"`
	Duration  time.Duration `json:"duration"`
	Timestamp time.Time     `json:"timestamp"`
}

// NewAutoHealingSystem 新しい自己修復システムを作成します
func NewAutoHealingSystem(interval time.Duration, retryAttempts int) *AutoHealingSystem {
	ctx, cancel := context.WithCancel(context.Background())

	return &AutoHealingSystem{
		healthCheckers:  make([]HealthChecker, 0),
		healingActions:  make(map[string][]HealingAction),
		alertManager:    NewAlertManager(100),
		metrics:         &SystemMetrics{},
		historySize:     100,
		healthHistory:   make([]HealthStatus, 0),
		issueHistory:    make([]HealthIssue, 0),
		actionHistory:   make([]HealingActionResult, 0),
		ctx:             ctx,
		cancel:          cancel,
		interval:        interval,
		retryAttempts:   retryAttempts,
		escalationDelay: 30 * time.Second,
	}
}

// AddHealthChecker 健全性チェッカーを追加します
func (ahs *AutoHealingSystem) AddHealthChecker(checker HealthChecker) {
	ahs.mutex.Lock()
	defer ahs.mutex.Unlock()
	ahs.healthCheckers = append(ahs.healthCheckers, checker)
}

// AddHealingAction 復旧アクションを追加します
func (ahs *AutoHealingSystem) AddHealingAction(issueType string, action HealingAction) {
	ahs.mutex.Lock()
	defer ahs.mutex.Unlock()
	ahs.healingActions[issueType] = append(ahs.healingActions[issueType], action)
}

// checkers 実行中に追加されても影響を受けないチェッカーの一覧を返します
func (ahs *AutoHealingSystem) checkers() []HealthChecker {
	ahs.mutex.RLock()
	defer ahs.mutex.RUnlock()
	return slices.Clone(ahs.healthCheckers)
}

// Start 自己修復システムを開始します
func (ahs *AutoHealingSystem) Start() error {
	ahs.lifecycle.Lock()
	defer ahs.lifecycle.Unlock()

	if ahs.ctx.Err() != nil {
		return errors.New("auto healing system has been stopped")
	}
	if !ahs.isRunning.CompareAndSwap(false, true) {
		return errors.New("auto healing system is already running")
	}

	log.Println("Starting Auto-Healing System...")

	// アラートマネージャーのコールバック設定
	ahs.alertManager.AddCallback(func(alert Alert) {
		log.Printf("ALERT [%s]: %s", alert.Level, alert.Message)
	})

	// メインループを開始
	ahs.wg.Go(ahs.mainLoop)

	// メトリクス更新ループを開始
	ahs.wg.Go(ahs.metricsUpdateLoop)

	log.Println("Auto-Healing System started successfully")
	return nil
}

// Stop 自己修復システムを停止します
func (ahs *AutoHealingSystem) Stop() {
	ahs.lifecycle.Lock()
	defer ahs.lifecycle.Unlock()

	// 一度も開始していなくても、以後の Start を拒否できるようにキャンセルしておく
	ahs.cancel()
	if !ahs.isRunning.CompareAndSwap(true, false) {
		return
	}

	log.Println("Stopping Auto-Healing System...")
	ahs.wg.Wait()
	log.Println("Auto-Healing System stopped")
}

// mainLoop メインの監視ループです
func (ahs *AutoHealingSystem) mainLoop() {
	tick := time.Tick(ahs.interval)

	for {
		select {
		case <-ahs.ctx.Done():
			return
		case <-tick:
			ahs.performHealthCheck()
		}
	}
}

// metricsUpdateLoop メトリクス更新ループです
func (ahs *AutoHealingSystem) metricsUpdateLoop() {
	tick := time.Tick(10 * time.Second)

	for {
		select {
		case <-ahs.ctx.Done():
			return
		case <-tick:
			ahs.updateSystemMetrics()
		}
	}
}

// performHealthCheck 健全性チェックを実行します
func (ahs *AutoHealingSystem) performHealthCheck() {
	ahs.totalChecks.Add(1)

	checkers := ahs.checkers()
	var wg sync.WaitGroup
	healthResults := make(chan HealthStatus, len(checkers))

	// 全チェッカーを並列実行
	for _, c := range checkers {
		wg.Go(func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("Health checker %s panicked: %v", c.Name(), r)
					healthResults <- HealthStatus{
						Name:      c.Name(),
						Status:    "critical",
						Message:   fmt.Sprintf("Checker panicked: %v", r),
						Timestamp: time.Now(),
						Score:     0.0,
					}
				}
			}()

			ctx, cancel := context.WithTimeout(ahs.ctx, 30*time.Second)
			defer cancel()

			start := time.Now()
			result := c.Check(ctx)
			duration := time.Since(start)

			if result.Details == nil {
				result.Details = make(map[string]any)
			}
			result.Details["check_duration"] = duration

			healthResults <- result
		})
	}

	// 結果収集
	go func() {
		wg.Wait()
		close(healthResults)
	}()

	ahs.processHealthResults(healthResults)
}

// processHealthResults 健全性チェック結果を処理します
func (ahs *AutoHealingSystem) processHealthResults(results chan HealthStatus) {
	var issues []HealthIssue

	for result := range results {
		// 履歴に追加
		ahs.addHealthHistory(result)

		// 問題を検出
		if result.Status != "healthy" {
			issue := ahs.convertToIssue(result)
			issues = append(issues, issue)
		}
	}

	// 検出された問題を処理
	for _, issue := range issues {
		ahs.totalIssues.Add(1)
		ahs.addIssueHistory(issue)

		if _, running := ahs.healingInFlight.LoadOrStore(issue.Type, struct{}{}); running {
			continue
		}
		// 復旧アクションを実行する。mainLoop が wg に数えられている間に起動するので、Stop の wg.Wait と競合しない
		ahs.wg.Go(func() {
			defer ahs.healingInFlight.Delete(issue.Type)
			if err := ahs.executeHealing(issue); err != nil {
				log.Printf("Failed to execute healing for issue %s: %v", issue.Type, err)
			}
		})
	}

	// 予兆検知
	ahs.predictiveAnalysis()
}

// convertToIssue HealthStatusをHealthIssueに変換します
func (ahs *AutoHealingSystem) convertToIssue(status HealthStatus) HealthIssue {
	var severity Severity
	switch status.Status {
	case "warning":
		severity = SeverityMedium
	case "critical":
		severity = SeverityCritical
	default:
		severity = SeverityLow
	}

	return HealthIssue{
		Type:        status.Name,
		Severity:    severity,
		Description: status.Message,
		Timestamp:   status.Timestamp,
		Checker:     status.Name,
		Details:     status.Details,
		Predictive:  false,
	}
}

// executeHealing 復旧処理を実行します
func (ahs *AutoHealingSystem) executeHealing(issue HealthIssue) error {
	ahs.mutex.RLock()
	actions := slices.Clone(ahs.healingActions[issue.Type])
	ahs.mutex.RUnlock()
	if len(actions) == 0 {
		log.Printf("No healing actions available for issue type: %s", issue.Type)
		return fmt.Errorf("no healing actions for issue type: %s", issue.Type)
	}

	// アラート送信
	ahs.alertManager.Send(Alert{
		Level:   AlertLevelWarning,
		Message: fmt.Sprintf("Issue detected: %s - %s", issue.Type, issue.Description),
		Source:  "auto-healing",
		Details: map[string]any{
			"issue":    issue,
			"severity": issue.Severity.String(),
		},
	})

	for attempt := range ahs.retryAttempts {
		for _, action := range actions {
			if !action.CanHandle(issue.Type) {
				continue
			}

			ahs.totalActions.Add(1)

			start := time.Now()
			result := HealingActionResult{
				Action:    action.Name(),
				Issue:     issue,
				Timestamp: start,
			}

			log.Printf("Executing healing action: %s for issue: %s (attempt %d/%d)",
				action.Name(), issue.Type, attempt+1, ahs.retryAttempts)

			err := action.Execute(ahs.ctx, issue)
			result.Duration = time.Since(start)

			if err != nil {
				result.Success = false
				result.Error = err.Error()
				log.Printf("Healing action %s failed: %v", action.Name(), err)

				ahs.alertManager.Send(Alert{
					Level:   AlertLevelError,
					Message: fmt.Sprintf("Healing action failed: %s - %v", action.Name(), err),
					Source:  "auto-healing",
				})
			} else {
				result.Success = true
				ahs.totalSuccessfulActions.Add(1)
				log.Printf("Healing action %s completed successfully", action.Name())

				ahs.alertManager.Send(Alert{
					Level:   AlertLevelInfo,
					Message: fmt.Sprintf("Healing action completed: %s", action.Name()),
					Source:  "auto-healing",
				})
			}

			ahs.addActionHistory(result)

			// 復旧確認
			if result.Success && ahs.verifyRecovery(issue) {
				return nil
			}
		}

		if attempt < ahs.retryAttempts-1 {
			delay := time.Duration(attempt+1) * ahs.escalationDelay
			log.Printf("Waiting %v before next attempt...", delay)
			// 停止時に待機を打ち切れるよう、固定の Sleep ではなく ctx と一緒に待つ
			select {
			case <-ahs.ctx.Done():
				return ahs.ctx.Err()
			case <-time.After(delay):
			}
		}
	}

	// 最終的に失敗
	ahs.alertManager.Send(Alert{
		Level:   AlertLevelCritical,
		Message: fmt.Sprintf("Auto-healing failed after %d attempts for issue: %s", ahs.retryAttempts, issue.Type),
		Source:  "auto-healing",
		Details: map[string]any{
			"issue": issue,
		},
	})

	return fmt.Errorf("auto-healing failed after %d attempts", ahs.retryAttempts)
}

// verifyRecovery 復旧を確認します
func (ahs *AutoHealingSystem) verifyRecovery(issue HealthIssue) bool {
	// 対象のチェッカーを再実行
	for _, checker := range ahs.checkers() {
		if checker.Name() == issue.Checker {
			ctx, cancel := context.WithTimeout(ahs.ctx, 15*time.Second)
			result := checker.Check(ctx)
			cancel()

			return result.Status == "healthy"
		}
	}
	return false
}

// predictiveAnalysis 予兆検知分析を行います
func (ahs *AutoHealingSystem) predictiveAnalysis() {
	ahs.mutex.RLock()
	defer ahs.mutex.RUnlock()

	if len(ahs.healthHistory) < 10 {
		return // 十分なデータがない
	}

	// 簡単な予兆検知：同じ種類の問題が頻発している場合
	recentIssues := make(map[string]int)
	cutoff := time.Now().Add(-time.Hour)

	for _, issue := range ahs.issueHistory {
		if issue.Timestamp.After(cutoff) {
			recentIssues[issue.Type]++
		}
	}

	for issueType, count := range recentIssues {
		if count >= 3 { // 1時間以内に3回以上
			log.Printf("Potential recurring issue detected: %s (count: %d)", issueType, count)

			ahs.alertManager.Send(Alert{
				Level:   AlertLevelWarning,
				Message: fmt.Sprintf("Recurring issue pattern detected: %s", issueType),
				Source:  "predictive-analysis",
				Details: map[string]any{
					"issue_type":   issueType,
					"recent_count": count,
					"time_window":  "1 hour",
				},
			})
		}
	}
}

// updateSystemMetrics システムメトリクスを更新します
func (ahs *AutoHealingSystem) updateSystemMetrics() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	ahs.mutex.Lock()
	ahs.metrics.GoroutineCount = runtime.NumGoroutine()
	ahs.metrics.HeapSize = m.HeapAlloc
	ahs.metrics.MemoryUsage = float64(m.HeapAlloc) / float64(m.Sys) * 100
	ahs.metrics.LastUpdated = time.Now()
	ahs.mutex.Unlock()
}

// addHealthHistory 健全性履歴に追加します
func (ahs *AutoHealingSystem) addHealthHistory(status HealthStatus) {
	ahs.mutex.Lock()
	defer ahs.mutex.Unlock()

	ahs.healthHistory = append(ahs.healthHistory, status)
	if len(ahs.healthHistory) > ahs.historySize {
		ahs.healthHistory = ahs.healthHistory[len(ahs.healthHistory)-ahs.historySize:]
	}
}

// addIssueHistory 問題履歴に追加します
func (ahs *AutoHealingSystem) addIssueHistory(issue HealthIssue) {
	ahs.mutex.Lock()
	defer ahs.mutex.Unlock()

	ahs.issueHistory = append(ahs.issueHistory, issue)
	if len(ahs.issueHistory) > ahs.historySize {
		ahs.issueHistory = ahs.issueHistory[len(ahs.issueHistory)-ahs.historySize:]
	}
}

// addActionHistory アクション履歴に追加します
func (ahs *AutoHealingSystem) addActionHistory(result HealingActionResult) {
	ahs.mutex.Lock()
	defer ahs.mutex.Unlock()

	ahs.actionHistory = append(ahs.actionHistory, result)
	if len(ahs.actionHistory) > ahs.historySize {
		ahs.actionHistory = ahs.actionHistory[len(ahs.actionHistory)-ahs.historySize:]
	}
}

// GetStatus システム状態を取得します
func (ahs *AutoHealingSystem) GetStatus() map[string]any {
	ahs.mutex.RLock()
	defer ahs.mutex.RUnlock()

	return map[string]any{
		"is_running":               ahs.isRunning.Load(),
		"total_checks":             ahs.totalChecks.Load(),
		"total_issues":             ahs.totalIssues.Load(),
		"total_actions":            ahs.totalActions.Load(),
		"total_successful_actions": ahs.totalSuccessfulActions.Load(),
		"system_metrics":           *ahs.metrics, // 呼び出し側がロック外で読むので値でコピーする
		"recent_alerts":            ahs.alertManager.GetAlerts(10),
		"health_checkers":          len(ahs.healthCheckers),
		"healing_actions":          len(ahs.healingActions),
	}
}

// MemoryHealthChecker メモリ使用量をチェックします
type MemoryHealthChecker struct {
	threshold float64
}

func NewMemoryHealthChecker(threshold float64) *MemoryHealthChecker {
	return &MemoryHealthChecker{threshold: threshold}
}

func (mhc *MemoryHealthChecker) Name() string {
	return "memory_usage"
}

func (mhc *MemoryHealthChecker) Critical() bool {
	return true
}

func (mhc *MemoryHealthChecker) Check(ctx context.Context) HealthStatus {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	usage := float64(m.HeapAlloc) / float64(m.Sys) * 100

	status := HealthStatus{
		Name:      mhc.Name(),
		Timestamp: time.Now(),
		Details: map[string]any{
			"heap_alloc":    m.HeapAlloc,
			"heap_sys":      m.HeapSys,
			"usage_percent": usage,
			"threshold":     mhc.threshold,
		},
		Score: 1.0 - (usage / 100.0),
	}

	if usage > mhc.threshold {
		status.Status = "critical"
		status.Message = fmt.Sprintf("Memory usage is %.2f%%, exceeding threshold of %.2f%%", usage, mhc.threshold)
	} else if usage > mhc.threshold*0.8 {
		status.Status = "warning"
		status.Message = fmt.Sprintf("Memory usage is %.2f%%, approaching threshold of %.2f%%", usage, mhc.threshold)
	} else {
		status.Status = "healthy"
		status.Message = fmt.Sprintf("Memory usage is %.2f%%, within normal range", usage)
	}

	return status
}

// GoroutineHealthChecker Goroutine数をチェックします
type GoroutineHealthChecker struct {
	threshold int
}

func NewGoroutineHealthChecker(threshold int) *GoroutineHealthChecker {
	return &GoroutineHealthChecker{threshold: threshold}
}

func (ghc *GoroutineHealthChecker) Name() string {
	return "goroutine_count"
}

func (ghc *GoroutineHealthChecker) Critical() bool {
	return true
}

func (ghc *GoroutineHealthChecker) Check(ctx context.Context) HealthStatus {
	count := runtime.NumGoroutine()

	status := HealthStatus{
		Name:      ghc.Name(),
		Timestamp: time.Now(),
		Details: map[string]any{
			"current_count": count,
			"threshold":     ghc.threshold,
		},
		Score: 1.0 - (float64(count) / float64(ghc.threshold*2)),
	}

	if count > ghc.threshold {
		status.Status = "critical"
		status.Message = fmt.Sprintf("Goroutine count is %d, exceeding threshold of %d", count, ghc.threshold)
	} else if count > int(float64(ghc.threshold)*0.8) {
		status.Status = "warning"
		status.Message = fmt.Sprintf("Goroutine count is %d, approaching threshold of %d", count, ghc.threshold)
	} else {
		status.Status = "healthy"
		status.Message = fmt.Sprintf("Goroutine count is %d, within normal range", count)
	}

	return status
}

// GCTriggerAction GCを強制実行するアクションです
type GCTriggerAction struct{}

func NewGCTriggerAction() *GCTriggerAction {
	return &GCTriggerAction{}
}

func (gca *GCTriggerAction) Name() string {
	return "force_gc"
}

func (gca *GCTriggerAction) CanHandle(issueType string) bool {
	return issueType == "memory_usage"
}

func (gca *GCTriggerAction) EstimatedDuration() time.Duration {
	return 5 * time.Second
}

func (gca *GCTriggerAction) Execute(ctx context.Context, issue HealthIssue) error {
	log.Println("Executing forced garbage collection...")
	runtime.GC()
	log.Println("Garbage collection completed")
	return nil
}

// HTTPHealthChecker HTTPエンドポイントをチェックします
type HTTPHealthChecker struct {
	name     string
	url      string
	timeout  time.Duration
	critical bool
}

func NewHTTPHealthChecker(name, url string, timeout time.Duration, critical bool) *HTTPHealthChecker {
	return &HTTPHealthChecker{
		name:     name,
		url:      url,
		timeout:  timeout,
		critical: critical,
	}
}

func (hhc *HTTPHealthChecker) Name() string {
	return hhc.name
}

func (hhc *HTTPHealthChecker) Critical() bool {
	return hhc.critical
}

func (hhc *HTTPHealthChecker) Check(ctx context.Context) HealthStatus {
	client := &http.Client{Timeout: hhc.timeout}

	start := time.Now()
	resp, err := hhc.get(ctx, client)
	duration := time.Since(start)

	status := HealthStatus{
		Name:      hhc.Name(),
		Timestamp: time.Now(),
		Details: map[string]any{
			"url":           hhc.url,
			"response_time": duration,
		},
	}

	if err != nil {
		status.Status = "critical"
		status.Message = fmt.Sprintf("HTTP check failed: %v", err)
		status.Score = 0.0
		status.Details["error"] = err.Error()
	} else {
		defer func() {
			if err := resp.Body.Close(); err != nil {
				log.Printf("Failed to close response body: %v", err)
			}
		}()
		status.Details["status_code"] = resp.StatusCode

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			status.Status = "healthy"
			status.Message = fmt.Sprintf("HTTP check successful (status: %d, response time: %v)", resp.StatusCode, duration)
			status.Score = 1.0
		} else {
			status.Status = "warning"
			status.Message = fmt.Sprintf("HTTP check returned non-2xx status: %d", resp.StatusCode)
			status.Score = 0.5
		}
	}

	return status
}

// get 停止時に要求を打ち切れるよう、ctx 付きで GET を送ります
func (hhc *HTTPHealthChecker) get(ctx context.Context, client *http.Client) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, hhc.url, nil)
	if err != nil {
		return nil, err
	}
	return client.Do(req)
}

// 使用例とテスト用のmain関数
func main() {
	// 自己修復システムを作成
	ahs := NewAutoHealingSystem(30*time.Second, 3)

	// 健全性チェッカーを追加
	ahs.AddHealthChecker(NewMemoryHealthChecker(85.0))
	ahs.AddHealthChecker(NewGoroutineHealthChecker(1000))
	ahs.AddHealthChecker(NewHTTPHealthChecker("local_server", "http://localhost:8080/health", 5*time.Second, true))

	// 復旧アクションを追加
	ahs.AddHealingAction("memory_usage", NewGCTriggerAction())

	// システム開始
	if err := ahs.Start(); err != nil {
		log.Fatalf("Failed to start auto-healing system: %v", err)
	}

	// 簡単なHTTPサーバーを起動してテスト
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write([]byte("OK")); err != nil {
			log.Printf("Failed to write response: %v", err)
		}
	})

	http.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		status := ahs.GetStatus()
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(status); err != nil {
			log.Printf("Failed to encode status: %v", err)
		}
	})

	http.HandleFunc("/alerts", func(w http.ResponseWriter, r *http.Request) {
		alerts := ahs.alertManager.GetAlerts(20)
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(alerts); err != nil {
			log.Printf("Failed to encode alerts: %v", err)
		}
	})

	go func() {
		log.Println("Starting HTTP server on :8080")
		if err := http.ListenAndServe(":8080", nil); err != nil {
			log.Printf("HTTP server error: %v", err)
		}
	}()

	// システム監視
	log.Println("=== Auto-Healing System Demo ===")
	log.Println("Access http://localhost:8080/status for system status")
	log.Println("Access http://localhost:8080/alerts for recent alerts")
	log.Println("Press Ctrl+C to stop")

	// 1分間実行
	time.Sleep(60 * time.Second)

	ahs.Stop()
	log.Println("Demo completed")
}
