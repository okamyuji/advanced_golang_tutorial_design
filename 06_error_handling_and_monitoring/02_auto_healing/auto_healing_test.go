package main

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// MockHealthChecker はテスト用の健全性チェッカーです
type MockHealthChecker struct {
	name       string
	status     string
	message    string
	critical   bool
	shouldFail bool
}

func NewMockHealthChecker(name string, critical bool) *MockHealthChecker {
	return &MockHealthChecker{
		name:     name,
		status:   "healthy",
		message:  "All systems operational",
		critical: critical,
	}
}

func (mhc *MockHealthChecker) Name() string {
	return mhc.name
}

func (mhc *MockHealthChecker) Critical() bool {
	return mhc.critical
}

func (mhc *MockHealthChecker) Check(ctx context.Context) HealthStatus {
	if mhc.shouldFail {
		return HealthStatus{
			Name:      mhc.name,
			Status:    "critical",
			Message:   "Simulated failure",
			Timestamp: time.Now(),
			Score:     0.0,
		}
	}

	return HealthStatus{
		Name:      mhc.name,
		Status:    mhc.status,
		Message:   mhc.message,
		Timestamp: time.Now(),
		Score:     1.0,
	}
}

func (mhc *MockHealthChecker) SetStatus(status, message string) {
	mhc.status = status
	mhc.message = message
}

func (mhc *MockHealthChecker) SimulateFailure() {
	mhc.shouldFail = true
}

func (mhc *MockHealthChecker) SimulateRecovery() {
	mhc.shouldFail = false
}

// MockHealingAction はテスト用の復旧アクションです
type MockHealingAction struct {
	name           string
	handledTypes   []string
	shouldFail     bool
	executionCount int64
	duration       time.Duration
}

func NewMockHealingAction(name string, handledTypes []string) *MockHealingAction {
	return &MockHealingAction{
		name:         name,
		handledTypes: handledTypes,
		duration:     100 * time.Millisecond,
	}
}

func (mha *MockHealingAction) Name() string {
	return mha.name
}

func (mha *MockHealingAction) CanHandle(issueType string) bool {
	for _, t := range mha.handledTypes {
		if t == issueType {
			return true
		}
	}
	return false
}

func (mha *MockHealingAction) EstimatedDuration() time.Duration {
	return mha.duration
}

func (mha *MockHealingAction) Execute(ctx context.Context, issue HealthIssue) error {
	atomic.AddInt64(&mha.executionCount, 1)

	time.Sleep(mha.duration)

	if mha.shouldFail {
		return errors.New("simulated healing action failure")
	}

	return nil
}

func (mha *MockHealingAction) GetExecutionCount() int64 {
	return atomic.LoadInt64(&mha.executionCount)
}

func (mha *MockHealingAction) SetShouldFail(shouldFail bool) {
	mha.shouldFail = shouldFail
}

func TestAutoHealingSystem_BasicOperation(t *testing.T) {
	ahs := NewAutoHealingSystem(100*time.Millisecond, 2)

	// モックチェッカーとアクションを追加
	mockChecker := NewMockHealthChecker("test_service", true)
	mockAction := NewMockHealingAction("restart_service", []string{"test_service"})

	ahs.AddHealthChecker(mockChecker)
	ahs.AddHealingAction("test_service", mockAction)

	// システム開始
	err := ahs.Start()
	if err != nil {
		t.Fatalf("Failed to start auto-healing system: %v", err)
	}
	defer ahs.Stop()

	// 正常状態での動作確認
	time.Sleep(300 * time.Millisecond)

	status := ahs.GetStatus()
	if !status["is_running"].(bool) {
		t.Error("Expected system to be running")
	}

	if status["total_checks"].(int64) == 0 {
		t.Error("Expected some health checks to be performed")
	}
}

func TestAutoHealingSystem_IssueDetectionAndHealing(t *testing.T) {
	ahs := NewAutoHealingSystem(100*time.Millisecond, 2)

	mockChecker := NewMockHealthChecker("test_service", true)
	mockAction := NewMockHealingAction("fix_service", []string{"test_service"})

	ahs.AddHealthChecker(mockChecker)
	ahs.AddHealingAction("test_service", mockAction)

	err := ahs.Start()
	if err != nil {
		t.Fatalf("Failed to start auto-healing system: %v", err)
	}
	defer ahs.Stop()

	// 最初は正常状態
	time.Sleep(200 * time.Millisecond)

	// 障害を発生させる
	mockChecker.SimulateFailure()

	// 復旧アクションが実行されるまで待機
	time.Sleep(500 * time.Millisecond)

	// 復旧アクションが実行されたことを確認
	if mockAction.GetExecutionCount() == 0 {
		t.Error("Expected healing action to be executed")
	}

	status := ahs.GetStatus()
	if status["total_issues"].(int64) == 0 {
		t.Error("Expected issues to be detected")
	}

	if status["total_actions"].(int64) == 0 {
		t.Error("Expected healing actions to be executed")
	}
}

func TestAutoHealingSystem_HealingActionRetry(t *testing.T) {
	ahs := NewAutoHealingSystem(50*time.Millisecond, 3)
	ahs.escalationDelay = 100 * time.Millisecond // 短い遅延

	mockChecker := NewMockHealthChecker("failing_service", true)
	mockAction := NewMockHealingAction("unreliable_fix", []string{"failing_service"})

	// 最初は失敗するように設定
	mockAction.SetShouldFail(true)
	mockChecker.SimulateFailure()

	ahs.AddHealthChecker(mockChecker)
	ahs.AddHealingAction("failing_service", mockAction)

	err := ahs.Start()
	if err != nil {
		t.Fatalf("Failed to start auto-healing system: %v", err)
	}
	defer ahs.Stop()

	// リトライが発生するまで待機
	time.Sleep(1 * time.Second)

	// 複数回実行されたことを確認
	executionCount := mockAction.GetExecutionCount()
	if executionCount < 2 {
		t.Errorf("Expected multiple retry attempts, got %d", executionCount)
	}

	// アクションを成功するように変更
	mockAction.SetShouldFail(false)
	mockChecker.SimulateRecovery()

	// 復旧処理を待機
	time.Sleep(500 * time.Millisecond)
}

func TestAutoHealingSystem_MultipleCheckers(t *testing.T) {
	ahs := NewAutoHealingSystem(100*time.Millisecond, 2)

	// 複数のチェッカーを追加
	checker1 := NewMockHealthChecker("service_1", true)
	checker2 := NewMockHealthChecker("service_2", false)
	checker3 := NewMockHealthChecker("service_3", true)

	action1 := NewMockHealingAction("fix_service_1", []string{"service_1"})
	action2 := NewMockHealingAction("fix_service_2", []string{"service_2"})
	action3 := NewMockHealingAction("fix_service_3", []string{"service_3"})

	ahs.AddHealthChecker(checker1)
	ahs.AddHealthChecker(checker2)
	ahs.AddHealthChecker(checker3)

	ahs.AddHealingAction("service_1", action1)
	ahs.AddHealingAction("service_2", action2)
	ahs.AddHealingAction("service_3", action3)

	err := ahs.Start()
	if err != nil {
		t.Fatalf("Failed to start auto-healing system: %v", err)
	}
	defer ahs.Stop()

	// すべてのサービスで障害を発生
	checker1.SimulateFailure()
	checker2.SimulateFailure()
	checker3.SimulateFailure()

	// 復旧処理を待機
	time.Sleep(800 * time.Millisecond)

	// すべてのアクションが実行されたことを確認
	if action1.GetExecutionCount() == 0 {
		t.Error("Expected action1 to be executed")
	}
	if action2.GetExecutionCount() == 0 {
		t.Error("Expected action2 to be executed")
	}
	if action3.GetExecutionCount() == 0 {
		t.Error("Expected action3 to be executed")
	}

	status := ahs.GetStatus()
	if status["total_issues"].(int64) < 3 {
		t.Errorf("Expected at least 3 issues, got %d", status["total_issues"].(int64))
	}
}

func TestAutoHealingSystem_AlertGeneration(t *testing.T) {
	ahs := NewAutoHealingSystem(100*time.Millisecond, 2)

	mockChecker := NewMockHealthChecker("alerting_service", true)
	mockAction := NewMockHealingAction("fix_alerting", []string{"alerting_service"})

	ahs.AddHealthChecker(mockChecker)
	ahs.AddHealingAction("alerting_service", mockAction)

	var alertReceived bool
	ahs.alertManager.AddCallback(func(alert Alert) {
		if alert.Level == AlertLevelWarning || alert.Level == AlertLevelError {
			alertReceived = true
		}
	})

	err := ahs.Start()
	if err != nil {
		t.Fatalf("Failed to start auto-healing system: %v", err)
	}
	defer ahs.Stop()

	// 障害を発生させる
	mockChecker.SimulateFailure()

	// アラートが生成されるまで待機
	time.Sleep(500 * time.Millisecond)

	if !alertReceived {
		t.Error("Expected alert to be generated")
	}

	// アラート履歴を確認
	alerts := ahs.alertManager.GetAlerts(10)
	if len(alerts) == 0 {
		t.Error("Expected alerts to be stored in history")
	}
}

func TestAutoHealingSystem_StatusReporting(t *testing.T) {
	ahs := NewAutoHealingSystem(200*time.Millisecond, 2)

	mockChecker := NewMockHealthChecker("status_service", true)
	ahs.AddHealthChecker(mockChecker)

	err := ahs.Start()
	if err != nil {
		t.Fatalf("Failed to start auto-healing system: %v", err)
	}
	defer ahs.Stop()

	// しばらく実行させる
	time.Sleep(500 * time.Millisecond)

	status := ahs.GetStatus()

	// 必要なフィールドが含まれているかチェック
	expectedFields := []string{
		"is_running", "total_checks", "total_issues",
		"total_actions", "total_successful_actions",
		"system_metrics", "recent_alerts",
		"health_checkers", "healing_actions",
	}

	for _, field := range expectedFields {
		if _, exists := status[field]; !exists {
			t.Errorf("Expected field %s to be present in status", field)
		}
	}

	if !status["is_running"].(bool) {
		t.Error("Expected system to be running")
	}

	if status["health_checkers"].(int) != 1 {
		t.Errorf("Expected 1 health checker, got %d", status["health_checkers"].(int))
	}
}

func TestAutoHealingSystem_StartStop(t *testing.T) {
	ahs := NewAutoHealingSystem(100*time.Millisecond, 2)

	// 最初は停止状態
	status := ahs.GetStatus()
	if status["is_running"].(bool) {
		t.Error("Expected system to be stopped initially")
	}

	// 開始
	err := ahs.Start()
	if err != nil {
		t.Fatalf("Failed to start auto-healing system: %v", err)
	}

	// 実行状態を確認
	status = ahs.GetStatus()
	if !status["is_running"].(bool) {
		t.Error("Expected system to be running after start")
	}

	// 重複開始はエラーになるはず
	err = ahs.Start()
	if err == nil {
		t.Error("Expected error when starting already running system")
	}

	// 停止
	ahs.Stop()

	// 停止状態を確認
	time.Sleep(100 * time.Millisecond)
	status = ahs.GetStatus()
	if status["is_running"].(bool) {
		t.Error("Expected system to be stopped after stop")
	}
}

func TestMemoryHealthChecker(t *testing.T) {
	checker := NewMemoryHealthChecker(90.0)

	if checker.Name() != "memory_usage" {
		t.Errorf("Expected name 'memory_usage', got %s", checker.Name())
	}

	if !checker.Critical() {
		t.Error("Expected memory checker to be critical")
	}

	ctx := context.Background()
	result := checker.Check(ctx)

	if result.Name != "memory_usage" {
		t.Errorf("Expected result name 'memory_usage', got %s", result.Name)
	}

	if result.Timestamp.IsZero() {
		t.Error("Expected timestamp to be set")
	}

	if result.Details == nil {
		t.Error("Expected details to be set")
	}

	// メモリ使用量の詳細をチェック
	if _, exists := result.Details["heap_alloc"]; !exists {
		t.Error("Expected heap_alloc in details")
	}

	if _, exists := result.Details["usage_percent"]; !exists {
		t.Error("Expected usage_percent in details")
	}
}

func TestGoroutineHealthChecker(t *testing.T) {
	checker := NewGoroutineHealthChecker(100)

	if checker.Name() != "goroutine_count" {
		t.Errorf("Expected name 'goroutine_count', got %s", checker.Name())
	}

	ctx := context.Background()
	result := checker.Check(ctx)

	if result.Name != "goroutine_count" {
		t.Errorf("Expected result name 'goroutine_count', got %s", result.Name)
	}

	// Goroutine数の詳細をチェック
	if _, exists := result.Details["current_count"]; !exists {
		t.Error("Expected current_count in details")
	}

	if _, exists := result.Details["threshold"]; !exists {
		t.Error("Expected threshold in details")
	}
}

func TestGCTriggerAction(t *testing.T) {
	action := NewGCTriggerAction()

	if action.Name() != "force_gc" {
		t.Errorf("Expected name 'force_gc', got %s", action.Name())
	}

	if !action.CanHandle("memory_usage") {
		t.Error("Expected action to handle memory_usage issues")
	}

	if action.CanHandle("disk_usage") {
		t.Error("Expected action to not handle disk_usage issues")
	}

	ctx := context.Background()
	issue := HealthIssue{
		Type:        "memory_usage",
		Severity:    SeverityHigh,
		Description: "High memory usage detected",
	}

	err := action.Execute(ctx, issue)
	if err != nil {
		t.Errorf("Expected no error from GC trigger, got %v", err)
	}

	duration := action.EstimatedDuration()
	if duration <= 0 {
		t.Error("Expected positive estimated duration")
	}
}

// ベンチマークテスト
func BenchmarkAutoHealingSystem_HealthCheck(b *testing.B) {
	ahs := NewAutoHealingSystem(time.Hour, 1) // 長い間隔でベンチマーク中の実行を防ぐ

	checker := NewMockHealthChecker("benchmark_service", false)
	ahs.AddHealthChecker(checker)

	ctx := context.Background()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			checker.Check(ctx)
		}
	})
}
