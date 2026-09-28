package main

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

// MockHealthChecker テスト用の健全性チェッカーです
// テストの goroutine が状態を書き換え、監視ループの goroutine が読むので、共有する状態はすべて同期して扱います。
type MockHealthChecker struct {
	name       string
	critical   bool
	shouldFail atomic.Bool

	mu      sync.Mutex
	status  string
	message string
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
	if mhc.shouldFail.Load() {
		return HealthStatus{
			Name:      mhc.name,
			Status:    "critical",
			Message:   "Simulated failure",
			Timestamp: time.Now(),
			Score:     0.0,
		}
	}

	mhc.mu.Lock()
	status, message := mhc.status, mhc.message
	mhc.mu.Unlock()

	return HealthStatus{
		Name:      mhc.name,
		Status:    status,
		Message:   message,
		Timestamp: time.Now(),
		Score:     1.0,
	}
}

func (mhc *MockHealthChecker) SetStatus(status, message string) {
	mhc.mu.Lock()
	defer mhc.mu.Unlock()
	mhc.status = status
	mhc.message = message
}

func (mhc *MockHealthChecker) SimulateFailure() {
	mhc.shouldFail.Store(true)
}

func (mhc *MockHealthChecker) SimulateRecovery() {
	mhc.shouldFail.Store(false)
}

// MockHealingAction テスト用の復旧アクションです
type MockHealingAction struct {
	name           string
	handledTypes   []string
	shouldFail     atomic.Bool
	executionCount atomic.Int64
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
	return slices.Contains(mha.handledTypes, issueType)
}

func (mha *MockHealingAction) EstimatedDuration() time.Duration {
	return mha.duration
}

func (mha *MockHealingAction) Execute(ctx context.Context, issue HealthIssue) error {
	mha.executionCount.Add(1)

	time.Sleep(mha.duration)

	if mha.shouldFail.Load() {
		return errors.New("simulated healing action failure")
	}

	return nil
}

func (mha *MockHealingAction) GetExecutionCount() int64 {
	return mha.executionCount.Load()
}

func (mha *MockHealingAction) SetShouldFail(shouldFail bool) {
	mha.shouldFail.Store(shouldFail)
}

func TestAutoHealingSystem_BasicOperation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ahs := NewAutoHealingSystem(100*time.Millisecond, 2)

		// モックチェッカーとアクションを追加
		mockChecker := NewMockHealthChecker("test_service", true)
		mockAction := NewMockHealingAction("restart_service", []string{"test_service"})

		ahs.AddHealthChecker(mockChecker)
		ahs.AddHealingAction("test_service", mockAction)

		// システム開始
		if err := ahs.Start(); err != nil {
			t.Fatalf("Failed to start auto-healing system: %v", err)
		}
		defer ahs.Stop()

		// 100ms 間隔なので 300ms で 3 回チェックされる
		synctest.Sleep(300 * time.Millisecond)

		status := ahs.GetStatus()
		if !status["is_running"].(bool) {
			t.Error("Expected system to be running")
		}

		if got := status["total_checks"].(int64); got != 3 {
			t.Errorf("Expected 3 health checks, got %d", got)
		}

		if got := mockAction.GetExecutionCount(); got != 0 {
			t.Errorf("Expected no healing action while healthy, got %d", got)
		}
	})
}

func TestAutoHealingSystem_IssueDetectionAndHealing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ahs := NewAutoHealingSystem(100*time.Millisecond, 2)

		mockChecker := NewMockHealthChecker("test_service", true)
		mockAction := NewMockHealingAction("fix_service", []string{"test_service"})

		ahs.AddHealthChecker(mockChecker)
		ahs.AddHealingAction("test_service", mockAction)

		if err := ahs.Start(); err != nil {
			t.Fatalf("Failed to start auto-healing system: %v", err)
		}
		defer ahs.Stop()

		// 最初は正常状態
		synctest.Sleep(200 * time.Millisecond)

		// 障害を発生させる
		mockChecker.SimulateFailure()

		// 復旧アクションが実行されるまで待機
		synctest.Sleep(500 * time.Millisecond)

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

		// Stop は実行中の復旧処理が終わるまで待つので、この後は件数が動かない
		ahs.Stop()

		// 復旧確認に失敗した成功アクションも、履歴には1回の実行として1件だけ残る
		ahs.mutex.RLock()
		historyLen := len(ahs.actionHistory)
		ahs.mutex.RUnlock()
		if want := int(mockAction.GetExecutionCount()); historyLen != want {
			t.Errorf("Expected %d action history entries (one per execution), got %d", want, historyLen)
		}
	})
}

func TestAutoHealingSystem_HealingActionRetry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ahs := NewAutoHealingSystem(50*time.Millisecond, 3)
		ahs.escalationDelay = 100 * time.Millisecond // 短い遅延

		mockChecker := NewMockHealthChecker("failing_service", true)
		mockAction := NewMockHealingAction("unreliable_fix", []string{"failing_service"})

		// 最初は失敗するように設定
		mockAction.SetShouldFail(true)
		mockChecker.SimulateFailure()

		ahs.AddHealthChecker(mockChecker)
		ahs.AddHealingAction("failing_service", mockAction)

		if err := ahs.Start(); err != nil {
			t.Fatalf("Failed to start auto-healing system: %v", err)
		}
		defer ahs.Stop()

		// リトライが発生するまで待機
		synctest.Sleep(1 * time.Second)

		// 複数回実行されたことを確認
		if executionCount := mockAction.GetExecutionCount(); executionCount < 2 {
			t.Errorf("Expected multiple retry attempts, got %d", executionCount)
		}

		// アクションを成功するように変更
		mockAction.SetShouldFail(false)
		mockChecker.SimulateRecovery()

		// 実行中のリトライが復旧確認で終わるまで待つ
		synctest.Sleep(1 * time.Second)
		settled := mockAction.GetExecutionCount()

		// 復旧後は新しい問題が検出されないので、実行回数は増えない
		synctest.Sleep(500 * time.Millisecond)
		if got := mockAction.GetExecutionCount(); got != settled {
			t.Errorf("Expected no more healing actions after recovery, got %d -> %d", settled, got)
		}
	})
}

func TestAutoHealingSystem_MultipleCheckers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
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

		if err := ahs.Start(); err != nil {
			t.Fatalf("Failed to start auto-healing system: %v", err)
		}
		defer ahs.Stop()

		// すべてのサービスで障害を発生
		checker1.SimulateFailure()
		checker2.SimulateFailure()
		checker3.SimulateFailure()

		// 復旧処理を待機
		synctest.Sleep(800 * time.Millisecond)

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
	})
}

func TestAutoHealingSystem_AlertGeneration(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ahs := NewAutoHealingSystem(100*time.Millisecond, 2)

		mockChecker := NewMockHealthChecker("alerting_service", true)
		mockAction := NewMockHealingAction("fix_alerting", []string{"alerting_service"})

		ahs.AddHealthChecker(mockChecker)
		ahs.AddHealingAction("alerting_service", mockAction)

		// コールバックは別の goroutine で呼ばれる
		var alertReceived atomic.Bool
		ahs.alertManager.AddCallback(func(alert Alert) {
			if alert.Level == AlertLevelWarning || alert.Level == AlertLevelError {
				alertReceived.Store(true)
			}
		})

		if err := ahs.Start(); err != nil {
			t.Fatalf("Failed to start auto-healing system: %v", err)
		}
		defer ahs.Stop()

		// 障害を発生させる
		mockChecker.SimulateFailure()

		// アラートが生成されるまで待機
		synctest.Sleep(500 * time.Millisecond)

		if !alertReceived.Load() {
			t.Error("Expected alert to be generated")
		}

		// アラート履歴を確認
		alerts := ahs.alertManager.GetAlerts(10)
		if len(alerts) == 0 {
			t.Error("Expected alerts to be stored in history")
		}
	})
}

func TestAutoHealingSystem_StatusReporting(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ahs := NewAutoHealingSystem(200*time.Millisecond, 2)

		mockChecker := NewMockHealthChecker("status_service", true)
		ahs.AddHealthChecker(mockChecker)

		if err := ahs.Start(); err != nil {
			t.Fatalf("Failed to start auto-healing system: %v", err)
		}
		defer ahs.Stop()

		// しばらく実行させる
		synctest.Sleep(500 * time.Millisecond)

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
	})
}

func TestAutoHealingSystem_StartStop(t *testing.T) {
	ahs := NewAutoHealingSystem(100*time.Millisecond, 2)

	// 最初は停止状態
	status := ahs.GetStatus()
	if status["is_running"].(bool) {
		t.Error("Expected system to be stopped initially")
	}

	// 開始
	if err := ahs.Start(); err != nil {
		t.Fatalf("Failed to start auto-healing system: %v", err)
	}

	// 実行状態を確認
	status = ahs.GetStatus()
	if !status["is_running"].(bool) {
		t.Error("Expected system to be running after start")
	}

	// 重複開始はエラーになるはず
	if err := ahs.Start(); err == nil {
		t.Error("Expected error when starting already running system")
	}

	// Stop は監視ループの終了まで待ってから戻る
	ahs.Stop()

	status = ahs.GetStatus()
	if status["is_running"].(bool) {
		t.Error("Expected system to be stopped after stop")
	}

	// 停止後の再開始は監視ループが動かないのでエラーにする
	if err := ahs.Start(); err == nil {
		t.Error("Expected error when starting a stopped system")
	}
}

func TestAutoHealingSystem_ConcurrentStartStop(t *testing.T) {
	for range 50 {
		ahs := NewAutoHealingSystem(time.Millisecond, 1)
		ahs.AddHealthChecker(NewMockHealthChecker("svc", true))

		var wg sync.WaitGroup
		wg.Go(func() { _ = ahs.Start() })
		wg.Go(ahs.Stop)
		wg.Wait()
		ahs.Stop()

		if err := ahs.Start(); err == nil {
			t.Fatal("Expected error when starting after stop")
		}
		if ahs.GetStatus()["is_running"].(bool) {
			t.Fatal("Expected system to stay stopped")
		}
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

	result := checker.Check(t.Context())

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

	result := checker.Check(t.Context())

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

	issue := HealthIssue{
		Type:        "memory_usage",
		Severity:    SeverityHigh,
		Description: "High memory usage detected",
	}

	if err := action.Execute(t.Context(), issue); err != nil {
		t.Errorf("Expected no error from GC trigger, got %v", err)
	}

	if duration := action.EstimatedDuration(); duration <= 0 {
		t.Error("Expected positive estimated duration")
	}
}

// ベンチマークテスト
func BenchmarkAutoHealingSystem_HealthCheck(b *testing.B) {
	ahs := NewAutoHealingSystem(time.Hour, 1) // 長い間隔でベンチマーク中の実行を防ぐ

	checker := NewMockHealthChecker("benchmark_service", false)
	ahs.AddHealthChecker(checker)

	ctx := b.Context()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			checker.Check(ctx)
		}
	})
}
