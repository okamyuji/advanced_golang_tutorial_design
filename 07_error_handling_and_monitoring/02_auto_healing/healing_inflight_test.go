package main

import (
	"context"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

// slowAction 実行中の数と、その最大値を記録する復旧アクション
type slowAction struct {
	running, maxRunning atomic.Int64
}

func (a *slowAction) Name() string                     { return "slow" }
func (a *slowAction) CanHandle(string) bool            { return true }
func (a *slowAction) EstimatedDuration() time.Duration { return time.Second }
func (a *slowAction) Execute(ctx context.Context, _ HealthIssue) error {
	n := a.running.Add(1)
	defer a.running.Add(-1)
	for {
		m := a.maxRunning.Load()
		if n <= m || a.maxRunning.CompareAndSwap(m, n) {
			break
		}
	}
	select {
	case <-time.After(time.Second):
	case <-ctx.Done():
	}
	return nil
}

// TestAutoHealingSystem_同じ障害の復旧は同時に1つだけ走る
func TestAutoHealingSystem_同じ障害の復旧は同時に1つだけ走る(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ahs := NewAutoHealingSystem(100*time.Millisecond, 1)
		checker := NewMockHealthChecker("db", true)
		action := &slowAction{}
		ahs.AddHealthChecker(checker)
		ahs.AddHealingAction("db", action)

		checker.SimulateFailure()
		if err := ahs.Start(); err != nil {
			t.Fatal(err)
		}
		// 復旧に1秒かかる間に、監視は10回ほど同じ障害を検出する
		synctest.Sleep(2 * time.Second)
		ahs.Stop()

		if got := action.maxRunning.Load(); got != 1 {
			t.Fatalf("同じ障害の復旧が同時に%d個走った, want 1", got)
		}
	})
}
