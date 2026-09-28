package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"testing"
	"time"
)

func newTestAlert(alertType string, n int) *Alert {
	return &Alert{
		ID:        fmt.Sprintf("%s_%d", alertType, n),
		Timestamp: time.Now(),
		Type:      alertType,
		Severity:  "WARNING",
		Metadata:  map[string]any{"n": n},
		Status:    "active",
	}
}

// TestProcessAlertDeduplicatesByType 同じ種類のアラートは1件にまとめ、履歴を増やさないことを確かめる
func TestProcessAlertDeduplicatesByType(t *testing.T) {
	pms := NewProductionMonitoringSystem()

	pms.processAlert(newTestAlert("cpu_warning", 1))
	pms.processAlert(newTestAlert("cpu_warning", 2))
	pms.processAlert(newTestAlert("memory_warning", 3))

	pms.alertManager.mutex.RLock()
	active := len(pms.alertManager.activeAlerts)
	history := len(pms.alertManager.alertHistory)
	pms.alertManager.mutex.RUnlock()
	if active != 2 || history != 2 {
		t.Fatalf("active=%d history=%d, want 2 and 2", active, history)
	}
}

// TestAnalyzePerformanceWithoutGC GC がまだ起きていない期間の分析結果に NaN が出ないことを確かめる
func TestAnalyzePerformanceWithoutGC(t *testing.T) {
	pms := NewProductionMonitoringSystem()
	got := pms.analyzePerformance([]*SystemMetrics{{GCPauseTime: 0}, {GCPauseTime: 0}})
	if strings.Contains(got, "NaN") {
		t.Fatalf("GC なしの分析結果に NaN が含まれる: %s", got)
	}
}

// TestDashboardAPIReflectsSystemState ダッシュボード API が監視システムの状態を返すことを確かめる
func TestDashboardAPIReflectsSystemState(t *testing.T) {
	pms := NewProductionMonitoringSystem()
	pms.startTime = time.Now().Add(-time.Hour)
	pms.metricsCollector.AddMetrics(&SystemMetrics{Timestamp: time.Now(), GoroutineCount: 42})
	pms.processAlert(newTestAlert("cpu_warning", 1))

	get := func(path string) map[string]any {
		t.Helper()
		w := httptest.NewRecorder()
		pms.dashboardServer.httpServer.Handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
		if w.Code != http.StatusOK {
			t.Fatalf("%s: status=%d", path, w.Code)
		}
		var body map[string]any
		if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
			t.Fatalf("%s: Decode failed: %v", path, err)
		}
		return body
	}

	uptime, err := time.ParseDuration(fmt.Sprint(get("/api/health")["uptime"]))
	if err != nil || uptime < time.Hour {
		t.Errorf("uptime が起動からの時間になっていない: %v (err=%v)", uptime, err)
	}
	if got := get("/api/metrics")["goroutine_count"]; got != float64(42) {
		t.Errorf("/api/metrics の goroutine_count = %v, want 42", got)
	}
	if got := get("/api/alerts")["active_alerts"]; got != float64(1) {
		t.Errorf("/api/alerts の active_alerts = %v, want 1", got)
	}
}

// TestStopConcurrentWithCreateAlert 開始・停止とアラート投入を並行させても panic せず、停止後の再開がエラーになることを確かめる
func TestStopConcurrentWithCreateAlert(t *testing.T) {
	metrics := &SystemMetrics{Timestamp: time.Now()}
	for range 50 {
		pms := NewProductionMonitoringSystem()
		pms.dashboardServer.httpServer.Addr = "127.0.0.1:0"
		if err := pms.Start(t.Context()); err != nil {
			t.Fatalf("Start failed: %v", err)
		}

		var wg sync.WaitGroup
		wg.Go(func() {
			for range 20 {
				pms.createAlert("cpu_warning", "WARNING", "テスト", metrics)
			}
		})
		if err := pms.Stop(); err != nil {
			t.Fatalf("Stop failed: %v", err)
		}
		wg.Wait()

		if err := pms.Stop(); err != nil {
			t.Fatalf("二度目の Stop がエラーを返した: %v", err)
		}
		if err := pms.Start(t.Context()); err == nil {
			t.Fatal("停止後の Start がエラーになっていない")
		}
	}
}

// TestReadersConcurrentWithWriters メトリクスとアラートの書き込みと、読み出し・配信を並行させる
func TestReadersConcurrentWithWriters(t *testing.T) {
	pms := NewProductionMonitoringSystem()
	done := make(chan struct{})

	var wg sync.WaitGroup
	wg.Go(func() {
		defer close(done)
		for i := range 200 {
			pms.metricsCollector.AddMetrics(&SystemMetrics{Timestamp: time.Now(), GoroutineCount: i})
			pms.processAlert(newTestAlert("cpu_warning", i))
		}
	})
	wg.Go(func() {
		for {
			select {
			case update := <-pms.dashboardServer.updateChannel:
				pms.broadcastUpdate(update)
			case <-done:
				return
			}
		}
	})
	wg.Go(func() {
		for {
			select {
			case <-done:
				return
			default:
			}
			pms.metricsCollector.GetHistoricalData(10)
			pms.metricsCollector.GetCurrentMetrics()
		}
	})
	wg.Wait()
}

// TestGCPauseTimeCountsOnlyNewGCs GC が起きていない間の計測では、前回と同じ GC 停止時間を数え直さないことを確かめる
func TestGCPauseTimeCountsOnlyNewGCs(t *testing.T) {
	pms := NewProductionMonitoringSystem()
	runtime.GC()
	first := pms.collectSystemMetrics()
	if first.GCPauseTime == 0 {
		t.Fatal("直前の GC の停止時間が記録されていない")
	}
	// 2回目の計測までに GC が走らないよう止めておく
	defer debug.SetGCPercent(debug.SetGCPercent(-1))
	second := pms.collectSystemMetrics()
	if second.NumGC == first.NumGC && second.GCPauseTime != 0 {
		t.Fatalf("新しい GC がないのに GCPauseTime=%d", second.GCPauseTime)
	}
}

// TestStopWhileHealthRequestsInFlight ヘルス API へのリクエスト中に停止しても、停止が待たされないことを確かめる
func TestStopWhileHealthRequestsInFlight(t *testing.T) {
	pms := NewProductionMonitoringSystem()
	pms.dashboardServer.httpServer.Addr = "127.0.0.1:0"
	if err := pms.Start(t.Context()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	url := "http://" + pms.dashboardServer.httpServer.Addr + "/api/health"

	done := make(chan struct{})
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for {
				select {
				case <-done:
					return
				default:
				}
				resp, err := http.Get(url)
				if err != nil {
					continue
				}
				if err := resp.Body.Close(); err != nil {
					t.Errorf("Close failed: %v", err)
				}
			}
		})
	}

	start := time.Now()
	if err := pms.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}
	elapsed := time.Since(start)
	close(done)
	wg.Wait()

	// Shutdown の待ち時間の上限は5秒。処理中のリクエストが停止を妨げていなければ、はるかに短く終わる
	if elapsed > 2*time.Second {
		t.Fatalf("停止に %v かかった", elapsed)
	}
}
