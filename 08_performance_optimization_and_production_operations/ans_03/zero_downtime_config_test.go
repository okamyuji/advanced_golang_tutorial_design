package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// newTestSystem 設定ファイル・バックアップ・監査ログをすべて一時ディレクトリに向けたシステムを作る
func newTestSystem(t *testing.T) *ZeroDowntimeConfigSystem {
	t.Helper()
	dir := t.TempDir()
	zcs := NewZeroDowntimeConfigSystem(filepath.Join(dir, "app_config.json"))
	zcs.configManager.backupDir = filepath.Join(dir, "config_backups")
	zcs.auditLogger.logFile = filepath.Join(dir, "config_audit.log")
	zcs.httpServer.Addr = "127.0.0.1:0"
	if err := os.MkdirAll(zcs.configManager.backupDir, 0o755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}
	return zcs
}

func validConfigJSON(t *testing.T, version string) []byte {
	t.Helper()
	cfg := ApplicationConfig{
		Version:  version,
		Server:   &ServerConfig{Port: 8081, ReadTimeout: 5 * time.Second},
		Database: &DatabaseConfig{Host: "db", Port: 5432},
	}
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}
	return data
}

func postConfig(zcs *ZeroDowntimeConfigSystem, body []byte) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/api/config", strings.NewReader(string(body)))
	w := httptest.NewRecorder()
	zcs.httpServer.Handler.ServeHTTP(w, req)
	return w
}

// TestPerformHealthChecksDuringRolloutDoesNotDeadlock ロールアウト中にヘルスチェックが失敗しても、ヘルスチェック処理が止まらないことを確かめる
func TestPerformHealthChecksDuringRolloutDoesNotDeadlock(t *testing.T) {
	zcs := newTestSystem(t)
	if err := zcs.loadInitialConfig(); err != nil {
		t.Fatalf("loadInitialConfig failed: %v", err)
	}
	zcs.healthChecker.checks["always_fail"] = func(*ApplicationConfig) error {
		return errors.New("失敗")
	}
	zcs.rolloutManager.currentRollout = &RolloutExecution{
		ID:       "rollout_test",
		Strategy: zcs.rolloutManager.rolloutStrategy,
		Status:   "in_progress",
		Metrics:  &RolloutMetrics{},
	}

	done := make(chan struct{})
	go func() {
		zcs.performHealthChecks()
		close(done)
	}()
	// 返ってくるかどうかだけを見る。時間は止まったことを判定するための上限
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("performHealthChecks が返らない（デッドロック）")
	}

	req := httptest.NewRequest(http.MethodGet, "/api/config/health", nil)
	w := httptest.NewRecorder()
	zcs.httpServer.Handler.ServeHTTP(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("失敗したチェックがあるのに status=%d", w.Code)
	}
}

// TestConfigAPIPostAppliesPostedConfig POST /api/config で送った設定がロールアウト対象になることを確かめる
func TestConfigAPIPostAppliesPostedConfig(t *testing.T) {
	zcs := newTestSystem(t)
	if err := zcs.loadInitialConfig(); err != nil {
		t.Fatalf("loadInitialConfig failed: %v", err)
	}

	w := postConfig(zcs, validConfigJSON(t, "2.0.0"))
	if w.Code != http.StatusAccepted {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}

	event := <-zcs.updateChan
	if err := zcs.processConfigUpdate(event); err != nil {
		t.Fatalf("processConfigUpdate failed: %v", err)
	}

	zcs.configManager.mutex.RLock()
	pending := zcs.configManager.pendingConfig
	zcs.configManager.mutex.RUnlock()
	if pending == nil || pending.Version != "2.0.0" {
		t.Fatalf("送った設定がロールアウト対象になっていない: %+v", pending)
	}
}

// TestConfigAPIPostPartialConfigIsRejected server や database を欠いた設定は panic せずに検証エラーになることを確かめる
func TestConfigAPIPostPartialConfigIsRejected(t *testing.T) {
	zcs := newTestSystem(t)
	if err := zcs.loadInitialConfig(); err != nil {
		t.Fatalf("loadInitialConfig failed: %v", err)
	}

	w := postConfig(zcs, []byte(`{"version":"3.0.0"}`))
	if w.Code != http.StatusAccepted {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}

	event := <-zcs.updateChan
	if err := zcs.processConfigUpdate(event); err == nil {
		t.Fatal("server/database を欠いた設定が受け入れられた")
	}
}

// TestRolloutStatusIncludesTrafficSplitting ロールアウト状況 API がトラフィック分散の状態を返すことを確かめる
func TestRolloutStatusIncludesTrafficSplitting(t *testing.T) {
	zcs := newTestSystem(t)
	if err := zcs.loadInitialConfig(); err != nil {
		t.Fatalf("loadInitialConfig failed: %v", err)
	}
	w := postConfig(zcs, validConfigJSON(t, "2.0.0"))
	if w.Code != http.StatusAccepted {
		t.Fatalf("status=%d", w.Code)
	}
	if err := zcs.processConfigUpdate(<-zcs.updateChan); err != nil {
		t.Fatalf("processConfigUpdate failed: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/rollout/status", nil)
	rec := httptest.NewRecorder()
	zcs.httpServer.Handler.ServeHTTP(rec, req)

	var resp struct {
		TrafficSplitting map[string]any `json:"traffic_splitting"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("Decode failed: %v", err)
	}
	if got, ok := resp.TrafficSplitting["target_percent"].(float64); !ok || got != 10 {
		t.Fatalf("traffic_splitting.target_percent が 10 ではない: %v", resp.TrafficSplitting)
	}
}

// TestFileChangeIsDetectedAndApplied 設定ファイルの書き換えを検出して、新しい版をロールアウト対象にすることを確かめる
func TestFileChangeIsDetectedAndApplied(t *testing.T) {
	zcs := newTestSystem(t)
	if err := zcs.loadInitialConfig(); err != nil {
		t.Fatalf("loadInitialConfig failed: %v", err)
	}
	path := zcs.configManager.configFile
	stat, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	watcher := &FileWatcher{filePath: path, lastModTime: stat.ModTime(), lastSize: stat.Size()}

	if err := os.WriteFile(path, validConfigJSON(t, "2.1.0"), 0o644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}
	// ファイルシステムの時刻の粒度に依存しないよう、更新時刻を明示的に進める
	later := stat.ModTime().Add(time.Second)
	if err := os.Chtimes(path, later, later); err != nil {
		t.Fatalf("Chtimes failed: %v", err)
	}

	if !zcs.checkFileChange(watcher) {
		t.Fatal("ファイル変更が検出されない")
	}
	if err := zcs.processConfigUpdate(&ConfigUpdateEvent{Type: "file_change", UserID: "system"}); err != nil {
		t.Fatalf("processConfigUpdate failed: %v", err)
	}
	zcs.configManager.mutex.RLock()
	pending := zcs.configManager.pendingConfig
	zcs.configManager.mutex.RUnlock()
	if pending == nil || pending.Version != "2.1.0" {
		t.Fatalf("ファイルの新しい版がロールアウト対象になっていない: %+v", pending)
	}
}

// TestStatusReadersConcurrentWithWriters ロールアウト監視・ヘルスチェックの書き込みと、状態 API の読み出しを並行させる
func TestStatusReadersConcurrentWithWriters(t *testing.T) {
	zcs := newTestSystem(t)
	if err := zcs.loadInitialConfig(); err != nil {
		t.Fatalf("loadInitialConfig failed: %v", err)
	}
	if w := postConfig(zcs, validConfigJSON(t, "2.0.0")); w.Code != http.StatusAccepted {
		t.Fatalf("status=%d", w.Code)
	}
	if err := zcs.processConfigUpdate(<-zcs.updateChan); err != nil {
		t.Fatalf("processConfigUpdate failed: %v", err)
	}

	var wg sync.WaitGroup
	wg.Go(func() {
		for range 20 {
			zcs.monitorRolloutProgress()
			zcs.performHealthChecks()
		}
	})
	wg.Go(func() {
		for range 20 {
			zcs.checkRollbackTriggers()
		}
	})
	for _, path := range []string{"/api/rollout/status", "/api/config/health", "/api/config", "/api/config/versions", "/api/config/audit"} {
		wg.Go(func() {
			for range 20 {
				req := httptest.NewRequest(http.MethodGet, path, nil)
				zcs.httpServer.Handler.ServeHTTP(httptest.NewRecorder(), req)
			}
		})
	}
	wg.Wait()
}

// TestStopConcurrentWithSubmit 投入と停止を並行させても panic せず、停止後の投入がエラーになることを確かめる
func TestStopConcurrentWithSubmit(t *testing.T) {
	for range 50 {
		zcs := newTestSystem(t)
		if err := zcs.Start(t.Context()); err != nil {
			t.Fatalf("Start failed: %v", err)
		}

		var wg sync.WaitGroup
		wg.Go(func() {
			for range 10 {
				postConfig(zcs, validConfigJSON(t, "2.0.0"))
				req := httptest.NewRequest(http.MethodPost, "/api/config/reload", nil)
				zcs.httpServer.Handler.ServeHTTP(httptest.NewRecorder(), req)
			}
		})
		if err := zcs.Stop(); err != nil {
			t.Fatalf("Stop failed: %v", err)
		}
		wg.Wait()

		if err := zcs.Stop(); err != nil {
			t.Fatalf("二度目の Stop がエラーを返した: %v", err)
		}
		if w := postConfig(zcs, validConfigJSON(t, "2.0.0")); w.Code != http.StatusServiceUnavailable {
			t.Fatalf("停止後の投入が拒否されていない: status=%d", w.Code)
		}
	}
}

func TestValidateConfigRejectsMissingSections(t *testing.T) {
	zcs := newTestSystem(t)
	cases := map[string]*ApplicationConfig{
		"server なし":   {Database: &DatabaseConfig{Host: "db", Port: 5432}},
		"database なし": {Server: &ServerConfig{Port: 8080, ReadTimeout: time.Second}},
	}
	for name, cfg := range cases {
		t.Run(name, func(t *testing.T) {
			if err := zcs.validateConfig(cfg); err == nil {
				t.Fatal("欠けた設定が検証を通った")
			}
		})
	}
}
