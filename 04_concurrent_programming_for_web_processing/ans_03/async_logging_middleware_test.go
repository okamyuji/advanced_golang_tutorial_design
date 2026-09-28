package main

import (
	"bufio"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func newTestMiddleware(t *testing.T) (*AsyncLoggingMiddleware, string) {
	t.Helper()

	outputFile := filepath.Join(t.TempDir(), "access.log")
	config := LoggingConfig{
		BatchSize:     100,
		FlushInterval: time.Hour, // Shutdownでの明示flushだけをテストで使う
		MaxQueueSize:  1000,
		OutputFile:    outputFile,
		RetryAttempts: 1,
		RetryDelay:    time.Millisecond,
	}

	return NewAsyncLoggingMiddleware(config), outputFile
}

func countLogLines(t *testing.T, path string) int {
	t.Helper()

	file, err := os.Open(path)
	if os.IsNotExist(err) {
		return 0
	}
	if err != nil {
		t.Fatalf("failed to open log file: %v", err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			t.Errorf("failed to close log file: %v", err)
		}
	}()

	count := 0
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		if scanner.Text() != "" {
			count++
		}
	}
	return count
}

// TestMiddlewareLogsAndFlushesOnShutdown リクエストがログキューに乗り、Shutdownで
// 確実にファイルへ書き込まれることを確認します（Sleep待ちではなくShutdownの完了で判定）。
func TestMiddlewareLogsAndFlushesOnShutdown(t *testing.T) {
	middleware, outputFile := newTestMiddleware(t)

	handler := middleware.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/test", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if err := middleware.Shutdown(t.Context()); err != nil {
		t.Fatalf("Shutdown failed: %v", err)
	}

	if got := countLogLines(t, outputFile); got != 1 {
		t.Errorf("expected 1 log line after shutdown, got %d", got)
	}

	metrics := middleware.GetMetrics()
	if metrics.TotalRequests != 1 {
		t.Errorf("expected 1 total request, got %d", metrics.TotalRequests)
	}
	if metrics.ProcessedLogs != 1 {
		t.Errorf("expected 1 processed log, got %d", metrics.ProcessedLogs)
	}

	// ログ内容を確認
	file, err := os.Open(outputFile)
	if err != nil {
		t.Fatalf("failed to open log file: %v", err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			t.Errorf("failed to close log file: %v", err)
		}
	}()

	var entry LogEntry
	if err := json.NewDecoder(file).Decode(&entry); err != nil {
		t.Fatalf("failed to decode log entry: %v", err)
	}
	if entry.Method != http.MethodGet || entry.URL != "/api/test" {
		t.Errorf("unexpected log entry: %+v", entry)
	}
}

// TestConcurrentRequestsAndShutdown 並行リクエストとShutdownを組み合わせても
// panicせず、送信した件数がすべてメトリクスとログファイルに反映されることを確認する
// 回帰テスト（cancel直前のログ取りこぼし）。
func TestConcurrentRequestsAndShutdown(t *testing.T) {
	for iter := range 20 {
		middleware, outputFile := newTestMiddleware(t)

		handler := middleware.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))

		const numRequests = 30
		var wg sync.WaitGroup
		for range numRequests {
			wg.Go(func() {
				req := httptest.NewRequest(http.MethodGet, "/api/test", nil)
				w := httptest.NewRecorder()
				handler.ServeHTTP(w, req)
			})
		}
		wg.Wait()

		if err := middleware.Shutdown(t.Context()); err != nil {
			t.Fatalf("iter %d: shutdown failed: %v", iter, err)
		}

		metrics := middleware.GetMetrics()
		if metrics.TotalRequests != numRequests {
			t.Errorf("iter %d: expected %d total requests, got %d", iter, numRequests, metrics.TotalRequests)
		}
		if got := countLogLines(t, outputFile); got != numRequests {
			t.Errorf("iter %d: expected %d log lines, got %d", iter, numRequests, got)
		}
	}
}
