package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"
	"time"
)

// TestNewServerConfig ServerConfig作成をテストします
func TestNewServerConfig(t *testing.T) {
	config := NewServerConfig()

	if config.Address != "localhost" {
		t.Errorf("Expected address localhost, got %s", config.Address)
	}
	if config.Port != 8080 {
		t.Errorf("Expected port 8080, got %d", config.Port)
	}
	if config.MaxWorkers <= 0 {
		t.Errorf("Expected positive MaxWorkers, got %d", config.MaxWorkers)
	}
	if config.RequestBuffer <= 0 {
		t.Errorf("Expected positive RequestBuffer, got %d", config.RequestBuffer)
	}
}

// TestNewConcurrentHTTPServer サーバー作成をテストします
func TestNewConcurrentHTTPServer(t *testing.T) {
	config := NewServerConfig()
	config.MaxWorkers = 2

	server := NewConcurrentHTTPServer(config)

	if server == nil {
		t.Fatal("Expected server to be created")
	}
	if server.maxWorkers != 2 {
		t.Errorf("Expected 2 workers, got %d", server.maxWorkers)
	}
	if len(server.workerPool) != 2 {
		t.Errorf("Expected 2 workers in pool, got %d", len(server.workerPool))
	}
}

// TestServerStartAndShutdown サーバーの開始と停止をテストします
func TestServerStartAndShutdown(t *testing.T) {
	config := NewServerConfig()
	config.MaxWorkers = 2
	config.Port = 0 // 自動ポート割り当て

	server := NewConcurrentHTTPServer(config)

	// サーバー開始
	err := server.Start()
	if err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}

	// 二重開始はエラーになることを確認
	err = server.Start()
	if err == nil {
		t.Error("Expected error when starting already running server")
	}

	// 停止
	err = server.Shutdown(5 * time.Second)
	if err != nil {
		t.Errorf("Shutdown failed: %v", err)
	}

	// 二重停止はエラーになることを確認
	err = server.Shutdown(1 * time.Second)
	if err == nil {
		t.Error("Expected error when shutting down already stopped server")
	}
}

// TestRequestProcessing リクエスト処理をテストします
func TestRequestProcessing(t *testing.T) {
	config := NewServerConfig()
	config.Port = 0
	config.MaxWorkers = 2
	config.RequestBuffer = 10

	server := NewConcurrentHTTPServer(config)

	err := server.Start()
	if err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}
	defer func() {
		if err := server.Shutdown(2 * time.Second); err != nil {
			t.Logf("Failed to shutdown server: %v", err)
		}
	}()

	// HTTPリクエストを作成
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()

	// リクエストを処理（ServeHTTPはワーカーの結果を受け取るまでブロックする）
	server.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d", w.Code)
	}

	// 統計を確認
	stats := server.GetStats()
	totalRequests := stats["total_requests"].(int64)
	if totalRequests == 0 {
		t.Error("Expected at least one request to be processed")
	}
}

// TestHealthCheckHandler ヘルスチェックハンドラーをテストします
func TestHealthCheckHandler(t *testing.T) {
	config := NewServerConfig()
	server := NewConcurrentHTTPServer(config)

	req := &HTTPRequest{
		ID:        "test-001",
		Method:    http.MethodGet,
		Path:      "/health",
		Timestamp: time.Now(),
	}

	response, err := server.healthCheckHandler(t.Context(), req)
	if err != nil {
		t.Fatalf("Health check failed: %v", err)
	}

	if response.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", response.StatusCode)
	}

	if !response.Success {
		t.Error("Expected successful response")
	}

	// JSONレスポンスを確認
	var healthData map[string]any
	err = json.Unmarshal(response.Body, &healthData)
	if err != nil {
		t.Fatalf("Failed to parse health response: %v", err)
	}

	if healthData["status"] != "healthy" {
		t.Errorf("Expected healthy status, got %v", healthData["status"])
	}
}

// TestEchoHandler エコーハンドラーをテストします
func TestEchoHandler(t *testing.T) {
	config := NewServerConfig()
	server := NewConcurrentHTTPServer(config)

	testBody := "test request body"
	req := &HTTPRequest{
		ID:        "test-002",
		Method:    http.MethodPost,
		Path:      "/api/echo",
		Body:      []byte(testBody),
		Headers:   map[string]string{"Content-Type": "application/json"},
		ClientIP:  "127.0.0.1",
		UserAgent: "test-client",
		Timestamp: time.Now(),
	}

	response, err := server.echoHandler(t.Context(), req)
	if err != nil {
		t.Fatalf("Echo handler failed: %v", err)
	}

	if response.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", response.StatusCode)
	}

	// エコー内容を確認
	var echoData map[string]any
	err = json.Unmarshal(response.Body, &echoData)
	if err != nil {
		t.Fatalf("Failed to parse echo response: %v", err)
	}

	if echoData["method"] != http.MethodPost {
		t.Errorf("Expected POST method, got %v", echoData["method"])
	}
	if echoData["body"] != testBody {
		t.Errorf("Expected body %s, got %v", testBody, echoData["body"])
	}
}

// TestSlowHandler 遅延ハンドラーをテストします
func TestSlowHandler(t *testing.T) {
	config := NewServerConfig()
	server := NewConcurrentHTTPServer(config)

	req := &HTTPRequest{
		ID:        "test-003",
		Method:    http.MethodGet,
		Path:      "/api/slow",
		Timestamp: time.Now(),
	}

	start := time.Now()

	// コンテキストでタイムアウトテスト
	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()

	response, err := server.slowHandler(ctx, req)
	elapsed := time.Since(start)

	// タイムアウトまたは正常完了のどちらでも許可
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Unexpected error: %v", err)
	}

	if errors.Is(err, context.DeadlineExceeded) {
		// タイムアウトした場合
		if elapsed < 400*time.Millisecond {
			t.Errorf("Expected timeout around 500ms, got %v", elapsed)
		}
	} else {
		// 正常完了した場合
		if response.StatusCode != http.StatusOK {
			t.Errorf("Expected status 200, got %d", response.StatusCode)
		}
	}
}

// TestMiddleware ミドルウェアをテストします
func TestMiddleware(t *testing.T) {
	config := NewServerConfig()
	server := NewConcurrentHTTPServer(config)

	// テスト用ミドルウェア
	var middlewareCalled bool
	testMiddleware := func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, req *HTTPRequest) (*HTTPResponse, error) {
			middlewareCalled = true
			return next(ctx, req)
		}
	}

	server.AddMiddleware(testMiddleware)

	// テストハンドラー
	testHandler := func(ctx context.Context, req *HTTPRequest) (*HTTPResponse, error) {
		return &HTTPResponse{
			StatusCode:  http.StatusOK,
			Success:     true,
			HandlerName: "test",
		}, nil
	}

	// ミドルウェアを適用
	finalHandler := server.middleware.apply(testHandler)

	req := &HTTPRequest{
		ID:        "test-004",
		Method:    http.MethodGet,
		Path:      "/test",
		Timestamp: time.Now(),
	}

	_, err := finalHandler(t.Context(), req)
	if err != nil {
		t.Fatalf("Middleware test failed: %v", err)
	}

	if !middlewareCalled {
		t.Error("Expected middleware to be called")
	}
}

// TestConcurrentRequests 並行リクエストをテストします
func TestConcurrentRequests(t *testing.T) {
	config := NewServerConfig()
	config.Port = 0
	config.MaxWorkers = 3
	config.RequestBuffer = 20

	server := NewConcurrentHTTPServer(config)

	err := server.Start()
	if err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}
	defer func() {
		if err := server.Shutdown(3 * time.Second); err != nil {
			t.Logf("Failed to shutdown server: %v", err)
		}
	}()

	const numRequests = 10
	var wg sync.WaitGroup

	// 複数のgoroutineで並行リクエスト。ServeHTTPは結果を受け取るまでブロックするので、
	// wg.Wait()が返った時点ですべてのリクエストが処理済みになっている。
	for i := range numRequests {
		wg.Go(func() {
			req := httptest.NewRequest(http.MethodGet, "/health", nil)
			w := httptest.NewRecorder()
			server.ServeHTTP(w, req)
			if w.Code != http.StatusOK {
				t.Errorf("request %d: expected status 200, got %d", i, w.Code)
			}
		})
	}

	wg.Wait()

	// 統計確認
	stats := server.GetStats()
	totalRequests := stats["total_requests"].(int64)

	if totalRequests < int64(numRequests) {
		t.Errorf("Expected at least %d requests, got %d", numRequests, totalRequests)
	}
}

// TestWorkerStats ワーカー統計をテストします
func TestWorkerStats(t *testing.T) {
	config := NewServerConfig()
	config.Port = 0
	config.MaxWorkers = 2

	server := NewConcurrentHTTPServer(config)

	err := server.Start()
	if err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}
	defer func() {
		if err := server.Shutdown(2 * time.Second); err != nil {
			t.Logf("Failed to shutdown server: %v", err)
		}
	}()

	// いくつかのリクエストを送信（ServeHTTPは同期的に完了する）
	for range 5 {
		req := httptest.NewRequest(http.MethodGet, "/health", nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)
	}

	// ワーカー統計を確認
	stats := server.GetStats()
	workerStats := stats["worker_stats"].(map[string]any)

	if len(workerStats) != 2 {
		t.Errorf("Expected 2 workers in stats, got %d", len(workerStats))
	}

	// 少なくとも1つのワーカーが処理していることを確認
	foundActiveWorker := false
	for _, workerStat := range workerStats {
		workerData := workerStat.(map[string]any)
		processed := workerData["processed_requests"].(int64)
		if processed > 0 {
			foundActiveWorker = true
			break
		}
	}

	if !foundActiveWorker {
		t.Error("Expected at least one worker to have processed requests")
	}
}

// TestGetStats 統計取得をテストします
func TestGetStats(t *testing.T) {
	config := NewServerConfig()
	server := NewConcurrentHTTPServer(config)

	stats := server.GetStats()

	// 必要なフィールドが存在することを確認
	requiredFields := []string{
		"total_requests", "active_requests", "completed_requests",
		"error_requests", "average_response_time", "requests_per_second",
		"uptime", "worker_count", "worker_stats",
	}

	for _, field := range requiredFields {
		if _, exists := stats[field]; !exists {
			t.Errorf("Missing required field in stats: %s", field)
		}
	}
}

// TestDefaultHandler デフォルトハンドラーをテストします
func TestDefaultHandler(t *testing.T) {
	config := NewServerConfig()
	server := NewConcurrentHTTPServer(config)

	tests := []struct {
		method         string
		path           string
		expectedStatus int
	}{
		{http.MethodGet, "/nonexistent", http.StatusNotFound},
		{http.MethodPost, "/nonexistent", http.StatusMethodNotAllowed},
		{"UNKNOWN", "/nonexistent", http.StatusNotImplemented},
	}

	for _, test := range tests {
		handler := server.getDefaultHandler(test.method, test.path)
		req := &HTTPRequest{
			ID:        "test-default",
			Method:    test.method,
			Path:      test.path,
			Timestamp: time.Now(),
		}

		response, err := handler(t.Context(), req)
		if err != nil {
			t.Fatalf("Default handler failed for %s %s: %v", test.method, test.path, err)
		}

		if response.StatusCode != test.expectedStatus {
			t.Errorf("Expected status %d for %s %s, got %d",
				test.expectedStatus, test.method, test.path, response.StatusCode)
		}

		if response.Success {
			t.Errorf("Default handler should not return success for %s %s", test.method, test.path)
		}
	}
}

// TestRequestQueueFull リクエストキューが満杯の場合をテストします。
// タイミング頼みを避けるため、1本目でワーカーを、2本目でバッファを確実に埋めてから
// 残りを並行投入し、429が返ることを決定的に確認する。
func TestRequestQueueFull(t *testing.T) {
	config := NewServerConfig()
	config.Port = 0
	config.MaxWorkers = 1
	config.RequestBuffer = 1 // 非常に小さなバッファ

	server := NewConcurrentHTTPServer(config)

	release := make(chan struct{})
	blockHandler := func(ctx context.Context, req *HTTPRequest) (*HTTPResponse, error) {
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return &HTTPResponse{StatusCode: http.StatusOK, Success: true, HandlerName: "block"}, nil
	}
	// ハンドラー登録はStart前だけ行う（workerPool[].handlersはロックなしで共有されるため）。
	server.RegisterHandler(http.MethodGet, "/block", blockHandler)

	if err := server.Start(); err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}
	defer func() {
		if err := server.Shutdown(2 * time.Second); err != nil {
			t.Logf("Failed to shutdown server: %v", err)
		}
	}()

	// 1本目: 唯一のワーカーに掴ませてブロックさせる
	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		req := httptest.NewRequest(http.MethodGet, "/block", nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)
	}()
	for server.stats.activeRequests.Load() < 1 {
		time.Sleep(time.Millisecond)
	}

	// 2本目: バッファ(1)を埋める
	secondDone := make(chan struct{})
	go func() {
		defer close(secondDone)
		req := httptest.NewRequest(http.MethodGet, "/block", nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)
	}()
	for len(server.requestQueue) < 1 {
		time.Sleep(time.Millisecond)
	}

	// 3本目以降: ワーカーもバッファも埋まっているので429を期待する
	const extra = 8
	codes := make([]int, extra)
	var wg sync.WaitGroup
	for i := range extra {
		wg.Go(func() {
			req := httptest.NewRequest(http.MethodGet, "/block", nil)
			w := httptest.NewRecorder()
			server.ServeHTTP(w, req)
			codes[i] = w.Code
		})
	}
	wg.Wait()

	close(release)
	<-firstDone
	<-secondDone

	if !slices.Contains(codes, http.StatusTooManyRequests) {
		t.Errorf("Expected at least one 429 Too Many Requests, got %v", codes)
	}
}

// TestConcurrentSubmitAndShutdown 投入とShutdownを並行させてもpanicせず、
// 停止後の投入がエラーになることを確認する回帰テスト。
func TestConcurrentSubmitAndShutdown(t *testing.T) {
	for iter := range 50 {
		config := NewServerConfig()
		config.Port = 0
		config.MaxWorkers = 2
		config.RequestBuffer = 4

		server := NewConcurrentHTTPServer(config)
		if err := server.Start(); err != nil {
			t.Fatalf("iter %d: failed to start server: %v", iter, err)
		}

		var wg sync.WaitGroup
		for range 20 {
			wg.Go(func() {
				req := httptest.NewRequest(http.MethodGet, "/health", nil)
				w := httptest.NewRecorder()
				server.ServeHTTP(w, req)
			})
		}

		if err := server.Shutdown(2 * time.Second); err != nil {
			t.Fatalf("iter %d: shutdown failed: %v", iter, err)
		}
		wg.Wait()

		// 停止後の投入は503になる
		req := httptest.NewRequest(http.MethodGet, "/health", nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)
		if w.Code != http.StatusServiceUnavailable {
			t.Errorf("iter %d: expected 503 after shutdown, got %d", iter, w.Code)
		}
	}
}

// ベンチマークテスト
func BenchmarkHealthCheckHandler(b *testing.B) {
	config := NewServerConfig()
	server := NewConcurrentHTTPServer(config)

	req := &HTTPRequest{
		ID:        "bench-001",
		Method:    http.MethodGet,
		Path:      "/health",
		Timestamp: time.Now(),
	}

	for b.Loop() {
		_, err := server.healthCheckHandler(b.Context(), req)
		if err != nil {
			b.Fatalf("Benchmark failed: %v", err)
		}
	}
}
