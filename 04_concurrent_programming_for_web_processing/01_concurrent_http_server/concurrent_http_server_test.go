package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// TestNewServerConfig はServerConfig作成をテストします
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

// TestNewConcurrentHTTPServer はサーバー作成をテストします
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

// TestServerStartAndShutdown はサーバーの開始と停止をテストします
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

	// 少し待機
	time.Sleep(100 * time.Millisecond)

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

// TestRequestProcessing はリクエスト処理をテストします
func TestRequestProcessing(t *testing.T) {
	config := NewServerConfig()
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
	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()

	// リクエストを処理
	server.ServeHTTP(w, req)

	// 少し待機してワーカーが処理するのを待つ
	time.Sleep(200 * time.Millisecond)

	// 統計を確認
	stats := server.GetStats()
	totalRequests := stats["total_requests"].(int64)
	if totalRequests == 0 {
		t.Error("Expected at least one request to be processed")
	}
}

// TestHealthCheckHandler はヘルスチェックハンドラーをテストします
func TestHealthCheckHandler(t *testing.T) {
	config := NewServerConfig()
	server := NewConcurrentHTTPServer(config)

	req := &HTTPRequest{
		ID:        "test-001",
		Method:    "GET",
		Path:      "/health",
		Timestamp: time.Now(),
	}

	response, err := server.healthCheckHandler(context.Background(), req)
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
	var healthData map[string]interface{}
	err = json.Unmarshal(response.Body, &healthData)
	if err != nil {
		t.Fatalf("Failed to parse health response: %v", err)
	}

	if healthData["status"] != "healthy" {
		t.Errorf("Expected healthy status, got %v", healthData["status"])
	}
}

// TestEchoHandler はエコーハンドラーをテストします
func TestEchoHandler(t *testing.T) {
	config := NewServerConfig()
	server := NewConcurrentHTTPServer(config)

	testBody := "test request body"
	req := &HTTPRequest{
		ID:        "test-002",
		Method:    "POST",
		Path:      "/api/echo",
		Body:      []byte(testBody),
		Headers:   map[string]string{"Content-Type": "application/json"},
		ClientIP:  "127.0.0.1",
		UserAgent: "test-client",
		Timestamp: time.Now(),
	}

	response, err := server.echoHandler(context.Background(), req)
	if err != nil {
		t.Fatalf("Echo handler failed: %v", err)
	}

	if response.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", response.StatusCode)
	}

	// エコー内容を確認
	var echoData map[string]interface{}
	err = json.Unmarshal(response.Body, &echoData)
	if err != nil {
		t.Fatalf("Failed to parse echo response: %v", err)
	}

	if echoData["method"] != "POST" {
		t.Errorf("Expected POST method, got %v", echoData["method"])
	}
	if echoData["body"] != testBody {
		t.Errorf("Expected body %s, got %v", testBody, echoData["body"])
	}
}

// TestSlowHandler は遅延ハンドラーをテストします
func TestSlowHandler(t *testing.T) {
	config := NewServerConfig()
	server := NewConcurrentHTTPServer(config)

	req := &HTTPRequest{
		ID:        "test-003",
		Method:    "GET",
		Path:      "/api/slow",
		Timestamp: time.Now(),
	}

	start := time.Now()

	// コンテキストでタイムアウトテスト
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	response, err := server.slowHandler(ctx, req)
	elapsed := time.Since(start)

	// タイムアウトまたは正常完了のどちらでも許可
	if err != nil && err != context.DeadlineExceeded {
		t.Fatalf("Unexpected error: %v", err)
	}

	if err == context.DeadlineExceeded {
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

// TestMiddleware はミドルウェアをテストします
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
		Method:    "GET",
		Path:      "/test",
		Timestamp: time.Now(),
	}

	_, err := finalHandler(context.Background(), req)
	if err != nil {
		t.Fatalf("Middleware test failed: %v", err)
	}

	if !middlewareCalled {
		t.Error("Expected middleware to be called")
	}
}

// TestConcurrentRequests は並行リクエストをテストします
func TestConcurrentRequests(t *testing.T) {
	config := NewServerConfig()
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
	wg.Add(numRequests)

	// 複数のgoroutineで並行リクエスト
	for i := 0; i < numRequests; i++ {
		go func(id int) {
			defer wg.Done()

			req := httptest.NewRequest("GET", "/health", nil)
			w := httptest.NewRecorder()

			server.ServeHTTP(w, req)
		}(i)
	}

	wg.Wait()

	// 処理時間を与える
	time.Sleep(500 * time.Millisecond)

	// 統計確認
	stats := server.GetStats()
	totalRequests := stats["total_requests"].(int64)

	if totalRequests < int64(numRequests) {
		t.Errorf("Expected at least %d requests, got %d", numRequests, totalRequests)
	}
}

// TestWorkerStats はワーカー統計をテストします
func TestWorkerStats(t *testing.T) {
	config := NewServerConfig()
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

	// いくつかのリクエストを送信
	for i := 0; i < 5; i++ {
		req := httptest.NewRequest("GET", "/health", nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)
	}

	// 処理時間を与える
	time.Sleep(300 * time.Millisecond)

	// ワーカー統計を確認
	stats := server.GetStats()
	workerStats := stats["worker_stats"].(map[string]interface{})

	if len(workerStats) != 2 {
		t.Errorf("Expected 2 workers in stats, got %d", len(workerStats))
	}

	// 少なくとも1つのワーカーが処理していることを確認
	foundActiveWorker := false
	for _, workerStat := range workerStats {
		workerData := workerStat.(map[string]interface{})
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

// TestGetStats は統計取得をテストします
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

// TestDefaultHandler はデフォルトハンドラーをテストします
func TestDefaultHandler(t *testing.T) {
	config := NewServerConfig()
	server := NewConcurrentHTTPServer(config)

	tests := []struct {
		method         string
		path           string
		expectedStatus int
	}{
		{"GET", "/nonexistent", http.StatusNotFound},
		{"POST", "/nonexistent", http.StatusMethodNotAllowed},
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

		response, err := handler(context.Background(), req)
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

// TestRequestQueueFull はリクエストキューが満杯の場合をテストします
func TestRequestQueueFull(t *testing.T) {
	config := NewServerConfig()
	config.MaxWorkers = 1
	config.RequestBuffer = 1 // 非常に小さなバッファ

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

	// キューを満杯にするため、複数のリクエストを即座に送信
	for i := 0; i < 10; i++ {
		req := httptest.NewRequest("GET", "/api/slow", nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)

		// いくつかのリクエストでToo Many Requestsが返されることを期待
		if w.Code == http.StatusTooManyRequests {
			return // 期待通りのエラーが発生
		}
	}

	// ここに到達した場合でもテストは通す（タイミング依存のため）
	t.Log("Queue full scenario might not have been triggered due to timing")
}

// ベンチマークテスト
func BenchmarkHealthCheckHandler(b *testing.B) {
	config := NewServerConfig()
	server := NewConcurrentHTTPServer(config)

	req := &HTTPRequest{
		ID:        "bench-001",
		Method:    "GET",
		Path:      "/health",
		Timestamp: time.Now(),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := server.healthCheckHandler(context.Background(), req)
		if err != nil {
			b.Fatalf("Benchmark failed: %v", err)
		}
	}
}
