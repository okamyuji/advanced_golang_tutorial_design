package main

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestWebScraper_Start(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		scraper := NewWebScraper(2, 1.0)
		scraper.Start()
		defer func() {
			if err := scraper.Shutdown(5 * time.Second); err != nil {
				t.Logf("Failed to shutdown scraper: %v", err)
			}
		}()

		// 起動したgoroutineが待機状態に入るまで待つ
		synctest.Wait()

		// 基本的な状態確認
		total, _, _, _ := scraper.GetStats()
		if total != 0 {
			t.Errorf("Expected 0 initial requests, got %d", total)
		}
	})
}

func TestWebScraper_SubmitURL_Success(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// テスト用HTTPサーバーを作成
		server := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			if _, err := w.Write([]byte("Hello, World!")); err != nil {
				t.Logf("Failed to write response: %v", err)
			}
		}))

		scraper := NewWebScraper(1, 2.0)
		scraper.client = server.Client() // インメモリのネットワークで送る
		scraper.Start()
		defer func() {
			if err := scraper.Shutdown(5 * time.Second); err != nil {
				t.Logf("Failed to shutdown scraper: %v", err)
			}
		}()

		err := scraper.SubmitURL(server.URL, 1)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// 処理完了を待つ
		synctest.Sleep(2 * time.Second)

		total, success, failed, successRate := scraper.GetStats()
		if total != 1 {
			t.Errorf("Expected 1 total request, got %d", total)
		}
		if success != 1 {
			t.Errorf("Expected 1 successful request, got %d", success)
		}
		if failed != 0 {
			t.Errorf("Expected 0 failed requests, got %d", failed)
		}
		if successRate != 100.0 {
			t.Errorf("Expected 100%% success rate, got %.1f%%", successRate)
		}
	})
}

func TestWebScraper_HandleHTTPError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// エラーを返すテスト用サーバー
		server := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			if _, err := w.Write([]byte("Internal Server Error")); err != nil {
				t.Logf("Failed to write response: %v", err)
			}
		}))

		scraper := NewWebScraper(1, 2.0)
		scraper.client = server.Client() // インメモリのネットワークで送る
		scraper.Start()
		defer func() {
			if err := scraper.Shutdown(5 * time.Second); err != nil {
				t.Logf("Failed to shutdown scraper: %v", err)
			}
		}()

		err := scraper.SubmitURL(server.URL, 2) // 最大2回リトライ
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// リトライを含めた処理完了を待つ
		synctest.Sleep(10 * time.Second)

		total, success, failed, _ := scraper.GetStats()

		// 初回 + 2回のリトライ = 3回のリクエスト
		if total < 3 {
			t.Errorf("Expected at least 3 requests (including retries), got %d", total)
		}

		if success != 0 {
			t.Errorf("Expected 0 successful requests, got %d", success)
		}

		if failed == 0 {
			t.Error("Expected some failed requests, got 0")
		}
	})
}

func TestWebScraper_RateLimiting(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// リクエスト回数をカウントするサーバー
		var requestCount atomic.Int64
		server := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requestCount.Add(1)
			w.WriteHeader(http.StatusOK)
		}))

		// 1秒間に1リクエストの制限
		scraper := NewWebScraper(2, 1.0)
		scraper.client = server.Client() // インメモリのネットワークで送る
		scraper.Start()
		defer func() {
			if err := scraper.Shutdown(10 * time.Second); err != nil {
				t.Logf("Failed to shutdown scraper: %v", err)
			}
		}()

		// 3つのURLを即座に送信
		for range 3 {
			err := scraper.SubmitURL(server.URL, 0)
			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}
		}

		// 2秒後にリクエスト数をチェック
		synctest.Sleep(2 * time.Second)
		count1 := requestCount.Load()

		// さらに2秒後にチェック
		synctest.Sleep(2 * time.Second)
		count2 := requestCount.Load()

		// レート制限により、1秒間に1リクエストのペースになっているかチェック
		if count1 > 2 {
			t.Errorf("Rate limiting not working: %d requests in 2 seconds", count1)
		}

		if count2 != 3 {
			t.Errorf("Expected 3 total requests after 4 seconds, got %d", count2)
		}
	})
}

func TestWebScraper_ConcurrentRequests(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var requestCount atomic.Int64
		server := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requestCount.Add(1)
			time.Sleep(100 * time.Millisecond) // 処理時間をシミュレート
			w.WriteHeader(http.StatusOK)
		}))

		// 3並行、毎秒10リクエストの高いレート
		scraper := NewWebScraper(3, 10.0)
		scraper.client = server.Client() // インメモリのネットワークで送る
		scraper.Start()
		defer func() {
			if err := scraper.Shutdown(5 * time.Second); err != nil {
				t.Logf("Failed to shutdown scraper: %v", err)
			}
		}()

		// 5つのURLを送信
		for range 5 {
			err := scraper.SubmitURL(server.URL, 0)
			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}
		}

		// 処理完了を待つ
		synctest.Sleep(3 * time.Second)

		total, success, failed, _ := scraper.GetStats()
		finalCount := requestCount.Load()

		if total != 5 {
			t.Errorf("Expected 5 total requests, got %d", total)
		}
		if success != 5 {
			t.Errorf("Expected 5 successful requests, got %d", success)
		}
		if failed != 0 {
			t.Errorf("Expected 0 failed requests, got %d", failed)
		}
		if finalCount != 5 {
			t.Errorf("Expected 5 actual HTTP requests, got %d", finalCount)
		}
	})
}

func TestWebScraper_Shutdown_Graceful(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var completedRequests atomic.Int64
		server := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(1 * time.Second) // 長時間処理をシミュレート
			completedRequests.Add(1)
			w.WriteHeader(http.StatusOK)
		}))

		scraper := NewWebScraper(2, 5.0)
		scraper.client = server.Client() // インメモリのネットワークで送る
		scraper.Start()

		// 複数のリクエストを送信
		for range 3 {
			if err := scraper.SubmitURL(server.URL, 0); err != nil {
				t.Logf("Failed to submit URL: %v", err)
			}
		}

		// リクエストが実際に開始され処理されるまで待つ（1.5秒）
		synctest.Sleep(1500 * time.Millisecond)

		start := time.Now()
		err := scraper.Shutdown(5 * time.Second)
		duration := time.Since(start)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// いくつかのリクエストが完了していることを確認
		completed := completedRequests.Load()
		if completed == 0 {
			t.Error("No requests were completed during shutdown")
		}

		// 適切な時間でシャットダウンが完了していることを確認
		if duration > 6*time.Second {
			t.Errorf("Shutdown took too long: %v", duration)
		}
	})
}
