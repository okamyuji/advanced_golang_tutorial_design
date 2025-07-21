package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// ScrapingTaskはスクレイピングタスクを表現します
type ScrapingTask struct {
	URL        string
	Headers    map[string]string
	RetryCount int
	MaxRetries int
	Timeout    time.Duration
}

// ScrapingResultはスクレイピング結果を表現します
type ScrapingResult struct {
	URL           string
	StatusCode    int
	ContentLength int64
	Duration      time.Duration
	Success       bool
	Error         error
	RetryCount    int
}

// WebScraperはWebスクレイピング専用のワーカープールです
type WebScraper struct {
	// HTTP設定
	client        *http.Client
	rateLimiter   *time.Ticker
	maxConcurrent int

	// タスク管理
	taskQueue   chan ScrapingTask
	resultQueue chan ScrapingResult

	// 制御
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// 統計
	stats *ScrapingStats

	// 設定
	userAgent    string
	requestDelay time.Duration
}

// ScrapingStatsはスクレイピング統計を管理します
type ScrapingStats struct {
	mu              sync.RWMutex
	totalRequests   int64
	successRequests int64
	failedRequests  int64
	retryRequests   int64
	averageLatency  time.Duration
	statusCodes     map[int]int64
}

// NewWebScraperは新しいWebスクレイパーを作成します
func NewWebScraper(maxConcurrent int, requestsPerSecond float64) *WebScraper {
	ctx, cancel := context.WithCancel(context.Background())

	// HTTPクライアントを設定
	client := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 10,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	// レート制限を設定
	rateLimiter := time.NewTicker(time.Duration(float64(time.Second) / requestsPerSecond))

	return &WebScraper{
		client:        client,
		rateLimiter:   rateLimiter,
		maxConcurrent: maxConcurrent,
		taskQueue:     make(chan ScrapingTask, maxConcurrent*2),
		resultQueue:   make(chan ScrapingResult, maxConcurrent*2),
		ctx:           ctx,
		cancel:        cancel,
		stats: &ScrapingStats{
			statusCodes: make(map[int]int64),
		},
		userAgent:    "WebScraper/1.0",
		requestDelay: time.Duration(float64(time.Second) / requestsPerSecond),
	}
}

// Startはスクレイパーを開始します
func (ws *WebScraper) Start() {
	// ワーカーを開始
	for i := 0; i < ws.maxConcurrent; i++ {
		ws.wg.Add(1)
		go ws.worker(i)
	}

	// 結果処理を開始
	ws.wg.Add(1)
	go ws.resultHandler()

	// 統計レポートを開始
	ws.wg.Add(1)
	go ws.statsReporter()

	log.Printf("Web scraper started with %d workers", ws.maxConcurrent)
}

// workerはスクレイピングを実行するワーカーです
func (ws *WebScraper) worker(workerID int) {
	defer ws.wg.Done()

	for {
		select {
		case <-ws.ctx.Done():
			return
		case task, ok := <-ws.taskQueue:
			if !ok {
				return
			}

			// レート制限を適用
			<-ws.rateLimiter.C

			result := ws.scrapeURL(task, workerID)

			// 結果を送信
			select {
			case ws.resultQueue <- result:
			case <-ws.ctx.Done():
				return
			}
		}
	}
}

// scrapeURLは指定されたURLをスクレイピングします
func (ws *WebScraper) scrapeURL(task ScrapingTask, workerID int) ScrapingResult {
	start := time.Now()

	result := ScrapingResult{
		URL:        task.URL,
		RetryCount: task.RetryCount,
	}

	// HTTPリクエストを作成
	req, err := http.NewRequestWithContext(ws.ctx, "GET", task.URL, nil)
	if err != nil {
		result.Error = fmt.Errorf("failed to create request: %v", err)
		result.Duration = time.Since(start)
		return result
	}

	// ヘッダーを設定
	req.Header.Set("User-Agent", ws.userAgent)
	for key, value := range task.Headers {
		req.Header.Set(key, value)
	}

	// タイムアウトを設定
	if task.Timeout > 0 {
		ctx, cancel := context.WithTimeout(ws.ctx, task.Timeout)
		defer cancel()
		req = req.WithContext(ctx)
	}

	// リクエストを実行
	resp, err := ws.client.Do(req)
	if err != nil {
		result.Error = fmt.Errorf("request failed: %v", err)
		result.Duration = time.Since(start)

		// リトライ判定
		if task.RetryCount < task.MaxRetries {
			atomic.AddInt64(&ws.stats.retryRequests, 1)
			ws.scheduleRetry(task, workerID)
		}
		return result
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.Printf("Failed to close response body: %v", err)
		}
	}()

	// 結果を設定
	result.StatusCode = resp.StatusCode
	result.ContentLength = resp.ContentLength
	result.Duration = time.Since(start)
	result.Success = resp.StatusCode >= 200 && resp.StatusCode < 300

	if !result.Success {
		result.Error = fmt.Errorf("HTTP error: %d", resp.StatusCode)

		// 5xx系エラーはリトライ
		if resp.StatusCode >= 500 && task.RetryCount < task.MaxRetries {
			atomic.AddInt64(&ws.stats.retryRequests, 1)
			ws.scheduleRetry(task, workerID)
		}
	}

	log.Printf("Worker %d: %s -> %d (%v)", workerID, task.URL, resp.StatusCode, result.Duration)
	return result
}

// scheduleRetryはリトライをスケジュールします
func (ws *WebScraper) scheduleRetry(task ScrapingTask, workerID int) {
	retryTask := task
	retryTask.RetryCount++

	// 指数バックオフでリトライ
	backoffDelay := time.Duration(1<<uint(retryTask.RetryCount)) * time.Second

	go func() {
		select {
		case <-time.After(backoffDelay):
			select {
			case ws.taskQueue <- retryTask:
				log.Printf("Worker %d: Scheduled retry %d for %s",
					workerID, retryTask.RetryCount, task.URL)
			case <-ws.ctx.Done():
			}
		case <-ws.ctx.Done():
		}
	}()
}

// resultHandlerは結果を処理します
func (ws *WebScraper) resultHandler() {
	defer ws.wg.Done()

	for {
		select {
		case <-ws.ctx.Done():
			return
		case result, ok := <-ws.resultQueue:
			if !ok {
				return
			}

			ws.updateStats(result)
		}
	}
}

// updateStatsは統計を更新します
func (ws *WebScraper) updateStats(result ScrapingResult) {
	ws.stats.mu.Lock()
	defer ws.stats.mu.Unlock()

	atomic.AddInt64(&ws.stats.totalRequests, 1)

	if result.Success {
		atomic.AddInt64(&ws.stats.successRequests, 1)
	} else {
		atomic.AddInt64(&ws.stats.failedRequests, 1)
	}

	if result.StatusCode > 0 {
		ws.stats.statusCodes[result.StatusCode]++
	}

	// 平均レイテンシを更新（簡易版）
	totalRequests := atomic.LoadInt64(&ws.stats.totalRequests)
	ws.stats.averageLatency = time.Duration(
		(int64(ws.stats.averageLatency)*totalRequests + int64(result.Duration)) / (totalRequests + 1))
}

// statsReporter 統計を定期的に報告します
func (ws *WebScraper) statsReporter() {
	defer ws.wg.Done()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ws.ctx.Done():
			return
		case <-ticker.C:
			ws.printStats()
		}
	}
}

// printStat 統計を出力します
func (ws *WebScraper) printStats() {
	ws.stats.mu.RLock()
	defer ws.stats.mu.RUnlock()

	total := atomic.LoadInt64(&ws.stats.totalRequests)
	success := atomic.LoadInt64(&ws.stats.successRequests)
	failed := atomic.LoadInt64(&ws.stats.failedRequests)
	retries := atomic.LoadInt64(&ws.stats.retryRequests)

	successRate := float64(0)
	if total > 0 {
		successRate = float64(success) / float64(total) * 100
	}

	log.Printf("Scraping Stats: Total=%d, Success=%d (%.1f%%), Failed=%d, Retries=%d, AvgLatency=%v",
		total, success, successRate, failed, retries, ws.stats.averageLatency)

	// ステータスコード分布
	if len(ws.stats.statusCodes) > 0 {
		log.Printf("Status codes: %v", ws.stats.statusCodes)
	}
}

// SubmitURLはスクレイピング対象URLを追加します
func (ws *WebScraper) SubmitURL(url string, maxRetries int) error {
	task := ScrapingTask{
		URL:        url,
		Headers:    make(map[string]string),
		MaxRetries: maxRetries,
		Timeout:    30 * time.Second,
	}

	select {
	case ws.taskQueue <- task:
		return nil
	case <-ws.ctx.Done():
		return fmt.Errorf("scraper is shutting down")
	default:
		return fmt.Errorf("task queue is full")
	}
}

// Shutdownはスクレイパーを停止します
func (ws *WebScraper) Shutdown(timeout time.Duration) error {
	log.Println("Starting web scraper shutdown...")

	// 新しいタスクの受付を停止
	close(ws.taskQueue)

	// ワーカーに停止シグナルを送信
	ws.cancel()

	// レート制限タイマーを停止
	ws.rateLimiter.Stop()

	// 完了を待機
	done := make(chan struct{})
	go func() {
		ws.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("Web scraper shutdown completed")
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("shutdown timeout exceeded")
	}
}

// GetStatsは現在の統計を取得します
func (ws *WebScraper) GetStats() (int64, int64, int64, float64) {
	total := atomic.LoadInt64(&ws.stats.totalRequests)
	success := atomic.LoadInt64(&ws.stats.successRequests)
	failed := atomic.LoadInt64(&ws.stats.failedRequests)

	successRate := float64(0)
	if total > 0 {
		successRate = float64(success) / float64(total) * 100
	}

	return total, success, failed, successRate
}

func main() {
	// Webスクレイパーを作成（5並行、毎秒2リクエスト）
	scraper := NewWebScraper(5, 2.0)

	// スクレイパーを開始
	scraper.Start()

	// テスト用URL一覧
	testURLs := []string{
		"https://httpbin.org/delay/1",
		"https://httpbin.org/status/200",
		"https://httpbin.org/status/404",
		"https://httpbin.org/status/500",
		"https://httpbin.org/json",
		"https://example.com",
		"https://httpbin.org/delay/2",
		"https://httpbin.org/status/300",
	}

	// URLを送信
	go func() {
		for i, url := range testURLs {
			for j := 0; j < 3; j++ { // 各URLを3回
				finalURL := fmt.Sprintf("%s?request=%d", url, i*3+j+1)
				if err := scraper.SubmitURL(finalURL, 2); err != nil {
					log.Printf("Failed to submit URL %s: %v", finalURL, err)
				}
				time.Sleep(500 * time.Millisecond)
			}
		}
	}()

	// 45秒間動作させる
	time.Sleep(45 * time.Second)

	// 最終統計を表示
	total, success, failed, successRate := scraper.GetStats()
	log.Printf("Final Results: Total=%d, Success=%d, Failed=%d, Success Rate=%.1f%%",
		total, success, failed, successRate)

	// シャットダウン
	if err := scraper.Shutdown(10 * time.Second); err != nil {
		log.Printf("Shutdown error: %v", err)
	}
}
