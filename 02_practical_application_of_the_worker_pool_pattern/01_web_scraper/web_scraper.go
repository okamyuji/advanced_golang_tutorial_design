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

// ScrapingTask スクレイピングタスクを表現します
type ScrapingTask struct {
	URL        string
	Headers    map[string]string
	RetryCount int
	MaxRetries int
	Timeout    time.Duration
}

// ScrapingResult スクレイピング結果を表現します
type ScrapingResult struct {
	URL           string
	StatusCode    int
	ContentLength int64
	Duration      time.Duration
	Success       bool
	Error         error
	RetryCount    int
}

// WebScraper Webスクレイピング専用のワーカープールです
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

// ScrapingStats スクレイピング統計を管理します
type ScrapingStats struct {
	mu              sync.RWMutex
	totalRequests   atomic.Int64
	successRequests atomic.Int64
	failedRequests  atomic.Int64
	retryRequests   atomic.Int64
	averageLatency  time.Duration
	statusCodes     map[int]int64
}

// NewWebScraper 新しいWebスクレイパーを作成します
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

// Start スクレイパーを開始します
func (ws *WebScraper) Start() {
	// ワーカーを開始
	for i := range ws.maxConcurrent {
		ws.wg.Go(func() { ws.worker(i) })
	}

	// 結果処理を開始
	ws.wg.Go(ws.resultHandler)

	// 統計レポートを開始
	ws.wg.Go(ws.statsReporter)

	log.Printf("Web scraper started with %d workers", ws.maxConcurrent)
}

// worker スクレイピングを実行するワーカーです
func (ws *WebScraper) worker(workerID int) {
	for {
		select {
		case <-ws.ctx.Done():
			return
		case task, ok := <-ws.taskQueue:
			if !ok {
				return
			}

			// レート制限を適用する。Shutdownはtickerを止めるので、ctxのキャンセルも待つ
			select {
			case <-ws.rateLimiter.C:
			case <-ws.ctx.Done():
				return
			}

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

// scrapeURL 指定されたURLをスクレイピングします
func (ws *WebScraper) scrapeURL(task ScrapingTask, workerID int) ScrapingResult {
	start := time.Now()

	result := ScrapingResult{
		URL:        task.URL,
		RetryCount: task.RetryCount,
	}

	// HTTPリクエストを作成
	req, err := http.NewRequestWithContext(ws.ctx, http.MethodGet, task.URL, nil)
	if err != nil {
		result.Error = fmt.Errorf("failed to create request: %w", err)
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
		result.Error = fmt.Errorf("request failed: %w", err)
		result.Duration = time.Since(start)

		// リトライ判定
		if task.RetryCount < task.MaxRetries {
			ws.stats.retryRequests.Add(1)
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
			ws.stats.retryRequests.Add(1)
			ws.scheduleRetry(task, workerID)
		}
	}

	log.Printf("Worker %d: %s -> %d (%v)", workerID, task.URL, resp.StatusCode, result.Duration)
	return result
}

// scheduleRetry リトライをスケジュールします
func (ws *WebScraper) scheduleRetry(task ScrapingTask, workerID int) {
	retryTask := task
	retryTask.RetryCount++

	// 指数バックオフでリトライ
	backoffDelay := time.Second << retryTask.RetryCount

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

// resultHandler 結果を処理します
func (ws *WebScraper) resultHandler() {
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

// updateStats 統計を更新します
func (ws *WebScraper) updateStats(result ScrapingResult) {
	ws.stats.mu.Lock()
	defer ws.stats.mu.Unlock()

	ws.stats.totalRequests.Add(1)

	if result.Success {
		ws.stats.successRequests.Add(1)
	} else {
		ws.stats.failedRequests.Add(1)
	}

	if result.StatusCode > 0 {
		ws.stats.statusCodes[result.StatusCode]++
	}

	// 平均レイテンシを更新（簡易版）
	totalRequests := ws.stats.totalRequests.Load()
	ws.stats.averageLatency = time.Duration(
		(int64(ws.stats.averageLatency)*totalRequests + int64(result.Duration)) / (totalRequests + 1))
}

// statsReporter 統計を定期的に報告します
func (ws *WebScraper) statsReporter() {
	tick := time.Tick(10 * time.Second)

	for {
		select {
		case <-ws.ctx.Done():
			return
		case <-tick:
			ws.printStats()
		}
	}
}

// printStats 統計を出力します
func (ws *WebScraper) printStats() {
	ws.stats.mu.RLock()
	defer ws.stats.mu.RUnlock()

	total := ws.stats.totalRequests.Load()
	success := ws.stats.successRequests.Load()
	failed := ws.stats.failedRequests.Load()
	retries := ws.stats.retryRequests.Load()

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

// SubmitURL スクレイピング対象URLを追加します
func (ws *WebScraper) SubmitURL(url string, maxRetries int) error {
	// 停止後の投入を拒否する。キューはcloseせず、読む側はctxのキャンセルで止める
	if ws.ctx.Err() != nil {
		return fmt.Errorf("scraper is shutting down")
	}
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

// Shutdown スクレイパーを停止します
func (ws *WebScraper) Shutdown(timeout time.Duration) error {
	log.Println("Starting web scraper shutdown...")

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

// GetStats 現在の統計を取得します
func (ws *WebScraper) GetStats() (int64, int64, int64, float64) {
	total := ws.stats.totalRequests.Load()
	success := ws.stats.successRequests.Load()
	failed := ws.stats.failedRequests.Load()

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
			for j := range 3 { // 各URLを3回
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
