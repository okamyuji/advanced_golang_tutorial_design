package main

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"maps"
	"math"
	"math/rand/v2"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// RateLimitRequest レート制限チェック要求です
type RateLimitRequest struct {
	ClientID    string
	APIKey      string
	Endpoint    string
	Method      string
	UserTier    string
	ClientIP    string
	UserAgent   string
	RequestTime time.Time
}

// RateLimitResult レート制限チェック結果です
type RateLimitResult struct {
	Allowed        bool
	Remaining      int64
	ResetTime      time.Time
	RetryAfter     time.Duration
	LimitRule      string
	WindowStart    time.Time
	WindowEnd      time.Time
	RequestCount   int64
	BurstAvailable int64
}

// RateLimitRule レート制限ルールです
type RateLimitRule struct {
	Name        string
	Limit       int64
	Window      time.Duration
	BurstLimit  int64
	BurstWindow time.Duration
	UserTiers   []string
	Endpoints   []string
	Methods     []string
	Priority    int
}

// ClientBucket クライアント用バケットです
type ClientBucket struct {
	ClientID string
	APIKey   string
	UserTier string

	// レート制限状態
	RequestCount int64
	BurstCount   int64
	WindowStart  time.Time
	BurstStart   time.Time
	LastRequest  time.Time

	// 統計情報
	TotalRequests   int64
	BlockedRequests int64
	AllowedRequests int64

	// 同期制御
	mu sync.RWMutex
}

// APIRateLimiter API並行レート制限システムです
type APIRateLimiter struct {
	// 設定
	config *LimiterConfig

	// ルール管理
	rules   []*RateLimitRule
	rulesMu sync.RWMutex

	// クライアント管理
	buckets   map[string]*ClientBucket
	bucketsMu sync.RWMutex

	// 処理キュー
	requestQueue chan *RateLimitContext
	resultQueue  chan *RateLimitResult

	// ワーカー管理
	workers []*RateLimitWorker

	// コンテキスト制御
	ctx    context.Context
	cancel context.CancelFunc

	// 同期制御
	wg sync.WaitGroup

	// 統計・監視
	stats *LimiterStats

	// 制御フラグ
	isRunning atomic.Bool
	startTime time.Time
}

// RateLimitContext レート制限コンテキストです
type RateLimitContext struct {
	Request      *RateLimitRequest
	ResponseChan chan *RateLimitResult
	StartTime    time.Time
}

// RateLimitWorker レート制限ワーカーです
type RateLimitWorker struct {
	ID      int
	limiter *APIRateLimiter
	stats   *WorkerStats
}

// LimiterStats リミッター統計です
type LimiterStats struct {
	mu                sync.RWMutex
	totalRequests     atomic.Int64
	allowedRequests   atomic.Int64
	blockedRequests   atomic.Int64
	topBlockedClients map[string]int64
	topAllowedClients map[string]int64
	endpointStats     map[string]*EndpointStats
	tierStats         map[string]*TierStats
	startTime         time.Time
}

// WorkerStats ワーカー統計です。goroutineをまたいで読み書きされるため型付きatomicで保持します。
type WorkerStats struct {
	WorkerID          int
	ProcessedRequests atomic.Int64
	AllowedRequests   atomic.Int64
	BlockedRequests   atomic.Int64
	TotalProcessTime  atomic.Int64 // ナノ秒
}

// WorkerStatsSnapshot ある時点のワーカー統計の値コピーです
type WorkerStatsSnapshot struct {
	WorkerID           int
	ProcessedRequests  int64
	AllowedRequests    int64
	BlockedRequests    int64
	TotalProcessTime   time.Duration
	AverageProcessTime time.Duration
}

// Snapshot 現在の値をプレーンな構造体へコピーします
func (s *WorkerStats) Snapshot() WorkerStatsSnapshot {
	processed := s.ProcessedRequests.Load()
	total := time.Duration(s.TotalProcessTime.Load())

	var avg time.Duration
	if processed > 0 {
		avg = total / time.Duration(processed)
	}

	return WorkerStatsSnapshot{
		WorkerID:           s.WorkerID,
		ProcessedRequests:  processed,
		AllowedRequests:    s.AllowedRequests.Load(),
		BlockedRequests:    s.BlockedRequests.Load(),
		TotalProcessTime:   total,
		AverageProcessTime: avg,
	}
}

// EndpointStats エンドポイント統計です
type EndpointStats struct {
	Endpoint        string
	TotalRequests   int64
	AllowedRequests int64
	BlockedRequests int64
	BlockRate       float64
}

// TierStats ユーザーティア統計です
type TierStats struct {
	Tier            string
	TotalRequests   int64
	AllowedRequests int64
	BlockedRequests int64
	ActiveClients   int64
}

// LimiterConfig リミッター設定です
type LimiterConfig struct {
	WorkerCount       int
	RequestBufferSize int
	CleanupInterval   time.Duration
	BucketTTL         time.Duration
	DefaultRules      []*RateLimitRule
	EnableMetrics     bool
	MetricsInterval   time.Duration
}

// NewLimiterConfig デフォルト設定を作成します
func NewLimiterConfig() *LimiterConfig {
	return &LimiterConfig{
		WorkerCount:       4,
		RequestBufferSize: 10000,
		CleanupInterval:   5 * time.Minute,
		BucketTTL:         1 * time.Hour,
		DefaultRules:      getDefaultRateLimitRules(),
		EnableMetrics:     true,
		MetricsInterval:   30 * time.Second,
	}
}

// getDefaultRateLimitRules デフォルトレート制限ルールを取得します
func getDefaultRateLimitRules() []*RateLimitRule {
	return []*RateLimitRule{
		{
			Name:        "free_tier_general",
			Limit:       100,
			Window:      1 * time.Hour,
			BurstLimit:  10,
			BurstWindow: 1 * time.Minute,
			UserTiers:   []string{"free"},
			Priority:    100,
		},
		{
			Name:        "premium_tier_general",
			Limit:       1000,
			Window:      1 * time.Hour,
			BurstLimit:  50,
			BurstWindow: 1 * time.Minute,
			UserTiers:   []string{"premium"},
			Priority:    90,
		},
		{
			Name:        "enterprise_tier_general",
			Limit:       10000,
			Window:      1 * time.Hour,
			BurstLimit:  200,
			BurstWindow: 1 * time.Minute,
			UserTiers:   []string{"enterprise"},
			Priority:    80,
		},
		{
			Name:        "auth_endpoints_strict",
			Limit:       20,
			Window:      1 * time.Hour,
			BurstLimit:  5,
			BurstWindow: 1 * time.Minute,
			Endpoints:   []string{"/api/auth/login", "/api/auth/register"},
			Priority:    200,
		},
		{
			Name:        "write_operations_limit",
			Limit:       500,
			Window:      1 * time.Hour,
			BurstLimit:  20,
			BurstWindow: 1 * time.Minute,
			Methods:     []string{http.MethodPost, http.MethodPut, http.MethodDelete},
			Priority:    150,
		},
	}
}

// NewAPIRateLimiter 新しいAPIレート制限システムを作成します
func NewAPIRateLimiter(config *LimiterConfig) *APIRateLimiter {
	ctx, cancel := context.WithCancel(context.Background())

	limiter := &APIRateLimiter{
		config:       config,
		rules:        make([]*RateLimitRule, 0),
		buckets:      make(map[string]*ClientBucket),
		requestQueue: make(chan *RateLimitContext, config.RequestBufferSize),
		resultQueue:  make(chan *RateLimitResult, config.RequestBufferSize),
		workers:      make([]*RateLimitWorker, config.WorkerCount),
		ctx:          ctx,
		cancel:       cancel,
		stats: &LimiterStats{
			topBlockedClients: make(map[string]int64),
			topAllowedClients: make(map[string]int64),
			endpointStats:     make(map[string]*EndpointStats),
			tierStats:         make(map[string]*TierStats),
			startTime:         time.Now(),
		},
	}

	// デフォルトルールを追加
	for _, rule := range config.DefaultRules {
		limiter.AddRule(rule)
	}

	// ワーカーを初期化
	for i := range config.WorkerCount {
		worker := &RateLimitWorker{
			ID:      i,
			limiter: limiter,
			stats: &WorkerStats{
				WorkerID: i,
			},
		}
		limiter.workers[i] = worker
	}

	return limiter
}

// AddRule レート制限ルールを追加します
func (arl *APIRateLimiter) AddRule(rule *RateLimitRule) {
	arl.rulesMu.Lock()
	defer arl.rulesMu.Unlock()

	arl.rules = append(arl.rules, rule)

	// 優先度でソート（高い優先度が先）
	slices.SortFunc(arl.rules, func(a, b *RateLimitRule) int {
		return cmp.Compare(b.Priority, a.Priority)
	})

	log.Printf("Rate limit rule added: %s (Priority=%d, Limit=%d/%v)",
		rule.Name, rule.Priority, rule.Limit, rule.Window)
}

// Start レート制限システムを開始します
func (arl *APIRateLimiter) Start() error {
	if !arl.isRunning.CompareAndSwap(false, true) {
		return errors.New("limiter is already running")
	}

	arl.startTime = time.Now()
	arl.stats.startTime = arl.startTime

	log.Printf("Starting API rate limiter with %d workers", len(arl.workers))

	// ワーカーを開始
	for _, worker := range arl.workers {
		arl.wg.Go(worker.run)
	}

	// 結果処理を開始
	arl.wg.Go(arl.handleResults)

	// クリーンアップを開始
	arl.wg.Go(arl.cleanupLoop)

	// メトリクス監視を開始
	if arl.config.EnableMetrics {
		arl.wg.Go(arl.monitorMetrics)
	}

	log.Printf("API rate limiter started successfully")
	return nil
}

// CheckRateLimit レート制限をチェックします
func (arl *APIRateLimiter) CheckRateLimit(request *RateLimitRequest) (*RateLimitResult, error) {
	if arl.ctx.Err() != nil || !arl.isRunning.Load() {
		return nil, errors.New("limiter is not running")
	}

	// レスポンスチャネルを作成
	responseChan := make(chan *RateLimitResult, 1)

	rateLimitCtx := &RateLimitContext{
		Request:      request,
		ResponseChan: responseChan,
		StartTime:    time.Now(),
	}

	// リクエストキューに追加
	select {
	case arl.requestQueue <- rateLimitCtx:
		arl.stats.totalRequests.Add(1)
	case <-arl.ctx.Done():
		return nil, errors.New("limiter is shutting down")
	default:
		return nil, errors.New("request queue is full")
	}

	// 結果を待機
	select {
	case result := <-responseChan:
		return result, nil
	case <-time.After(5 * time.Second):
		return nil, errors.New("rate limit check timeout")
	case <-arl.ctx.Done():
		return nil, errors.New("limiter is shutting down")
	}
}

// run ワーカーのメインループです
func (rlw *RateLimitWorker) run() {
	log.Printf("Rate limit worker %d started", rlw.ID)

	for {
		select {
		case <-rlw.limiter.ctx.Done():
			log.Printf("Worker %d stopping due to context cancellation", rlw.ID)
			return
		case rateLimitCtx, ok := <-rlw.limiter.requestQueue:
			if !ok {
				log.Printf("Worker %d stopping due to request queue closure", rlw.ID)
				return
			}

			// レート制限チェックを処理
			result := rlw.processRateLimitCheck(rateLimitCtx)

			// 結果を送信
			select {
			case rateLimitCtx.ResponseChan <- result:
			case <-rlw.limiter.ctx.Done():
				return
			case <-time.After(1 * time.Second):
				log.Printf("Failed to send rate limit result: timeout")
			}
		}
	}
}

// processRateLimitCheck レート制限チェックを処理します
func (rlw *RateLimitWorker) processRateLimitCheck(rateLimitCtx *RateLimitContext) *RateLimitResult {
	start := time.Now()
	request := rateLimitCtx.Request

	// 統計更新
	rlw.stats.ProcessedRequests.Add(1)

	// 適用可能なルールを見つける
	rule := rlw.findApplicableRule(request)
	if rule == nil {
		// ルールが見つからない場合は許可
		result := &RateLimitResult{
			Allowed:     true,
			Remaining:   math.MaxInt64,
			ResetTime:   time.Now().Add(24 * time.Hour),
			LimitRule:   "no_rule",
			WindowStart: time.Now(),
			WindowEnd:   time.Now().Add(24 * time.Hour),
		}

		rlw.stats.AllowedRequests.Add(1)
		rlw.updateProcessingTime(time.Since(start))
		return result
	}

	// クライアントバケットを取得または作成
	bucket := rlw.limiter.getOrCreateBucket(request.ClientID, request.APIKey, request.UserTier)

	// レート制限チェックを実行
	result := rlw.checkRateLimit(bucket, rule, request)

	// 統計更新
	if result.Allowed {
		rlw.stats.AllowedRequests.Add(1)
		rlw.limiter.updateAllowedStats(request)
	} else {
		rlw.stats.BlockedRequests.Add(1)
		rlw.limiter.updateBlockedStats(request)
	}

	rlw.updateProcessingTime(time.Since(start))
	return result
}

// findApplicableRule 適用可能なルールを見つけます
func (rlw *RateLimitWorker) findApplicableRule(request *RateLimitRequest) *RateLimitRule {
	rlw.limiter.rulesMu.RLock()
	defer rlw.limiter.rulesMu.RUnlock()

	for _, rule := range rlw.limiter.rules {
		if rlw.ruleMatches(rule, request) {
			return rule
		}
	}

	return nil
}

// ruleMatches ルールがリクエストに適用可能かチェックします
func (rlw *RateLimitWorker) ruleMatches(rule *RateLimitRule, request *RateLimitRequest) bool {
	// ユーザーティアチェック
	if len(rule.UserTiers) > 0 && !slices.Contains(rule.UserTiers, request.UserTier) {
		return false
	}

	// エンドポイントチェック
	if len(rule.Endpoints) > 0 {
		endpointMatches := slices.ContainsFunc(rule.Endpoints, func(endpoint string) bool {
			return rlw.endpointMatches(endpoint, request.Endpoint)
		})
		if !endpointMatches {
			return false
		}
	}

	// メソッドチェック
	if len(rule.Methods) > 0 && !slices.Contains(rule.Methods, request.Method) {
		return false
	}

	return true
}

// endpointMatches エンドポイントがパターンにマッチするかチェックします
func (rlw *RateLimitWorker) endpointMatches(pattern, endpoint string) bool {
	// 簡単なワイルドカードマッチング
	if strings.Contains(pattern, "*") {
		parts := strings.Split(pattern, "*")
		if len(parts) == 2 {
			return strings.HasPrefix(endpoint, parts[0]) && strings.HasSuffix(endpoint, parts[1])
		}
	}

	return pattern == endpoint
}

// getOrCreateBucket クライアントバケットを取得または作成します
func (arl *APIRateLimiter) getOrCreateBucket(clientID, apiKey, userTier string) *ClientBucket {
	bucketKey := fmt.Sprintf("%s:%s", clientID, apiKey)

	arl.bucketsMu.RLock()
	bucket, exists := arl.buckets[bucketKey]
	arl.bucketsMu.RUnlock()

	if exists {
		return bucket
	}

	// 新しいバケットを作成
	arl.bucketsMu.Lock()
	defer arl.bucketsMu.Unlock()

	// ダブルチェック
	if bucket, exists := arl.buckets[bucketKey]; exists {
		return bucket
	}

	bucket = &ClientBucket{
		ClientID:    clientID,
		APIKey:      apiKey,
		UserTier:    userTier,
		WindowStart: time.Now(),
		BurstStart:  time.Now(),
		LastRequest: time.Now(),
	}

	arl.buckets[bucketKey] = bucket

	log.Printf("Created new rate limit bucket: clientID=%s (tier=%s)", clientID, userTier)
	return bucket
}

// checkRateLimit レート制限をチェックします
func (rlw *RateLimitWorker) checkRateLimit(bucket *ClientBucket, rule *RateLimitRule, request *RateLimitRequest) *RateLimitResult {
	bucket.mu.Lock()
	defer bucket.mu.Unlock()

	now := request.RequestTime
	if now.IsZero() {
		now = time.Now()
	}

	// ウィンドウのリセットをチェック
	if now.Sub(bucket.WindowStart) >= rule.Window {
		bucket.RequestCount = 0
		bucket.WindowStart = now
	}

	// バーストウィンドウのリセットをチェック
	if now.Sub(bucket.BurstStart) >= rule.BurstWindow {
		bucket.BurstCount = 0
		bucket.BurstStart = now
	}

	// バースト制限チェック
	if bucket.BurstCount >= rule.BurstLimit {
		return &RateLimitResult{
			Allowed:        false,
			Remaining:      0,
			ResetTime:      bucket.BurstStart.Add(rule.BurstWindow),
			RetryAfter:     bucket.BurstStart.Add(rule.BurstWindow).Sub(now),
			LimitRule:      rule.Name + "_burst",
			WindowStart:    bucket.BurstStart,
			WindowEnd:      bucket.BurstStart.Add(rule.BurstWindow),
			RequestCount:   bucket.BurstCount,
			BurstAvailable: 0,
		}
	}

	// 通常の制限チェック
	if bucket.RequestCount >= rule.Limit {
		return &RateLimitResult{
			Allowed:        false,
			Remaining:      0,
			ResetTime:      bucket.WindowStart.Add(rule.Window),
			RetryAfter:     bucket.WindowStart.Add(rule.Window).Sub(now),
			LimitRule:      rule.Name,
			WindowStart:    bucket.WindowStart,
			WindowEnd:      bucket.WindowStart.Add(rule.Window),
			RequestCount:   bucket.RequestCount,
			BurstAvailable: rule.BurstLimit - bucket.BurstCount,
		}
	}

	// リクエストを許可
	bucket.RequestCount++
	bucket.BurstCount++
	bucket.LastRequest = now
	bucket.TotalRequests++
	bucket.AllowedRequests++

	return &RateLimitResult{
		Allowed:        true,
		Remaining:      rule.Limit - bucket.RequestCount,
		ResetTime:      bucket.WindowStart.Add(rule.Window),
		RetryAfter:     0,
		LimitRule:      rule.Name,
		WindowStart:    bucket.WindowStart,
		WindowEnd:      bucket.WindowStart.Add(rule.Window),
		RequestCount:   bucket.RequestCount,
		BurstAvailable: rule.BurstLimit - bucket.BurstCount,
	}
}

// updateProcessingTime 処理時間を更新します
func (rlw *RateLimitWorker) updateProcessingTime(duration time.Duration) {
	rlw.stats.TotalProcessTime.Add(int64(duration))
}

// updateAllowedStats 許可統計を更新します
func (arl *APIRateLimiter) updateAllowedStats(request *RateLimitRequest) {
	arl.stats.mu.Lock()
	defer arl.stats.mu.Unlock()

	arl.stats.allowedRequests.Add(1)

	// クライアント統計
	arl.stats.topAllowedClients[request.ClientID]++

	// エンドポイント統計
	if _, exists := arl.stats.endpointStats[request.Endpoint]; !exists {
		arl.stats.endpointStats[request.Endpoint] = &EndpointStats{
			Endpoint: request.Endpoint,
		}
	}
	endpointStat := arl.stats.endpointStats[request.Endpoint]
	endpointStat.TotalRequests++
	endpointStat.AllowedRequests++

	// ティア統計
	if _, exists := arl.stats.tierStats[request.UserTier]; !exists {
		arl.stats.tierStats[request.UserTier] = &TierStats{
			Tier: request.UserTier,
		}
	}
	tierStat := arl.stats.tierStats[request.UserTier]
	tierStat.TotalRequests++
	tierStat.AllowedRequests++
}

// updateBlockedStats ブロック統計を更新します
func (arl *APIRateLimiter) updateBlockedStats(request *RateLimitRequest) {
	arl.stats.mu.Lock()
	defer arl.stats.mu.Unlock()

	arl.stats.blockedRequests.Add(1)

	// クライアント統計
	arl.stats.topBlockedClients[request.ClientID]++

	// エンドポイント統計
	if _, exists := arl.stats.endpointStats[request.Endpoint]; !exists {
		arl.stats.endpointStats[request.Endpoint] = &EndpointStats{
			Endpoint: request.Endpoint,
		}
	}
	endpointStat := arl.stats.endpointStats[request.Endpoint]
	endpointStat.TotalRequests++
	endpointStat.BlockedRequests++
	if endpointStat.TotalRequests > 0 {
		endpointStat.BlockRate = float64(endpointStat.BlockedRequests) / float64(endpointStat.TotalRequests) * 100
	}

	// ティア統計
	if _, exists := arl.stats.tierStats[request.UserTier]; !exists {
		arl.stats.tierStats[request.UserTier] = &TierStats{
			Tier: request.UserTier,
		}
	}
	tierStat := arl.stats.tierStats[request.UserTier]
	tierStat.TotalRequests++
	tierStat.BlockedRequests++
}

// handleResults 結果を処理します
func (arl *APIRateLimiter) handleResults() {
	log.Println("Result handler started")

	for {
		select {
		case <-arl.ctx.Done():
			log.Println("Result handler stopping due to context cancellation")
			return
		case result, ok := <-arl.resultQueue:
			if !ok {
				log.Println("Result handler stopping due to result queue closure")
				return
			}

			// 結果の後処理（ログ、通知等）
			arl.processResult(result)
		}
	}
}

// processResult 結果を処理します
func (arl *APIRateLimiter) processResult(result *RateLimitResult) {
	if !result.Allowed {
		log.Printf("Rate limit exceeded: rule=%s, remaining=%d, retry_after=%v",
			result.LimitRule, result.Remaining, result.RetryAfter)
	}
}

// cleanupLoop クリーンアップループを実行します
func (arl *APIRateLimiter) cleanupLoop() {
	// フィールドに保持せずStop/Resetも使わないため、Go 1.23以降はGCが回収できるtime.Tickでよい
	tick := time.Tick(arl.config.CleanupInterval)

	log.Println("Cleanup loop started")

	for {
		select {
		case <-arl.ctx.Done():
			log.Println("Cleanup loop stopping")
			return
		case <-tick:
			arl.cleanupBuckets()
		}
	}
}

// cleanupBuckets 古いバケットを削除します
func (arl *APIRateLimiter) cleanupBuckets() {
	now := time.Now()
	cutoff := now.Add(-arl.config.BucketTTL)

	arl.bucketsMu.Lock()
	defer arl.bucketsMu.Unlock()

	var toDelete []string

	for key, bucket := range arl.buckets {
		bucket.mu.RLock()
		if bucket.LastRequest.Before(cutoff) {
			toDelete = append(toDelete, key)
		}
		bucket.mu.RUnlock()
	}

	for _, key := range toDelete {
		delete(arl.buckets, key)
	}

	if len(toDelete) > 0 {
		log.Printf("Cleaned up %d inactive buckets", len(toDelete))
	}
}

// monitorMetrics メトリクスを監視します
func (arl *APIRateLimiter) monitorMetrics() {
	tick := time.Tick(arl.config.MetricsInterval)

	log.Println("Metrics monitor started")

	for {
		select {
		case <-arl.ctx.Done():
			log.Println("Metrics monitor stopping")
			return
		case <-tick:
			arl.reportMetrics()
		}
	}
}

// reportMetrics メトリクスを報告します
func (arl *APIRateLimiter) reportMetrics() {
	arl.stats.mu.RLock()
	defer arl.stats.mu.RUnlock()

	totalRequests := arl.stats.totalRequests.Load()
	allowedRequests := arl.stats.allowedRequests.Load()
	blockedRequests := arl.stats.blockedRequests.Load()

	uptime := time.Since(arl.stats.startTime)
	var rps float64
	if uptime.Seconds() > 0 {
		rps = float64(totalRequests) / uptime.Seconds()
	}

	var blockRate float64
	if totalRequests > 0 {
		blockRate = float64(blockedRequests) / float64(totalRequests) * 100
	}

	arl.bucketsMu.RLock()
	activeBuckets := len(arl.buckets)
	arl.bucketsMu.RUnlock()

	log.Printf("Rate Limiter Metrics: Total=%d, Allowed=%d, Blocked=%d, BlockRate=%.1f%%, RPS=%.2f, Buckets=%d",
		totalRequests, allowedRequests, blockedRequests, blockRate, rps, activeBuckets)

	// ワーカー別統計
	for _, worker := range arl.workers {
		snap := worker.stats.Snapshot()

		if snap.ProcessedRequests > 0 {
			allowRate := float64(snap.AllowedRequests) / float64(snap.ProcessedRequests) * 100
			log.Printf("Worker %d: Processed=%d, Allowed=%.1f%%, Blocked=%d, AvgTime=%v",
				snap.WorkerID, snap.ProcessedRequests, allowRate, snap.BlockedRequests, snap.AverageProcessTime)
		}
	}

	// トップブロッククライアント
	arl.reportTopClients()
}

// reportTopClients トップクライアントを報告します
func (arl *APIRateLimiter) reportTopClients() {
	// トップブロッククライアント
	type clientStat struct {
		ClientID string
		Count    int64
	}

	var topBlocked []clientStat
	for clientID, count := range arl.stats.topBlockedClients {
		topBlocked = append(topBlocked, clientStat{ClientID: clientID, Count: count})
	}

	slices.SortFunc(topBlocked, func(a, b clientStat) int {
		return cmp.Compare(b.Count, a.Count)
	})

	if len(topBlocked) > 0 {
		log.Printf("Top blocked clients:")
		for i, client := range topBlocked {
			if i >= 5 { // 上位5件
				break
			}
			log.Printf("  %d. %s: %d blocks", i+1, client.ClientID, client.Count)
		}
	}
}

// HTTPMiddleware HTTPミドルウェアとして機能します
func (arl *APIRateLimiter) HTTPMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// リクエスト情報を抽出
		request := &RateLimitRequest{
			ClientID:    arl.extractClientID(r),
			APIKey:      arl.extractAPIKey(r),
			Endpoint:    r.URL.Path,
			Method:      r.Method,
			UserTier:    arl.extractUserTier(r),
			ClientIP:    arl.extractClientIP(r),
			UserAgent:   r.UserAgent(),
			RequestTime: time.Now(),
		}

		// レート制限チェック
		result, err := arl.CheckRateLimit(request)
		if err != nil {
			http.Error(w, "Rate limit service error", http.StatusInternalServerError)
			return
		}

		// レート制限ヘッダーを設定
		w.Header().Set("X-RateLimit-Limit", fmt.Sprintf("%d", result.RequestCount+result.Remaining))
		w.Header().Set("X-RateLimit-Remaining", fmt.Sprintf("%d", result.Remaining))
		w.Header().Set("X-RateLimit-Reset", fmt.Sprintf("%d", result.ResetTime.Unix()))
		w.Header().Set("X-RateLimit-Rule", result.LimitRule)

		if !result.Allowed {
			w.Header().Set("Retry-After", fmt.Sprintf("%.0f", result.RetryAfter.Seconds()))

			errorResponse := map[string]any{
				"error":       "Rate limit exceeded",
				"rule":        result.LimitRule,
				"remaining":   result.Remaining,
				"reset_time":  result.ResetTime.Unix(),
				"retry_after": result.RetryAfter.Seconds(),
			}

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)
			if err := json.NewEncoder(w).Encode(errorResponse); err != nil {
				log.Printf("Failed to encode error response: %v", err)
			}
			return
		}

		// リクエストを次のハンドラーに渡す
		next(w, r)
	}
}

// ユーティリティメソッド

// extractClientID クライアントIDを抽出します
func (arl *APIRateLimiter) extractClientID(r *http.Request) string {
	// Authorization ヘッダーからクライアントIDを抽出
	auth := r.Header.Get("Authorization")
	if auth != "" {
		if _, token, ok := strings.Cut(auth, " "); ok {
			return token[:min(len(token), 10)] // 最初の10文字
		}
	}

	// クライアントIPをフォールバックとして使用
	return arl.extractClientIP(r)
}

// extractAPIKey APIキーを抽出します
func (arl *APIRateLimiter) extractAPIKey(r *http.Request) string {
	// X-API-Key ヘッダーから抽出
	if apiKey := r.Header.Get("X-API-Key"); apiKey != "" {
		return apiKey
	}

	// クエリパラメータから抽出
	if apiKey := r.URL.Query().Get("api_key"); apiKey != "" {
		return apiKey
	}

	return "default"
}

// extractUserTier ユーザーティアを抽出します
func (arl *APIRateLimiter) extractUserTier(r *http.Request) string {
	// X-User-Tier ヘッダーから抽出
	if tier := r.Header.Get("X-User-Tier"); tier != "" {
		return tier
	}

	// APIキーからティアを推測（実際の実装では、データベースルックアップを行う）
	apiKey := arl.extractAPIKey(r)
	switch {
	case strings.HasPrefix(apiKey, "ent_"):
		return "enterprise"
	case strings.HasPrefix(apiKey, "pre_"):
		return "premium"
	}

	return "free"
}

// extractClientIP クライアントIPを抽出します
func (arl *APIRateLimiter) extractClientIP(r *http.Request) string {
	// X-Forwarded-For ヘッダーをチェック
	if forwarded := r.Header.Get("X-Forwarded-For"); forwarded != "" {
		ips := strings.Split(forwarded, ",")
		return strings.TrimSpace(ips[0])
	}

	// X-Real-IP ヘッダーをチェック
	if realIP := r.Header.Get("X-Real-IP"); realIP != "" {
		return realIP
	}

	// RemoteAddr を使用
	if ip, _, _ := strings.Cut(r.RemoteAddr, ":"); ip != "" {
		return ip
	}

	return "unknown"
}

// Shutdown レート制限システムを停止します
func (arl *APIRateLimiter) Shutdown(timeout time.Duration) error {
	if !arl.isRunning.CompareAndSwap(true, false) {
		return errors.New("limiter is not running")
	}

	log.Println("Shutting down API rate limiter...")

	// 1. ワーカー・監視ループを止める。requestQueue closeしない。
	// CheckRateLimitからの投入とcloseが競合すると送信側がpanicするため、
	// 停止はctx.Done()だけに頼り、投入側はCheckRateLimit冒頭のarl.ctx.Err()チェックで弾く。
	arl.cancel()

	// 2. ワーカーの終了を待機
	done := make(chan struct{})
	go func() {
		arl.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("All workers stopped gracefully")
	case <-time.After(timeout):
		log.Println("Timeout reached waiting for workers")
		select {
		case <-done:
			log.Println("Workers stopped after extra wait")
		case <-time.After(2 * time.Second):
			// ワーカーがまだresultQueueへ送信中かもしれないのでcloseしない
			return errors.New("shutdown timed out waiting for workers to stop")
		}
	}

	// 3. すべてのワーカーが終わっているので結果キューを閉じる
	close(arl.resultQueue)

	log.Println("API rate limiter shutdown completed")
	return nil
}

// GetStats 統計情報を取得します
func (arl *APIRateLimiter) GetStats() map[string]any {
	arl.stats.mu.RLock()
	defer arl.stats.mu.RUnlock()

	totalRequests := arl.stats.totalRequests.Load()
	allowedRequests := arl.stats.allowedRequests.Load()
	blockedRequests := arl.stats.blockedRequests.Load()

	uptime := time.Since(arl.stats.startTime)
	var rps float64
	if uptime.Seconds() > 0 {
		rps = float64(totalRequests) / uptime.Seconds()
	}

	arl.bucketsMu.RLock()
	activeBuckets := len(arl.buckets)
	arl.bucketsMu.RUnlock()

	workerStats := make(map[string]any)
	for i, worker := range arl.workers {
		snap := worker.stats.Snapshot()
		workerStats[fmt.Sprintf("worker_%d", i)] = map[string]any{
			"processed_requests":      snap.ProcessedRequests,
			"allowed_requests":        snap.AllowedRequests,
			"blocked_requests":        snap.BlockedRequests,
			"average_processing_time": snap.AverageProcessTime,
		}
	}

	// 呼び出し元が内部マップを直接参照して書き換えられないよう値コピーを返す
	endpointStats := make(map[string]EndpointStats, len(arl.stats.endpointStats))
	for k, v := range arl.stats.endpointStats {
		endpointStats[k] = *v
	}
	tierStats := make(map[string]TierStats, len(arl.stats.tierStats))
	for k, v := range arl.stats.tierStats {
		tierStats[k] = *v
	}

	return map[string]any{
		"total_requests":      totalRequests,
		"allowed_requests":    allowedRequests,
		"blocked_requests":    blockedRequests,
		"requests_per_second": rps,
		"uptime":              uptime,
		"active_buckets":      activeBuckets,
		"worker_count":        len(arl.workers),
		"worker_stats":        workerStats,
		"endpoint_stats":      endpointStats,
		"tier_stats":          tierStats,
		"top_blocked_clients": maps.Clone(arl.stats.topBlockedClients),
		"top_allowed_clients": maps.Clone(arl.stats.topAllowedClients),
	}
}

func main() {
	// レート制限システム設定
	config := NewLimiterConfig()
	config.WorkerCount = 6
	config.RequestBufferSize = 5000

	// レート制限システム作成・開始
	limiter := NewAPIRateLimiter(config)
	if err := limiter.Start(); err != nil {
		log.Fatalf("Failed to start rate limiter: %v", err)
	}

	// テスト用HTTPサーバーを開始
	mux := http.NewServeMux()

	// レート制限付きAPIエンドポイント
	mux.HandleFunc("/api/data", limiter.HTTPMiddleware(func(w http.ResponseWriter, r *http.Request) {
		response := map[string]any{
			"message":   "API response",
			"timestamp": time.Now().Unix(),
			"method":    r.Method,
			"path":      r.URL.Path,
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(response); err != nil {
			log.Printf("Failed to encode response: %v", err)
		}
	}))

	mux.HandleFunc("/api/auth/login", limiter.HTTPMiddleware(func(w http.ResponseWriter, r *http.Request) {
		response := map[string]any{
			"message": "Login successful",
			"token":   "test_token_" + strconv.FormatInt(time.Now().Unix(), 10),
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(response); err != nil {
			log.Printf("Failed to encode login response: %v", err)
		}
	}))

	// 統計エンドポイント
	mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		stats := limiter.GetStats()
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(stats); err != nil {
			log.Printf("Failed to encode stats: %v", err)
		}
	})

	server := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	go func() {
		log.Println("HTTP server starting on :8080")
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("HTTP server error: %v", err)
		}
	}()

	// テストリクエストをシミュレート
	go func() {
		time.Sleep(2 * time.Second) // サーバー起動を待機

		userTiers := []string{"free", "premium", "enterprise"}
		endpoints := []string{"/api/data", "/api/auth/login", "/api/users", "/api/orders"}
		methods := []string{http.MethodGet, http.MethodPost, http.MethodPut, http.MethodDelete}

		for i := range 2000 {
			request := &RateLimitRequest{
				ClientID:    fmt.Sprintf("client_%d", i%50),
				APIKey:      fmt.Sprintf("key_%d", i%20),
				Endpoint:    endpoints[i%len(endpoints)],
				Method:      methods[i%len(methods)],
				UserTier:    userTiers[i%len(userTiers)],
				ClientIP:    fmt.Sprintf("192.168.1.%d", 100+(i%50)),
				UserAgent:   "TestClient/1.0",
				RequestTime: time.Now(),
			}

			result, err := limiter.CheckRateLimit(request)
			if err != nil {
				log.Printf("Rate limit check error: %v", err)
			} else if !result.Allowed {
				log.Printf("Request blocked: client=%s, rule=%s, retry_after=%v",
					request.ClientID, result.LimitRule, result.RetryAfter)
			}

			time.Sleep(10*time.Millisecond + rand.N(90*time.Millisecond))
		}

		log.Println("Test requests completed")
	}()

	// 30秒間実行
	time.Sleep(30 * time.Second)

	// HTTPサーバーを停止
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		log.Printf("HTTP server shutdown error: %v", err)
	}

	// 最終統計表示
	stats := limiter.GetStats()
	log.Printf("Final Stats: %+v", stats)

	// グレースフルシャットダウン
	if err := limiter.Shutdown(10 * time.Second); err != nil {
		log.Printf("Shutdown error: %v", err)
	}
}
