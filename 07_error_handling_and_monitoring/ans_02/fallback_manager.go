package main

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"
)

// FallbackMode フォールバックモードを表します
type FallbackMode int

const (
	NormalMode FallbackMode = iota
	Fallback
	TransitionMode
)

func (fm FallbackMode) String() string {
	switch fm {
	case NormalMode:
		return "NORMAL"
	case Fallback:
		return "FALLBACK"
	case TransitionMode:
		return "TRANSITION"
	default:
		return "UNKNOWN"
	}
}

// FallbackManagerConfig フォールバック管理の設定です
type FallbackManagerConfig struct {
	Name                 string
	ErrorRateThreshold   float64       // フォールバックに切り替える閾値
	RecoveryThreshold    float64       // 通常モードに戻る閾値
	MinimumRequestCount  int           // 最小リクエスト数
	SlidingWindowSize    int           // スライディングウィンドウサイズ
	EvaluationInterval   time.Duration // 評価間隔
	TransitionDuration   time.Duration // 移行期間
	MaxConcurrentWorkers int           // 最大並行ワーカー数
	OnModeChange         func(name string, from, to FallbackMode)
	OnRequestResult      func(success bool, mode FallbackMode, duration time.Duration)
}

// FallbackManagerMetrics フォールバック管理のメトリクスです
type FallbackManagerMetrics struct {
	TotalRequests        uint64    `json:"total_requests"`
	NormalModeRequests   uint64    `json:"normal_mode_requests"`
	FallbackModeRequests uint64    `json:"fallback_mode_requests"`
	SuccessfulRequests   uint64    `json:"successful_requests"`
	FailedRequests       uint64    `json:"failed_requests"`
	CurrentErrorRate     float64   `json:"current_error_rate"`
	LastModeChange       time.Time `json:"last_mode_change"`
	ModeChangeCount      uint64    `json:"mode_change_count"`
	FallbackActivations  uint64    `json:"fallback_activations"`
	CacheHits            uint64    `json:"cache_hits"`
	CacheMisses          uint64    `json:"cache_misses"`
}

// RequestResult リクエスト結果を表します
type RequestResult struct {
	Success   bool
	Mode      FallbackMode
	Timestamp time.Time
	Duration  time.Duration
}

// CacheEntry キャッシュエントリを表します
type CacheEntry struct {
	Data      any
	Timestamp time.Time
	TTL       time.Duration
}

// ExternalService 外部サービスインターフェースです
type ExternalService interface {
	Call(ctx context.Context, request any) (any, error)
	Name() string
}

// FallbackProvider フォールバック提供インターフェースです
type FallbackProvider interface {
	GetFallbackData(ctx context.Context, request any) (any, error)
	CanProvideFallback(request any) bool
}

// counters リクエストごとに並行して増えるカウンターです
// ロックを取らずに増やせるよう型付き atomic で持ち、GetMetrics で FallbackManagerMetrics に詰め替えます。
type counters struct {
	totalRequests        atomic.Uint64
	normalModeRequests   atomic.Uint64
	fallbackModeRequests atomic.Uint64
	successfulRequests   atomic.Uint64
	failedRequests       atomic.Uint64
	modeChangeCount      atomic.Uint64
	fallbackActivations  atomic.Uint64
	cacheHits            atomic.Uint64
	cacheMisses          atomic.Uint64
}

// FallbackManager エラー率ベースの自動フォールバック管理です
type FallbackManager struct {
	config           FallbackManagerConfig
	externalService  ExternalService
	fallbackProvider FallbackProvider
	cache            sync.Map // map[string]*CacheEntry

	counters counters

	// 以下は mutex で守ります
	currentMode    FallbackMode
	lastModeChange time.Time
	requestWindow  []RequestResult
	windowIndex    int
	windowFull     bool

	activeWorkers atomic.Int64
	isRunning     atomic.Bool

	ctx    context.Context
	cancel context.CancelFunc
	mutex  sync.RWMutex
	// lifecycle Start と Stop を直列化し、wg.Go と wg.Wait が同時に走らないようにします
	lifecycle sync.Mutex
	wg        sync.WaitGroup
}

var (
	ErrTooManyWorkers     = errors.New("too many concurrent workers")
	ErrServiceUnavailable = errors.New("service unavailable")
	ErrCacheMiss          = errors.New("cache miss")
	ErrStopped            = errors.New("fallback manager is stopped")
)

// NewFallbackManager 新しいフォールバック管理を作成します
func NewFallbackManager(config FallbackManagerConfig, service ExternalService, provider FallbackProvider) *FallbackManager {
	// デフォルト値設定
	config.ErrorRateThreshold = cmp.Or(config.ErrorRateThreshold, 0.3) // 30%
	config.RecoveryThreshold = cmp.Or(config.RecoveryThreshold, 0.1)   // 10%
	config.MinimumRequestCount = cmp.Or(config.MinimumRequestCount, 20)
	config.SlidingWindowSize = cmp.Or(config.SlidingWindowSize, 100)
	config.EvaluationInterval = cmp.Or(config.EvaluationInterval, 10*time.Second)
	config.TransitionDuration = cmp.Or(config.TransitionDuration, 30*time.Second)
	config.MaxConcurrentWorkers = cmp.Or(config.MaxConcurrentWorkers, 50)

	ctx, cancel := context.WithCancel(context.Background())

	return &FallbackManager{
		config:           config,
		externalService:  service,
		fallbackProvider: provider,
		currentMode:      NormalMode,
		requestWindow:    make([]RequestResult, config.SlidingWindowSize),
		lastModeChange:   time.Now(),
		ctx:              ctx,
		cancel:           cancel,
	}
}

// Start フォールバック管理を開始します
func (fm *FallbackManager) Start() error {
	fm.lifecycle.Lock()
	defer fm.lifecycle.Unlock()

	if fm.ctx.Err() != nil {
		return ErrStopped
	}
	if !fm.isRunning.CompareAndSwap(false, true) {
		return errors.New("fallback manager is already running")
	}

	log.Printf("Starting Fallback Manager [%s]...", fm.config.Name)

	// 評価ループを開始
	fm.wg.Go(fm.evaluationLoop)

	log.Printf("Fallback Manager [%s] started successfully", fm.config.Name)
	return nil
}

// Stop フォールバック管理を停止します
func (fm *FallbackManager) Stop() error {
	fm.lifecycle.Lock()
	defer fm.lifecycle.Unlock()

	if !fm.isRunning.CompareAndSwap(true, false) {
		return errors.New("fallback manager is not running")
	}

	log.Printf("Stopping Fallback Manager [%s]...", fm.config.Name)
	fm.cancel()
	fm.wg.Wait()
	log.Printf("Fallback Manager [%s] stopped", fm.config.Name)
	return nil
}

// Execute リクエストを実行します
func (fm *FallbackManager) Execute(ctx context.Context, request any) (any, error) {
	if fm.ctx.Err() != nil {
		return nil, ErrStopped
	}

	// 並行数制御。先に確保してから超過を判定しないと、Load と Add の間に他の呼び出しが割り込んで上限を超える
	if fm.activeWorkers.Add(1) > int64(fm.config.MaxConcurrentWorkers) {
		fm.activeWorkers.Add(-1)
		return nil, ErrTooManyWorkers
	}
	defer fm.activeWorkers.Add(-1)

	fm.counters.totalRequests.Add(1)

	start := time.Now()
	var result any
	var err error
	var success bool

	fm.mutex.RLock()
	currentMode := fm.currentMode
	fm.mutex.RUnlock()

	switch currentMode {
	case NormalMode:
		result, err, success = fm.executeNormalMode(ctx, request)
		fm.counters.normalModeRequests.Add(1)

	case Fallback:
		result, err, success = fm.executeFallbackMode(ctx, request)
		fm.counters.fallbackModeRequests.Add(1)

	case TransitionMode:
		result, err, success = fm.executeTransitionMode(ctx, request)

	default:
		err = ErrServiceUnavailable
		success = false
	}

	duration := time.Since(start)

	// 結果を記録
	fm.recordResult(success, currentMode, duration)

	// コールバック実行
	if fm.config.OnRequestResult != nil {
		go fm.config.OnRequestResult(success, currentMode, duration)
	}

	return result, err
}

// executeNormalMode 通常モードでリクエストを実行します
func (fm *FallbackManager) executeNormalMode(ctx context.Context, request any) (any, error, bool) {
	// キャッシュチェック
	if cached, found := fm.getFromCache(request); found {
		fm.counters.cacheHits.Add(1)
		return cached, nil, true
	}
	fm.counters.cacheMisses.Add(1)

	// 外部サービス呼び出し
	result, err := fm.externalService.Call(ctx, request)
	if err != nil {
		// エラー時はフォールバックを試行
		if fm.fallbackProvider.CanProvideFallback(request) {
			fallbackResult, fallbackErr := fm.fallbackProvider.GetFallbackData(ctx, request)
			if fallbackErr == nil {
				return fallbackResult, nil, false // エラーだがフォールバックで対応
			}
		}
		return nil, err, false
	}

	// 成功時はキャッシュに保存
	fm.saveToCache(request, result, 5*time.Minute)
	return result, nil, true
}

// executeFallbackMode フォールバックモードでリクエストを実行します
func (fm *FallbackManager) executeFallbackMode(ctx context.Context, request any) (any, error, bool) {
	// フォールバックデータを取得
	result, err := fm.fallbackProvider.GetFallbackData(ctx, request)
	if err != nil {
		// フォールバックも失敗した場合は古いキャッシュを試行
		if cached, found := fm.getFromCacheIgnoreTTL(request); found {
			fm.counters.cacheHits.Add(1)
			return cached, nil, false
		}
		return nil, err, false
	}

	return result, nil, true
}

// executeTransitionMode 移行モードでリクエストを実行します
func (fm *FallbackManager) executeTransitionMode(ctx context.Context, request any) (any, error, bool) {
	// 移行期間中は一部のリクエストを外部サービスに送信
	if rand.Float64() < 0.1 { // 10%のリクエストを外部サービスに
		fm.counters.normalModeRequests.Add(1)
		return fm.executeNormalMode(ctx, request)
	}
	fm.counters.fallbackModeRequests.Add(1)
	return fm.executeFallbackMode(ctx, request)
}

// recordResult リクエスト結果を記録します
func (fm *FallbackManager) recordResult(success bool, mode FallbackMode, duration time.Duration) {
	// メトリクス更新
	if success {
		fm.counters.successfulRequests.Add(1)
	} else {
		fm.counters.failedRequests.Add(1)
	}

	fm.mutex.Lock()
	defer fm.mutex.Unlock()

	// スライディングウィンドウに追加
	result := RequestResult{
		Success:   success,
		Mode:      mode,
		Timestamp: time.Now(),
		Duration:  duration,
	}

	fm.requestWindow[fm.windowIndex] = result
	fm.windowIndex = (fm.windowIndex + 1) % len(fm.requestWindow)

	if fm.windowIndex == 0 {
		fm.windowFull = true
	}
}

// evaluationLoop 評価ループを実行します
func (fm *FallbackManager) evaluationLoop() {
	tick := time.Tick(fm.config.EvaluationInterval)

	for {
		select {
		case <-fm.ctx.Done():
			return
		case <-tick:
			fm.evaluateMode()
		}
	}
}

// evaluateMode モードを評価します
func (fm *FallbackManager) evaluateMode() {
	fm.mutex.Lock()
	defer fm.mutex.Unlock()

	errorRate := fm.calculateCurrentErrorRate()

	windowSize := fm.getEffectiveWindowSize()
	if windowSize < fm.config.MinimumRequestCount {
		return // 十分なデータがない
	}

	switch fm.currentMode {
	case NormalMode:
		if errorRate >= fm.config.ErrorRateThreshold {
			fm.transitionTo(Fallback, errorRate)
		}

	case Fallback:
		if errorRate <= fm.config.RecoveryThreshold {
			fm.transitionTo(TransitionMode, errorRate)
		}

	case TransitionMode:
		// 移行期間が経過したか確認
		if time.Since(fm.lastModeChange) >= fm.config.TransitionDuration {
			if errorRate <= fm.config.RecoveryThreshold {
				fm.transitionTo(NormalMode, errorRate)
			} else {
				fm.transitionTo(Fallback, errorRate)
			}
		}
	}
}

// calculateCurrentErrorRate 現在のエラー率を計算します
func (fm *FallbackManager) calculateCurrentErrorRate() float64 {
	windowSize := fm.getEffectiveWindowSize()
	if windowSize == 0 {
		return 0.0
	}

	failures := 0
	for i := range windowSize {
		if !fm.requestWindow[i].Success {
			failures++
		}
	}

	return float64(failures) / float64(windowSize)
}

// getEffectiveWindowSize 有効なウィンドウサイズを取得します
func (fm *FallbackManager) getEffectiveWindowSize() int {
	if fm.windowFull {
		return len(fm.requestWindow)
	}
	return fm.windowIndex
}

// transitionTo 指定されたモードに遷移します
func (fm *FallbackManager) transitionTo(newMode FallbackMode, errorRate float64) {
	if fm.currentMode == newMode {
		return
	}

	oldMode := fm.currentMode
	fm.currentMode = newMode
	fm.lastModeChange = time.Now()
	fm.counters.modeChangeCount.Add(1)

	if newMode == Fallback {
		fm.counters.fallbackActivations.Add(1)
	}

	log.Printf("Fallback Manager [%s]: %s -> %s (Error Rate: %.2f%%)",
		fm.config.Name, oldMode, newMode, errorRate*100)

	// コールバック実行
	if fm.config.OnModeChange != nil {
		go fm.config.OnModeChange(fm.config.Name, oldMode, newMode)
	}
}

// getFromCache キャッシュからデータを取得します
func (fm *FallbackManager) getFromCache(request any) (any, bool) {
	key := fm.generateCacheKey(request)
	value, ok := fm.cache.Load(key)
	if !ok {
		return nil, false
	}

	entry := value.(*CacheEntry)
	if time.Since(entry.Timestamp) > entry.TTL {
		fm.cache.Delete(key)
		return nil, false
	}

	return entry.Data, true
}

// getFromCacheIgnoreTTL TTLを無視してキャッシュからデータを取得します
func (fm *FallbackManager) getFromCacheIgnoreTTL(request any) (any, bool) {
	key := fm.generateCacheKey(request)
	value, ok := fm.cache.Load(key)
	if !ok {
		return nil, false
	}

	entry := value.(*CacheEntry)
	return entry.Data, true
}

// saveToCache データをキャッシュに保存します
func (fm *FallbackManager) saveToCache(request any, data any, ttl time.Duration) {
	key := fm.generateCacheKey(request)
	entry := &CacheEntry{
		Data:      data,
		Timestamp: time.Now(),
		TTL:       ttl,
	}
	fm.cache.Store(key, entry)
}

// generateCacheKey キャッシュキーを生成します
func (fm *FallbackManager) generateCacheKey(request any) string {
	// 簡易実装：実際にはrequest内容をハッシュ化など
	return fmt.Sprintf("cache_%v", request)
}

// GetCurrentMode 現在のモードを取得します
func (fm *FallbackManager) GetCurrentMode() FallbackMode {
	fm.mutex.RLock()
	defer fm.mutex.RUnlock()
	return fm.currentMode
}

// GetMetrics メトリクスを取得します
func (fm *FallbackManager) GetMetrics() FallbackManagerMetrics {
	fm.mutex.RLock()
	defer fm.mutex.RUnlock()

	c := &fm.counters
	return FallbackManagerMetrics{
		TotalRequests:        c.totalRequests.Load(),
		NormalModeRequests:   c.normalModeRequests.Load(),
		FallbackModeRequests: c.fallbackModeRequests.Load(),
		SuccessfulRequests:   c.successfulRequests.Load(),
		FailedRequests:       c.failedRequests.Load(),
		CurrentErrorRate:     fm.calculateCurrentErrorRate(),
		LastModeChange:       fm.lastModeChange,
		ModeChangeCount:      c.modeChangeCount.Load(),
		FallbackActivations:  c.fallbackActivations.Load(),
		CacheHits:            c.cacheHits.Load(),
		CacheMisses:          c.cacheMisses.Load(),
	}
}

// ClearCache キャッシュをクリアします
func (fm *FallbackManager) ClearCache() {
	fm.cache.Clear()
}

// Reset フォールバック管理をリセットします
func (fm *FallbackManager) Reset() {
	fm.mutex.Lock()
	defer fm.mutex.Unlock()

	fm.currentMode = NormalMode
	fm.lastModeChange = time.Now()
	for _, c := range []*atomic.Uint64{
		&fm.counters.totalRequests, &fm.counters.normalModeRequests, &fm.counters.fallbackModeRequests,
		&fm.counters.successfulRequests, &fm.counters.failedRequests, &fm.counters.modeChangeCount,
		&fm.counters.fallbackActivations, &fm.counters.cacheHits, &fm.counters.cacheMisses,
	} {
		c.Store(0)
	}
	fm.requestWindow = make([]RequestResult, fm.config.SlidingWindowSize)
	fm.windowIndex = 0
	fm.windowFull = false
	fm.ClearCache()
}

// MockExternalService テスト用の外部サービスです
type MockExternalService struct {
	name string
	// failureRateBits 呼び出し中に SetFailureRate で変えられるよう、float64 のビット列を atomic に持ちます
	failureRateBits atomic.Uint64
	delay           time.Duration
}

func NewMockExternalService(name string, failureRate float64, delay time.Duration) *MockExternalService {
	mes := &MockExternalService{
		name:  name,
		delay: delay,
	}
	mes.SetFailureRate(failureRate)
	return mes
}

func (mes *MockExternalService) Name() string {
	return mes.name
}

func (mes *MockExternalService) Call(ctx context.Context, request any) (any, error) {
	// 遅延シミュレート
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(mes.delay):
	}

	// 失敗シミュレート
	if rand.Float64() < math.Float64frombits(mes.failureRateBits.Load()) {
		return nil, errors.New("external service error")
	}

	return fmt.Sprintf("External result for %v", request), nil
}

func (mes *MockExternalService) SetFailureRate(rate float64) {
	mes.failureRateBits.Store(math.Float64bits(rate))
}

// MockFallbackProvider テスト用のフォールバック提供者です
type MockFallbackProvider struct {
	cache map[string]any
	mutex sync.RWMutex
}

func NewMockFallbackProvider() *MockFallbackProvider {
	return &MockFallbackProvider{
		cache: make(map[string]any),
	}
}

func (mfp *MockFallbackProvider) GetFallbackData(ctx context.Context, request any) (any, error) {
	mfp.mutex.RLock()
	defer mfp.mutex.RUnlock()

	key := fmt.Sprintf("%v", request)
	if data, exists := mfp.cache[key]; exists {
		return data, nil
	}

	// デフォルトフォールバックデータ
	return fmt.Sprintf("Fallback result for %v", request), nil
}

func (mfp *MockFallbackProvider) CanProvideFallback(request any) bool {
	return true // すべてのリクエストにフォールバック可能
}

func (mfp *MockFallbackProvider) SetFallbackData(key string, data any) {
	mfp.mutex.Lock()
	defer mfp.mutex.Unlock()
	mfp.cache[key] = data
}

// runPhase 外部サービスのエラー率を変えて 30 件のリクエストを送り、5 件ごとに状態を表示します
func runPhase(fm *FallbackManager, svc *MockExternalService, title string, failureRate float64, firstID int) {
	log.Printf("\n--- %s ---", title)
	svc.SetFailureRate(failureRate)

	for n := range 30 {
		i := firstID + n
		request := fmt.Sprintf("request-%d", i)

		result, err := fm.Execute(context.Background(), request)
		if err != nil {
			log.Printf("Request %d failed: %v", i, err)
		} else {
			log.Printf("Request %d succeeded: %v", i, result)
		}

		time.Sleep(200 * time.Millisecond)

		// 5リクエストごとに状態表示
		if (n+1)%5 == 0 {
			mode := fm.GetCurrentMode()
			metrics := fm.GetMetrics()
			log.Printf("Current Mode: %s, Error Rate: %.2f%%, Total Requests: %d",
				mode, metrics.CurrentErrorRate*100, metrics.TotalRequests)
		}
	}
}

// 使用例とテスト用のmain関数
func main() {
	// モックサービスとプロバイダーを作成
	externalService := NewMockExternalService("test-api", 0.1, 100*time.Millisecond)
	fallbackProvider := NewMockFallbackProvider()

	// フォールバック管理設定
	config := FallbackManagerConfig{
		Name:                 "api-fallback-manager",
		ErrorRateThreshold:   0.3, // 30%
		RecoveryThreshold:    0.1, // 10%
		MinimumRequestCount:  20,
		SlidingWindowSize:    100,
		EvaluationInterval:   5 * time.Second,
		TransitionDuration:   15 * time.Second,
		MaxConcurrentWorkers: 10,
		OnModeChange: func(name string, from, to FallbackMode) {
			log.Printf("Mode Change [%s]: %s -> %s", name, from, to)
		},
		OnRequestResult: func(success bool, mode FallbackMode, duration time.Duration) {
			status := "SUCCESS"
			if !success {
				status = "FAILURE"
			}
			log.Printf("Request: %s in %s mode (duration: %v)", status, mode, duration)
		},
	}

	// フォールバック管理を作成
	fallbackManager := NewFallbackManager(config, externalService, fallbackProvider)

	// 開始
	if err := fallbackManager.Start(); err != nil {
		log.Fatalf("Failed to start fallback manager: %v", err)
	}
	defer func() {
		if err := fallbackManager.Stop(); err != nil {
			log.Printf("Failed to stop fallback manager: %v", err)
		}
	}()

	log.Println("=== Fallback Manager Demo ===")

	// 段階1: 正常動作（低エラー率）
	runPhase(fallbackManager, externalService, "Phase 1: Normal Operation (Low Error Rate)", 0.05, 0)

	// 段階2: 高エラー率（フォールバック発動）
	runPhase(fallbackManager, externalService, "Phase 2: High Error Rate (Fallback Activation)", 0.6, 30)

	// 段階3: 回復（低エラー率に戻す）
	runPhase(fallbackManager, externalService, "Phase 3: Recovery (Low Error Rate)", 0.05, 60)

	// 最終統計
	finalMetrics := fallbackManager.GetMetrics()
	log.Printf("\n=== Final Statistics ===")
	log.Printf("Total Requests: %d", finalMetrics.TotalRequests)
	log.Printf("Normal Mode Requests: %d", finalMetrics.NormalModeRequests)
	log.Printf("Fallback Mode Requests: %d", finalMetrics.FallbackModeRequests)
	log.Printf("Successful Requests: %d", finalMetrics.SuccessfulRequests)
	log.Printf("Failed Requests: %d", finalMetrics.FailedRequests)
	log.Printf("Final Error Rate: %.2f%%", finalMetrics.CurrentErrorRate*100)
	log.Printf("Mode Changes: %d", finalMetrics.ModeChangeCount)
	log.Printf("Fallback Activations: %d", finalMetrics.FallbackActivations)
	log.Printf("Cache Hits: %d", finalMetrics.CacheHits)
	log.Printf("Cache Misses: %d", finalMetrics.CacheMisses)

	log.Println("Demo completed")
}
