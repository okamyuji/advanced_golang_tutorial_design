package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// FallbackMode はフォールバックモードを表します
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

// FallbackManagerConfig はフォールバック管理の設定です
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

// FallbackManagerMetrics はフォールバック管理のメトリクスです
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

// RequestResult はリクエスト結果を表します
type RequestResult struct {
	Success   bool
	Mode      FallbackMode
	Timestamp time.Time
	Duration  time.Duration
}

// CacheEntry はキャッシュエントリを表します
type CacheEntry struct {
	Data      interface{}
	Timestamp time.Time
	TTL       time.Duration
}

// ExternalService は外部サービスインターフェースです
type ExternalService interface {
	Call(ctx context.Context, request interface{}) (interface{}, error)
	Name() string
}

// FallbackProvider はフォールバック提供インターフェースです
type FallbackProvider interface {
	GetFallbackData(ctx context.Context, request interface{}) (interface{}, error)
	CanProvideFallback(request interface{}) bool
}

// FallbackManager はエラー率ベースの自動フォールバック管理です
type FallbackManager struct {
	config           FallbackManagerConfig
	externalService  ExternalService
	fallbackProvider FallbackProvider
	cache            sync.Map // map[string]*CacheEntry

	currentMode   FallbackMode
	metrics       FallbackManagerMetrics
	requestWindow []RequestResult
	windowIndex   int
	windowFull    bool

	activeWorkers int64
	isRunning     int64

	ctx    context.Context
	cancel context.CancelFunc
	mutex  sync.RWMutex
}

var (
	ErrTooManyWorkers     = errors.New("too many concurrent workers")
	ErrServiceUnavailable = errors.New("service unavailable")
	ErrCacheMiss          = errors.New("cache miss")
)

// NewFallbackManager は新しいフォールバック管理を作成します
func NewFallbackManager(config FallbackManagerConfig, service ExternalService, provider FallbackProvider) *FallbackManager {
	// デフォルト値設定
	if config.ErrorRateThreshold == 0 {
		config.ErrorRateThreshold = 0.3 // 30%
	}
	if config.RecoveryThreshold == 0 {
		config.RecoveryThreshold = 0.1 // 10%
	}
	if config.MinimumRequestCount == 0 {
		config.MinimumRequestCount = 20
	}
	if config.SlidingWindowSize == 0 {
		config.SlidingWindowSize = 100
	}
	if config.EvaluationInterval == 0 {
		config.EvaluationInterval = 10 * time.Second
	}
	if config.TransitionDuration == 0 {
		config.TransitionDuration = 30 * time.Second
	}
	if config.MaxConcurrentWorkers == 0 {
		config.MaxConcurrentWorkers = 50
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &FallbackManager{
		config:           config,
		externalService:  service,
		fallbackProvider: provider,
		currentMode:      NormalMode,
		requestWindow:    make([]RequestResult, config.SlidingWindowSize),
		ctx:              ctx,
		cancel:           cancel,
		metrics: FallbackManagerMetrics{
			LastModeChange: time.Now(),
		},
	}
}

// Start はフォールバック管理を開始します
func (fm *FallbackManager) Start() error {
	if !atomic.CompareAndSwapInt64(&fm.isRunning, 0, 1) {
		return fmt.Errorf("fallback manager is already running")
	}

	log.Printf("Starting Fallback Manager [%s]...", fm.config.Name)

	// 評価ループを開始
	go fm.evaluationLoop()

	log.Printf("Fallback Manager [%s] started successfully", fm.config.Name)
	return nil
}

// Stop はフォールバック管理を停止します
func (fm *FallbackManager) Stop() error {
	if !atomic.CompareAndSwapInt64(&fm.isRunning, 1, 0) {
		return fmt.Errorf("fallback manager is not running")
	}

	log.Printf("Stopping Fallback Manager [%s]...", fm.config.Name)
	fm.cancel()
	log.Printf("Fallback Manager [%s] stopped", fm.config.Name)
	return nil
}

// Execute はリクエストを実行します
func (fm *FallbackManager) Execute(ctx context.Context, request interface{}) (interface{}, error) {
	// 並行数制御
	if atomic.LoadInt64(&fm.activeWorkers) >= int64(fm.config.MaxConcurrentWorkers) {
		return nil, ErrTooManyWorkers
	}

	atomic.AddInt64(&fm.activeWorkers, 1)
	defer atomic.AddInt64(&fm.activeWorkers, -1)

	atomic.AddUint64(&fm.metrics.TotalRequests, 1)

	start := time.Now()
	var result interface{}
	var err error
	var success bool

	fm.mutex.RLock()
	currentMode := fm.currentMode
	fm.mutex.RUnlock()

	switch currentMode {
	case NormalMode:
		result, err, success = fm.executeNormalMode(ctx, request)
		atomic.AddUint64(&fm.metrics.NormalModeRequests, 1)

	case Fallback:
		result, err, success = fm.executeFallbackMode(ctx, request)
		atomic.AddUint64(&fm.metrics.FallbackModeRequests, 1)

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

// executeNormalMode は通常モードでリクエストを実行します
func (fm *FallbackManager) executeNormalMode(ctx context.Context, request interface{}) (interface{}, error, bool) {
	// キャッシュチェック
	if cached, found := fm.getFromCache(request); found {
		atomic.AddUint64(&fm.metrics.CacheHits, 1)
		return cached, nil, true
	}
	atomic.AddUint64(&fm.metrics.CacheMisses, 1)

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

// executeFallbackMode はフォールバックモードでリクエストを実行します
func (fm *FallbackManager) executeFallbackMode(ctx context.Context, request interface{}) (interface{}, error, bool) {
	// フォールバックデータを取得
	result, err := fm.fallbackProvider.GetFallbackData(ctx, request)
	if err != nil {
		// フォールバックも失敗した場合は古いキャッシュを試行
		if cached, found := fm.getFromCacheIgnoreTTL(request); found {
			atomic.AddUint64(&fm.metrics.CacheHits, 1)
			return cached, nil, false
		}
		return nil, err, false
	}

	return result, nil, true
}

// executeTransitionMode は移行モードでリクエストを実行します
func (fm *FallbackManager) executeTransitionMode(ctx context.Context, request interface{}) (interface{}, error, bool) {
	// 移行期間中は一部のリクエストを外部サービスに送信
	if rand.Float64() < 0.1 { // 10%のリクエストを外部サービスに
		atomic.AddUint64(&fm.metrics.NormalModeRequests, 1)
		return fm.executeNormalMode(ctx, request)
	} else {
		atomic.AddUint64(&fm.metrics.FallbackModeRequests, 1)
		return fm.executeFallbackMode(ctx, request)
	}
}

// recordResult はリクエスト結果を記録します
func (fm *FallbackManager) recordResult(success bool, mode FallbackMode, duration time.Duration) {
	fm.mutex.Lock()
	defer fm.mutex.Unlock()

	// メトリクス更新
	if success {
		atomic.AddUint64(&fm.metrics.SuccessfulRequests, 1)
	} else {
		atomic.AddUint64(&fm.metrics.FailedRequests, 1)
	}

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

// evaluationLoop は評価ループを実行します
func (fm *FallbackManager) evaluationLoop() {
	ticker := time.NewTicker(fm.config.EvaluationInterval)
	defer ticker.Stop()

	for {
		select {
		case <-fm.ctx.Done():
			return
		case <-ticker.C:
			fm.evaluateMode()
		}
	}
}

// evaluateMode はモードを評価します
func (fm *FallbackManager) evaluateMode() {
	fm.mutex.Lock()
	defer fm.mutex.Unlock()

	errorRate := fm.calculateCurrentErrorRate()
	fm.metrics.CurrentErrorRate = errorRate

	windowSize := fm.getEffectiveWindowSize()
	if windowSize < fm.config.MinimumRequestCount {
		return // 十分なデータがない
	}

	switch fm.currentMode {
	case NormalMode:
		if errorRate >= fm.config.ErrorRateThreshold {
			fm.transitionTo(Fallback)
		}

	case Fallback:
		if errorRate <= fm.config.RecoveryThreshold {
			fm.transitionTo(TransitionMode)
		}

	case TransitionMode:
		// 移行期間が経過したか確認
		if time.Since(fm.metrics.LastModeChange) >= fm.config.TransitionDuration {
			if errorRate <= fm.config.RecoveryThreshold {
				fm.transitionTo(NormalMode)
			} else {
				fm.transitionTo(Fallback)
			}
		}
	}
}

// calculateCurrentErrorRate は現在のエラー率を計算します
func (fm *FallbackManager) calculateCurrentErrorRate() float64 {
	windowSize := fm.getEffectiveWindowSize()
	if windowSize == 0 {
		return 0.0
	}

	failures := 0
	for i := 0; i < windowSize; i++ {
		if !fm.requestWindow[i].Success {
			failures++
		}
	}

	return float64(failures) / float64(windowSize)
}

// getEffectiveWindowSize は有効なウィンドウサイズを取得します
func (fm *FallbackManager) getEffectiveWindowSize() int {
	if fm.windowFull {
		return len(fm.requestWindow)
	}
	return fm.windowIndex
}

// transitionTo は指定されたモードに遷移します
func (fm *FallbackManager) transitionTo(newMode FallbackMode) {
	if fm.currentMode == newMode {
		return
	}

	oldMode := fm.currentMode
	fm.currentMode = newMode
	fm.metrics.LastModeChange = time.Now()
	atomic.AddUint64(&fm.metrics.ModeChangeCount, 1)

	if newMode == Fallback {
		atomic.AddUint64(&fm.metrics.FallbackActivations, 1)
	}

	log.Printf("Fallback Manager [%s]: %s -> %s (Error Rate: %.2f%%)",
		fm.config.Name, oldMode, newMode, fm.metrics.CurrentErrorRate*100)

	// コールバック実行
	if fm.config.OnModeChange != nil {
		go fm.config.OnModeChange(fm.config.Name, oldMode, newMode)
	}
}

// getFromCache はキャッシュからデータを取得します
func (fm *FallbackManager) getFromCache(request interface{}) (interface{}, bool) {
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

// getFromCacheIgnoreTTL はTTLを無視してキャッシュからデータを取得します
func (fm *FallbackManager) getFromCacheIgnoreTTL(request interface{}) (interface{}, bool) {
	key := fm.generateCacheKey(request)
	value, ok := fm.cache.Load(key)
	if !ok {
		return nil, false
	}

	entry := value.(*CacheEntry)
	return entry.Data, true
}

// saveToCache はデータをキャッシュに保存します
func (fm *FallbackManager) saveToCache(request interface{}, data interface{}, ttl time.Duration) {
	key := fm.generateCacheKey(request)
	entry := &CacheEntry{
		Data:      data,
		Timestamp: time.Now(),
		TTL:       ttl,
	}
	fm.cache.Store(key, entry)
}

// generateCacheKey はキャッシュキーを生成します
func (fm *FallbackManager) generateCacheKey(request interface{}) string {
	// 簡易実装：実際にはrequest内容をハッシュ化など
	return fmt.Sprintf("cache_%v", request)
}

// GetCurrentMode は現在のモードを取得します
func (fm *FallbackManager) GetCurrentMode() FallbackMode {
	fm.mutex.RLock()
	defer fm.mutex.RUnlock()
	return fm.currentMode
}

// GetMetrics はメトリクスを取得します
func (fm *FallbackManager) GetMetrics() FallbackManagerMetrics {
	fm.mutex.RLock()
	defer fm.mutex.RUnlock()

	// コピーを返す
	metrics := fm.metrics
	metrics.CurrentErrorRate = fm.calculateCurrentErrorRate()
	return metrics
}

// ClearCache はキャッシュをクリアします
func (fm *FallbackManager) ClearCache() {
	fm.cache.Range(func(key, value interface{}) bool {
		fm.cache.Delete(key)
		return true
	})
}

// Reset はフォールバック管理をリセットします
func (fm *FallbackManager) Reset() {
	fm.mutex.Lock()
	defer fm.mutex.Unlock()

	fm.currentMode = NormalMode
	fm.metrics = FallbackManagerMetrics{
		LastModeChange: time.Now(),
	}
	fm.requestWindow = make([]RequestResult, fm.config.SlidingWindowSize)
	fm.windowIndex = 0
	fm.windowFull = false
	fm.ClearCache()
}

// MockExternalService はテスト用の外部サービスです
type MockExternalService struct {
	name        string
	failureRate float64
	delay       time.Duration
}

func NewMockExternalService(name string, failureRate float64, delay time.Duration) *MockExternalService {
	return &MockExternalService{
		name:        name,
		failureRate: failureRate,
		delay:       delay,
	}
}

func (mes *MockExternalService) Name() string {
	return mes.name
}

func (mes *MockExternalService) Call(ctx context.Context, request interface{}) (interface{}, error) {
	// 遅延シミュレート
	time.Sleep(mes.delay)

	// 失敗シミュレート
	if rand.Float64() < mes.failureRate {
		return nil, errors.New("external service error")
	}

	return fmt.Sprintf("External result for %v", request), nil
}

func (mes *MockExternalService) SetFailureRate(rate float64) {
	mes.failureRate = rate
}

// MockFallbackProvider はテスト用のフォールバック提供者です
type MockFallbackProvider struct {
	cache map[string]interface{}
	mutex sync.RWMutex
}

func NewMockFallbackProvider() *MockFallbackProvider {
	return &MockFallbackProvider{
		cache: make(map[string]interface{}),
	}
}

func (mfp *MockFallbackProvider) GetFallbackData(ctx context.Context, request interface{}) (interface{}, error) {
	mfp.mutex.RLock()
	defer mfp.mutex.RUnlock()

	key := fmt.Sprintf("%v", request)
	if data, exists := mfp.cache[key]; exists {
		return data, nil
	}

	// デフォルトフォールバックデータ
	return fmt.Sprintf("Fallback result for %v", request), nil
}

func (mfp *MockFallbackProvider) CanProvideFallback(request interface{}) bool {
	return true // すべてのリクエストにフォールバック可能
}

func (mfp *MockFallbackProvider) SetFallbackData(key string, data interface{}) {
	mfp.mutex.Lock()
	defer mfp.mutex.Unlock()
	mfp.cache[key] = data
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
	log.Println("\n--- Phase 1: Normal Operation (Low Error Rate) ---")
	externalService.SetFailureRate(0.05) // 5%エラー率

	for i := 0; i < 30; i++ {
		ctx := context.Background()
		request := fmt.Sprintf("request-%d", i)

		result, err := fallbackManager.Execute(ctx, request)
		if err != nil {
			log.Printf("Request %d failed: %v", i, err)
		} else {
			log.Printf("Request %d succeeded: %v", i, result)
		}

		time.Sleep(200 * time.Millisecond)

		// 5リクエストごとに状態表示
		if (i+1)%5 == 0 {
			mode := fallbackManager.GetCurrentMode()
			metrics := fallbackManager.GetMetrics()
			log.Printf("Current Mode: %s, Error Rate: %.2f%%, Total Requests: %d",
				mode, metrics.CurrentErrorRate*100, metrics.TotalRequests)
		}
	}

	// 段階2: 高エラー率（フォールバック発動）
	log.Println("\n--- Phase 2: High Error Rate (Fallback Activation) ---")
	externalService.SetFailureRate(0.6) // 60%エラー率

	for i := 30; i < 60; i++ {
		ctx := context.Background()
		request := fmt.Sprintf("request-%d", i)

		result, err := fallbackManager.Execute(ctx, request)
		if err != nil {
			log.Printf("Request %d failed: %v", i, err)
		} else {
			log.Printf("Request %d succeeded: %v", i, result)
		}

		time.Sleep(200 * time.Millisecond)

		if (i+1)%5 == 0 {
			mode := fallbackManager.GetCurrentMode()
			metrics := fallbackManager.GetMetrics()
			log.Printf("Current Mode: %s, Error Rate: %.2f%%, Total Requests: %d",
				mode, metrics.CurrentErrorRate*100, metrics.TotalRequests)
		}
	}

	// 段階3: 回復（低エラー率に戻す）
	log.Println("\n--- Phase 3: Recovery (Low Error Rate) ---")
	externalService.SetFailureRate(0.05) // 5%エラー率

	for i := 60; i < 90; i++ {
		ctx := context.Background()
		request := fmt.Sprintf("request-%d", i)

		result, err := fallbackManager.Execute(ctx, request)
		if err != nil {
			log.Printf("Request %d failed: %v", i, err)
		} else {
			log.Printf("Request %d succeeded: %v", i, result)
		}

		time.Sleep(200 * time.Millisecond)

		if (i+1)%5 == 0 {
			mode := fallbackManager.GetCurrentMode()
			metrics := fallbackManager.GetMetrics()
			log.Printf("Current Mode: %s, Error Rate: %.2f%%, Total Requests: %d",
				mode, metrics.CurrentErrorRate*100, metrics.TotalRequests)
		}
	}

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
