package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// MultiAuthSystem は複数認証プロバイダー並列検証システム
type MultiAuthSystem struct {
	providers map[string]AuthProvider
	cache     *AuthCache
	config    *AuthConfig
	metrics   *AuthMetrics
	mu        sync.RWMutex
}

// AuthConfig は認証システム設定
type AuthConfig struct {
	EnabledProviders  []string      // 有効なプロバイダーリスト
	DefaultProvider   string        // デフォルトプロバイダー
	ParallelTimeout   time.Duration // 並列認証タイムアウト
	CacheExpiration   time.Duration // キャッシュ有効期限
	MaxCacheSize      int           // 最大キャッシュサイズ
	RequireAllSuccess bool          // 全プロバイダー成功が必要か
	FallbackOrder     []string      // フォールバック順序
	RetryAttempts     int           // リトライ回数
	RetryDelay        time.Duration // リトライ間隔
}

// AuthProvider は認証プロバイダーインターフェース
type AuthProvider interface {
	Authenticate(ctx context.Context, token string) (*AuthResult, error)
	GetProviderInfo() *ProviderInfo
	HealthCheck(ctx context.Context) error
}

// AuthResult は認証結果
type AuthResult struct {
	Success      bool                   `json:"success"`
	UserID       string                 `json:"user_id"`
	Username     string                 `json:"username"`
	Email        string                 `json:"email"`
	Roles        []string               `json:"roles"`
	Claims       map[string]interface{} `json:"claims,omitempty"`
	Provider     string                 `json:"provider"`
	ExpiresAt    time.Time              `json:"expires_at"`
	IssuedAt     time.Time              `json:"issued_at"`
	TokenHash    string                 `json:"token_hash"`
	ResponseTime time.Duration          `json:"response_time"`
}

// ProviderInfo はプロバイダー情報
type ProviderInfo struct {
	Name        string        `json:"name"`
	Type        string        `json:"type"`
	Version     string        `json:"version"`
	Endpoint    string        `json:"endpoint"`
	IsHealthy   bool          `json:"is_healthy"`
	LastCheck   time.Time     `json:"last_check"`
	AvgLatency  time.Duration `json:"avg_latency"`
	SuccessRate float64       `json:"success_rate"`
}

// AuthCache は認証結果キャッシュ
type AuthCache struct {
	cache     map[string]*CacheEntry
	mu        sync.RWMutex
	maxSize   int
	evictions int64
}

// CacheEntry はキャッシュエントリ
type CacheEntry struct {
	Result      *AuthResult
	ExpiresAt   time.Time
	AccessCount int64
	LastAccess  time.Time
}

// AuthMetrics は認証メトリクス
type AuthMetrics struct {
	totalRequests   int64
	successfulAuth  int64
	failedAuth      int64
	cacheHits       int64
	cacheMisses     int64
	parallelAuth    int64
	providerStats   map[string]*ProviderStats
	avgResponseTime int64
	mu              sync.RWMutex
}

// ProviderStats はプロバイダー統計
type ProviderStats struct {
	totalRequests int64
	successCount  int64
	errorCount    int64
	totalLatency  int64
	timeoutCount  int64
	lastError     string
	lastErrorTime time.Time
}

// ParallelAuthResult は並列認証結果
type ParallelAuthResult struct {
	Provider string
	Result   *AuthResult
	Error    error
	Duration time.Duration
	TimedOut bool
}

// OAuth2Provider はOAuth2認証プロバイダー（標準ライブラリのみ）
type OAuth2Provider struct {
	name         string
	clientID     string
	clientSecret string
	endpoint     string
	timeout      time.Duration
	stats        *ProviderStats
	client       *http.Client
	mu           sync.RWMutex
}

// JWTProvider はJWT認証プロバイダー（標準ライブラリのみ）
type JWTProvider struct {
	name      string
	secretKey []byte
	issuer    string
	audience  string
	timeout   time.Duration
	stats     *ProviderStats
	mu        sync.RWMutex
}

// APIKeyProvider はAPIキー認証プロバイダー
type APIKeyProvider struct {
	name       string
	apiKeys    map[string]*APIKeyInfo
	headerName string
	keyPrefix  string
	timeout    time.Duration
	stats      *ProviderStats
	mu         sync.RWMutex
}

// APIKeyInfo はAPIキー情報
type APIKeyInfo struct {
	KeyID     string
	UserID    string
	Username  string
	Email     string
	Roles     []string
	CreatedAt time.Time
	ExpiresAt time.Time
	IsActive  bool
}

// JWTHeader はJWTヘッダー
type JWTHeader struct {
	Algorithm string `json:"alg"`
	Type      string `json:"typ"`
}

// JWTPayload はJWTペイロード
type JWTPayload struct {
	Issuer    string                 `json:"iss,omitempty"`
	Subject   string                 `json:"sub,omitempty"`
	Audience  string                 `json:"aud,omitempty"`
	ExpiresAt int64                  `json:"exp,omitempty"`
	NotBefore int64                  `json:"nbf,omitempty"`
	IssuedAt  int64                  `json:"iat,omitempty"`
	JWTID     string                 `json:"jti,omitempty"`
	Username  string                 `json:"username,omitempty"`
	Email     string                 `json:"email,omitempty"`
	Roles     []string               `json:"roles,omitempty"`
	Custom    map[string]interface{} `json:",inline"`
}

// NewMultiAuthSystem は新しい複数認証システムを作成
func NewMultiAuthSystem(config *AuthConfig) *MultiAuthSystem {
	return &MultiAuthSystem{
		providers: make(map[string]AuthProvider),
		cache: &AuthCache{
			cache:   make(map[string]*CacheEntry),
			maxSize: config.MaxCacheSize,
		},
		config: config,
		metrics: &AuthMetrics{
			providerStats: make(map[string]*ProviderStats),
		},
	}
}

// RegisterProvider はプロバイダーを登録
func (mas *MultiAuthSystem) RegisterProvider(name string, provider AuthProvider) {
	mas.mu.Lock()
	defer mas.mu.Unlock()

	mas.providers[name] = provider
	mas.metrics.providerStats[name] = &ProviderStats{}

	log.Printf("Registered auth provider: %s", name)
}

// AuthenticateParallel は並列認証を実行（メイン機能）
func (mas *MultiAuthSystem) AuthenticateParallel(ctx context.Context, token string) (*AuthResult, error) {
	start := time.Now()
	defer func() {
		duration := time.Since(start)
		atomic.AddInt64(&mas.metrics.avgResponseTime, duration.Nanoseconds())
		atomic.AddInt64(&mas.metrics.totalRequests, 1)
		atomic.AddInt64(&mas.metrics.parallelAuth, 1)
	}()

	// キャッシュチェック
	if result := mas.checkCache(token); result != nil {
		atomic.AddInt64(&mas.metrics.cacheHits, 1)
		return result, nil
	}
	atomic.AddInt64(&mas.metrics.cacheMisses, 1)

	// 並列認証実行
	results, err := mas.executeParallelAuth(ctx, token)
	if err != nil {
		atomic.AddInt64(&mas.metrics.failedAuth, 1)
		return nil, err
	}

	// 最適な結果を選択
	selectedResult := mas.selectBestResult(results)
	if selectedResult == nil {
		atomic.AddInt64(&mas.metrics.failedAuth, 1)
		return nil, fmt.Errorf("all authentication providers failed")
	}

	// キャッシュに保存
	mas.cacheResult(token, selectedResult)
	atomic.AddInt64(&mas.metrics.successfulAuth, 1)

	return selectedResult, nil
}

// executeParallelAuth は並列認証を実行
func (mas *MultiAuthSystem) executeParallelAuth(ctx context.Context, token string) ([]*ParallelAuthResult, error) {
	// タイムアウト付きコンテキスト作成
	authCtx, cancel := context.WithTimeout(ctx, mas.config.ParallelTimeout)
	defer cancel()

	// 有効なプロバイダーを取得
	enabledProviders := mas.getEnabledProviders()
	if len(enabledProviders) == 0 {
		return nil, fmt.Errorf("no enabled authentication providers")
	}

	// 並列認証実行
	resultChan := make(chan *ParallelAuthResult, len(enabledProviders))
	var wg sync.WaitGroup

	for _, providerName := range enabledProviders {
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			mas.authenticateWithProvider(authCtx, name, token, resultChan)
		}(providerName)
	}

	// 結果収集
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	var results []*ParallelAuthResult
	for result := range resultChan {
		results = append(results, result)
		mas.updateProviderStats(result)
	}

	return results, nil
}

// authenticateWithProvider は個別プロバイダーで認証
func (mas *MultiAuthSystem) authenticateWithProvider(ctx context.Context, providerName string, token string, resultChan chan<- *ParallelAuthResult) {
	start := time.Now()

	mas.mu.RLock()
	provider, exists := mas.providers[providerName]
	mas.mu.RUnlock()

	if !exists {
		resultChan <- &ParallelAuthResult{
			Provider: providerName,
			Error:    fmt.Errorf("provider %s not found", providerName),
			Duration: time.Since(start),
		}
		return
	}

	// プロバイダー認証実行
	result, err := provider.Authenticate(ctx, token)
	duration := time.Since(start)

	// タイムアウトチェック
	timedOut := err != nil && ctx.Err() == context.DeadlineExceeded

	if result != nil {
		result.Provider = providerName
		result.ResponseTime = duration
	}

	resultChan <- &ParallelAuthResult{
		Provider: providerName,
		Result:   result,
		Error:    err,
		Duration: duration,
		TimedOut: timedOut,
	}
}

// selectBestResult は最適な認証結果を選択
func (mas *MultiAuthSystem) selectBestResult(results []*ParallelAuthResult) *AuthResult {
	var successResults []*AuthResult

	// 成功結果を収集
	for _, result := range results {
		if result.Error == nil && result.Result != nil && result.Result.Success {
			successResults = append(successResults, result.Result)
		}
	}

	if len(successResults) == 0 {
		return nil
	}

	// 全プロバイダー成功が必要な場合
	if mas.config.RequireAllSuccess {
		enabledCount := len(mas.getEnabledProviders())
		if len(successResults) != enabledCount {
			return nil
		}
	}

	// デフォルトプロバイダーの結果を優先
	if mas.config.DefaultProvider != "" {
		for _, result := range successResults {
			if result.Provider == mas.config.DefaultProvider {
				return result
			}
		}
	}

	// フォールバック順序に従って選択
	for _, providerName := range mas.config.FallbackOrder {
		for _, result := range successResults {
			if result.Provider == providerName {
				return result
			}
		}
	}

	// 最も早く成功した結果を返す
	fastest := successResults[0]
	for _, result := range successResults[1:] {
		if result.ResponseTime < fastest.ResponseTime {
			fastest = result
		}
	}

	return fastest
}

// getEnabledProviders は有効なプロバイダーリストを取得
func (mas *MultiAuthSystem) getEnabledProviders() []string {
	mas.mu.RLock()
	defer mas.mu.RUnlock()

	var enabled []string
	for _, name := range mas.config.EnabledProviders {
		if _, exists := mas.providers[name]; exists {
			enabled = append(enabled, name)
		}
	}

	return enabled
}

// updateProviderStats はプロバイダー統計を更新
func (mas *MultiAuthSystem) updateProviderStats(result *ParallelAuthResult) {
	mas.metrics.mu.Lock()
	defer mas.metrics.mu.Unlock()

	stats, exists := mas.metrics.providerStats[result.Provider]
	if !exists {
		stats = &ProviderStats{}
		mas.metrics.providerStats[result.Provider] = stats
	}

	atomic.AddInt64(&stats.totalRequests, 1)
	atomic.AddInt64(&stats.totalLatency, result.Duration.Nanoseconds())

	if result.TimedOut {
		atomic.AddInt64(&stats.timeoutCount, 1)
	}

	if result.Error != nil {
		atomic.AddInt64(&stats.errorCount, 1)
		stats.lastError = result.Error.Error()
		stats.lastErrorTime = time.Now()
	} else if result.Result != nil && result.Result.Success {
		atomic.AddInt64(&stats.successCount, 1)
	}
}

// checkCache はキャッシュから認証結果をチェック
func (mas *MultiAuthSystem) checkCache(token string) *AuthResult {
	tokenHash := mas.hashToken(token)

	mas.cache.mu.RLock()
	entry, exists := mas.cache.cache[tokenHash]
	mas.cache.mu.RUnlock()

	if !exists {
		return nil
	}

	// 有効期限チェック
	if time.Now().After(entry.ExpiresAt) {
		mas.evictCacheEntry(tokenHash)
		return nil
	}

	// アクセス情報更新
	atomic.AddInt64(&entry.AccessCount, 1)
	entry.LastAccess = time.Now()

	return entry.Result
}

// cacheResult は認証結果をキャッシュ
func (mas *MultiAuthSystem) cacheResult(token string, result *AuthResult) {
	if mas.config.CacheExpiration == 0 {
		return
	}

	tokenHash := mas.hashToken(token)
	expiresAt := time.Now().Add(mas.config.CacheExpiration)

	// 結果の有効期限がキャッシュ期限より短い場合は結果の期限を使用
	if !result.ExpiresAt.IsZero() && result.ExpiresAt.Before(expiresAt) {
		expiresAt = result.ExpiresAt
	}

	entry := &CacheEntry{
		Result:      result,
		ExpiresAt:   expiresAt,
		AccessCount: 1,
		LastAccess:  time.Now(),
	}

	mas.cache.mu.Lock()
	defer mas.cache.mu.Unlock()

	// キャッシュサイズ制限チェック
	if len(mas.cache.cache) >= mas.cache.maxSize {
		mas.evictLeastRecentlyUsed()
	}

	mas.cache.cache[tokenHash] = entry
}

// evictLeastRecentlyUsed は最も使用頻度の低いキャッシュエントリを削除
func (mas *MultiAuthSystem) evictLeastRecentlyUsed() {
	var oldestKey string
	var oldestTime = time.Now()

	for key, entry := range mas.cache.cache {
		if entry.LastAccess.Before(oldestTime) {
			oldestTime = entry.LastAccess
			oldestKey = key
		}
	}

	if oldestKey != "" {
		delete(mas.cache.cache, oldestKey)
		atomic.AddInt64(&mas.cache.evictions, 1)
	}
}

// evictCacheEntry は指定されたキャッシュエントリを削除
func (mas *MultiAuthSystem) evictCacheEntry(tokenHash string) {
	mas.cache.mu.Lock()
	defer mas.cache.mu.Unlock()

	delete(mas.cache.cache, tokenHash)
	atomic.AddInt64(&mas.cache.evictions, 1)
}

// hashToken はトークンのハッシュを計算
func (mas *MultiAuthSystem) hashToken(token string) string {
	h := sha256.New()
	h.Write([]byte(token))
	return fmt.Sprintf("%x", h.Sum(nil))[:16] // 16文字に短縮
}

// NewOAuth2Provider は新しいOAuth2プロバイダーを作成
func NewOAuth2Provider(name, clientID, clientSecret, endpoint string, timeout time.Duration) *OAuth2Provider {
	return &OAuth2Provider{
		name:         name,
		clientID:     clientID,
		clientSecret: clientSecret,
		endpoint:     endpoint,
		timeout:      timeout,
		stats:        &ProviderStats{},
		client: &http.Client{
			Timeout: timeout,
		},
	}
}

// Authenticate はOAuth2認証を実行
func (o *OAuth2Provider) Authenticate(ctx context.Context, token string) (*AuthResult, error) {
	start := time.Now()
	defer func() {
		o.mu.Lock()
		o.stats.totalLatency = time.Since(start).Nanoseconds()
		o.mu.Unlock()
	}()

	// OAuth2トークン形式チェック
	if !strings.HasPrefix(token, "oauth2_") {
		return &AuthResult{Success: false}, fmt.Errorf("invalid OAuth2 token format")
	}

	// トークン検証リクエスト作成（実際の実装では外部OAuth2サーバーへのHTTPリクエスト）
	reqCtx, cancel := context.WithTimeout(ctx, o.timeout)
	defer cancel()

	// フォームデータ作成
	data := url.Values{}
	data.Set("token", token)
	data.Set("client_id", o.clientID)
	data.Set("client_secret", o.clientSecret)

	req, err := http.NewRequestWithContext(reqCtx, "POST", o.endpoint+"/oauth/token/info", strings.NewReader(data.Encode()))
	if err != nil {
		return &AuthResult{Success: false}, err
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	// リクエスト実行（この例では簡略化してモックレスポンス）
	// 実際の実装では o.client.Do(req) を使用
	_ = req

	// モックレスポンス（実際にはHTTPレスポンスをパース）
	if strings.Contains(token, "invalid") {
		return &AuthResult{Success: false}, fmt.Errorf("invalid token")
	}

	return &AuthResult{
		Success:   true,
		UserID:    "oauth2_user_123",
		Username:  "oauth2_user",
		Email:     "user@oauth2.example.com",
		Roles:     []string{"user"},
		Provider:  o.name,
		ExpiresAt: time.Now().Add(1 * time.Hour),
		IssuedAt:  time.Now(),
		TokenHash: o.hashToken(token),
	}, nil
}

// GetProviderInfo はプロバイダー情報を取得
func (o *OAuth2Provider) GetProviderInfo() *ProviderInfo {
	o.mu.RLock()
	defer o.mu.RUnlock()

	return &ProviderInfo{
		Name:        o.name,
		Type:        "OAuth2",
		Version:     "2.0",
		Endpoint:    o.endpoint,
		IsHealthy:   true,
		LastCheck:   time.Now(),
		AvgLatency:  time.Duration(o.stats.totalLatency),
		SuccessRate: o.calculateSuccessRate(),
	}
}

// HealthCheck はヘルスチェックを実行
func (o *OAuth2Provider) HealthCheck(ctx context.Context) error {
	// 実際の実装では外部サービスへのpingまたはヘルスチェックエンドポイントへのアクセス
	return nil
}

// calculateSuccessRate は成功率を計算
func (o *OAuth2Provider) calculateSuccessRate() float64 {
	total := atomic.LoadInt64(&o.stats.totalRequests)
	if total == 0 {
		return 100.0
	}
	success := atomic.LoadInt64(&o.stats.successCount)
	return float64(success) / float64(total) * 100.0
}

// hashToken はトークンのハッシュを計算
func (o *OAuth2Provider) hashToken(token string) string {
	h := sha256.New()
	h.Write([]byte(token))
	return fmt.Sprintf("oauth2_%x", h.Sum(nil))[:16]
}

// NewJWTProvider は新しいJWTプロバイダーを作成
func NewJWTProvider(name string, secretKey []byte, issuer, audience string, timeout time.Duration) *JWTProvider {
	return &JWTProvider{
		name:      name,
		secretKey: secretKey,
		issuer:    issuer,
		audience:  audience,
		timeout:   timeout,
		stats:     &ProviderStats{},
	}
}

// Authenticate はJWT認証を実行（標準ライブラリのみ）
func (j *JWTProvider) Authenticate(ctx context.Context, tokenString string) (*AuthResult, error) {
	start := time.Now()
	defer func() {
		j.mu.Lock()
		j.stats.totalLatency = time.Since(start).Nanoseconds()
		j.mu.Unlock()
	}()

	// JWT形式チェック
	parts := strings.Split(tokenString, ".")
	if len(parts) != 3 {
		return &AuthResult{Success: false}, fmt.Errorf("invalid JWT format")
	}

	// ヘッダーデコード
	headerBytes, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return &AuthResult{Success: false}, fmt.Errorf("invalid header encoding: %v", err)
	}

	var header JWTHeader
	if err := json.Unmarshal(headerBytes, &header); err != nil {
		return &AuthResult{Success: false}, fmt.Errorf("invalid header JSON: %v", err)
	}

	// アルゴリズムチェック
	if header.Algorithm != "HS256" {
		return &AuthResult{Success: false}, fmt.Errorf("unsupported algorithm: %s", header.Algorithm)
	}

	// ペイロードデコード
	payloadBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return &AuthResult{Success: false}, fmt.Errorf("invalid payload encoding: %v", err)
	}

	var payload JWTPayload
	if err := json.Unmarshal(payloadBytes, &payload); err != nil {
		return &AuthResult{Success: false}, fmt.Errorf("invalid payload JSON: %v", err)
	}

	// 署名検証
	expectedSignature := j.generateSignature(parts[0] + "." + parts[1])
	actualSignature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return &AuthResult{Success: false}, fmt.Errorf("invalid signature encoding: %v", err)
	}

	if !hmac.Equal(expectedSignature, actualSignature) {
		return &AuthResult{Success: false}, fmt.Errorf("invalid signature")
	}

	// クレーム検証
	now := time.Now().Unix()

	// 有効期限チェック
	if payload.ExpiresAt > 0 && now > payload.ExpiresAt {
		return &AuthResult{Success: false}, fmt.Errorf("token expired")
	}

	// 発行者チェック
	if j.issuer != "" && payload.Issuer != j.issuer {
		return &AuthResult{Success: false}, fmt.Errorf("invalid issuer")
	}

	// オーディエンスチェック
	if j.audience != "" && payload.Audience != j.audience {
		return &AuthResult{Success: false}, fmt.Errorf("invalid audience")
	}

	// 有効期限設定
	expiresAt := time.Now().Add(1 * time.Hour)
	if payload.ExpiresAt > 0 {
		expiresAt = time.Unix(payload.ExpiresAt, 0)
	}

	// 認証結果構築
	result := &AuthResult{
		Success:   true,
		UserID:    payload.Subject,
		Username:  payload.Username,
		Email:     payload.Email,
		Roles:     payload.Roles,
		Provider:  j.name,
		ExpiresAt: expiresAt,
		IssuedAt:  time.Unix(payload.IssuedAt, 0),
		TokenHash: j.hashToken(tokenString),
		Claims:    make(map[string]interface{}),
	}

	// カスタムクレームを追加
	if payload.Custom != nil {
		for k, v := range payload.Custom {
			result.Claims[k] = v
		}
	}

	return result, nil
}

// generateSignature はHMAC-SHA256署名を生成
func (j *JWTProvider) generateSignature(message string) []byte {
	h := hmac.New(sha256.New, j.secretKey)
	h.Write([]byte(message))
	return h.Sum(nil)
}

// GetProviderInfo はプロバイダー情報を取得
func (j *JWTProvider) GetProviderInfo() *ProviderInfo {
	j.mu.RLock()
	defer j.mu.RUnlock()

	return &ProviderInfo{
		Name:        j.name,
		Type:        "JWT",
		Version:     "1.0",
		Endpoint:    j.issuer,
		IsHealthy:   true,
		LastCheck:   time.Now(),
		AvgLatency:  time.Duration(j.stats.totalLatency),
		SuccessRate: j.calculateSuccessRate(),
	}
}

// HealthCheck はヘルスチェックを実行
func (j *JWTProvider) HealthCheck(ctx context.Context) error {
	return nil
}

// calculateSuccessRate は成功率を計算
func (j *JWTProvider) calculateSuccessRate() float64 {
	total := atomic.LoadInt64(&j.stats.totalRequests)
	if total == 0 {
		return 100.0
	}
	success := atomic.LoadInt64(&j.stats.successCount)
	return float64(success) / float64(total) * 100.0
}

// hashToken はトークンのハッシュを計算
func (j *JWTProvider) hashToken(token string) string {
	h := sha256.New()
	h.Write([]byte(token))
	return fmt.Sprintf("jwt_%x", h.Sum(nil))[:16]
}

// NewAPIKeyProvider は新しいAPIキープロバイダーを作成
func NewAPIKeyProvider(name, headerName, keyPrefix string, timeout time.Duration) *APIKeyProvider {
	return &APIKeyProvider{
		name:       name,
		apiKeys:    make(map[string]*APIKeyInfo),
		headerName: headerName,
		keyPrefix:  keyPrefix,
		timeout:    timeout,
		stats:      &ProviderStats{},
	}
}

// AddAPIKey はAPIキーを追加
func (a *APIKeyProvider) AddAPIKey(keyID, userID, username, email string, roles []string, expiresAt time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.apiKeys[keyID] = &APIKeyInfo{
		KeyID:     keyID,
		UserID:    userID,
		Username:  username,
		Email:     email,
		Roles:     roles,
		CreatedAt: time.Now(),
		ExpiresAt: expiresAt,
		IsActive:  true,
	}
}

// Authenticate はAPIキー認証を実行
func (a *APIKeyProvider) Authenticate(ctx context.Context, token string) (*AuthResult, error) {
	start := time.Now()
	defer func() {
		a.mu.Lock()
		a.stats.totalLatency = time.Since(start).Nanoseconds()
		a.mu.Unlock()
	}()

	// プレフィックスチェック
	if a.keyPrefix != "" && !strings.HasPrefix(token, a.keyPrefix) {
		return &AuthResult{Success: false}, fmt.Errorf("invalid API key format")
	}

	// プレフィックス除去
	keyID := strings.TrimPrefix(token, a.keyPrefix)

	a.mu.RLock()
	keyInfo, exists := a.apiKeys[keyID]
	a.mu.RUnlock()

	if !exists {
		return &AuthResult{Success: false}, fmt.Errorf("API key not found")
	}

	// キーの有効性チェック
	if !keyInfo.IsActive {
		return &AuthResult{Success: false}, fmt.Errorf("API key is inactive")
	}

	if !keyInfo.ExpiresAt.IsZero() && time.Now().After(keyInfo.ExpiresAt) {
		return &AuthResult{Success: false}, fmt.Errorf("API key has expired")
	}

	return &AuthResult{
		Success:   true,
		UserID:    keyInfo.UserID,
		Username:  keyInfo.Username,
		Email:     keyInfo.Email,
		Roles:     keyInfo.Roles,
		Provider:  a.name,
		ExpiresAt: keyInfo.ExpiresAt,
		IssuedAt:  keyInfo.CreatedAt,
		TokenHash: a.hashToken(token),
	}, nil
}

// GetProviderInfo はプロバイダー情報を取得
func (a *APIKeyProvider) GetProviderInfo() *ProviderInfo {
	a.mu.RLock()
	defer a.mu.RUnlock()

	return &ProviderInfo{
		Name:        a.name,
		Type:        "API_KEY",
		Version:     "1.0",
		IsHealthy:   true,
		LastCheck:   time.Now(),
		AvgLatency:  time.Duration(a.stats.totalLatency),
		SuccessRate: a.calculateSuccessRate(),
	}
}

// HealthCheck はヘルスチェックを実行
func (a *APIKeyProvider) HealthCheck(ctx context.Context) error {
	return nil
}

// calculateSuccessRate は成功率を計算
func (a *APIKeyProvider) calculateSuccessRate() float64 {
	total := atomic.LoadInt64(&a.stats.totalRequests)
	if total == 0 {
		return 100.0
	}
	success := atomic.LoadInt64(&a.stats.successCount)
	return float64(success) / float64(total) * 100.0
}

// hashToken はトークンのハッシュを計算
func (a *APIKeyProvider) hashToken(token string) string {
	h := sha256.New()
	h.Write([]byte(token))
	return fmt.Sprintf("apikey_%x", h.Sum(nil))[:16]
}

// HTTPHandler はHTTPハンドラー
func (mas *MultiAuthSystem) HTTPHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Authorization ヘッダーからトークン取得
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			http.Error(w, "Authorization header required", http.StatusUnauthorized)
			return
		}

		// Bearer トークンを抽出
		token := strings.TrimPrefix(authHeader, "Bearer ")
		if token == authHeader {
			// Bearer でない場合は API キーとして扱う
			token = authHeader
		}

		// 並列認証実行
		result, err := mas.AuthenticateParallel(r.Context(), token)
		if err != nil {
			log.Printf("Authentication failed: %v", err)
			http.Error(w, "Authentication failed", http.StatusUnauthorized)
			return
		}

		if !result.Success {
			http.Error(w, "Authentication failed", http.StatusUnauthorized)
			return
		}

		// 認証成功レスポンス
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(result); err != nil {
			log.Printf("Failed to encode auth result: %v", err)
		}
	}
}

// MetricsHandler はメトリクス情報を返すハンドラー
func (mas *MultiAuthSystem) MetricsHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		mas.metrics.mu.RLock()
		providerStats := make(map[string]interface{})
		for provider, stats := range mas.metrics.providerStats {
			total := atomic.LoadInt64(&stats.totalRequests)
			success := atomic.LoadInt64(&stats.successCount)
			errors := atomic.LoadInt64(&stats.errorCount)
			timeouts := atomic.LoadInt64(&stats.timeoutCount)
			avgLatency := float64(atomic.LoadInt64(&stats.totalLatency)) / float64(time.Millisecond)
			if total > 0 {
				avgLatency = avgLatency / float64(total)
			}

			providerStats[provider] = map[string]interface{}{
				"total_requests":  total,
				"success_count":   success,
				"error_count":     errors,
				"timeout_count":   timeouts,
				"avg_latency_ms":  avgLatency,
				"success_rate":    float64(success) / float64(total) * 100,
				"last_error":      stats.lastError,
				"last_error_time": stats.lastErrorTime.Unix(),
			}
		}
		mas.metrics.mu.RUnlock()

		metrics := map[string]interface{}{
			"total_requests":       atomic.LoadInt64(&mas.metrics.totalRequests),
			"successful_auth":      atomic.LoadInt64(&mas.metrics.successfulAuth),
			"failed_auth":          atomic.LoadInt64(&mas.metrics.failedAuth),
			"cache_hits":           atomic.LoadInt64(&mas.metrics.cacheHits),
			"cache_misses":         atomic.LoadInt64(&mas.metrics.cacheMisses),
			"parallel_auth":        atomic.LoadInt64(&mas.metrics.parallelAuth),
			"avg_response_time_ms": float64(atomic.LoadInt64(&mas.metrics.avgResponseTime)) / float64(time.Millisecond),
			"provider_stats":       providerStats,
			"cache_size":           len(mas.cache.cache),
			"cache_evictions":      atomic.LoadInt64(&mas.cache.evictions),
		}

		if err := json.NewEncoder(w).Encode(metrics); err != nil {
			log.Printf("Failed to encode metrics: %v", err)
		}
	}
}

// TestJWTGenerator はテスト用JWTを生成
func (mas *MultiAuthSystem) TestJWTGenerator() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Only POST method allowed", http.StatusMethodNotAllowed)
			return
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Failed to read request body", http.StatusBadRequest)
			return
		}

		var req map[string]interface{}
		if err := json.Unmarshal(body, &req); err != nil {
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}

		// JWT生成（テスト用）
		secretKey := []byte("test-secret-key")
		now := time.Now()

		header := JWTHeader{
			Algorithm: "HS256",
			Type:      "JWT",
		}

		payload := JWTPayload{
			Issuer:    "test-issuer",
			Subject:   "test-user",
			Audience:  "test-audience",
			ExpiresAt: now.Add(1 * time.Hour).Unix(),
			IssuedAt:  now.Unix(),
			Username:  "testuser",
			Email:     "test@example.com",
			Roles:     []string{"user", "admin"},
		}

		// カスタムクレーム追加
		if username, ok := req["username"]; ok {
			payload.Username = username.(string)
		}
		if email, ok := req["email"]; ok {
			payload.Email = email.(string)
		}

		headerJSON, _ := json.Marshal(header)
		payloadJSON, _ := json.Marshal(payload)

		headerB64 := base64.RawURLEncoding.EncodeToString(headerJSON)
		payloadB64 := base64.RawURLEncoding.EncodeToString(payloadJSON)

		message := headerB64 + "." + payloadB64

		h := hmac.New(sha256.New, secretKey)
		h.Write([]byte(message))
		signature := base64.RawURLEncoding.EncodeToString(h.Sum(nil))

		token := message + "." + signature

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string]string{
			"token": token,
			"type":  "Bearer",
		}); err != nil {
			log.Printf("Failed to encode token response: %v", err)
		}
	}
}

func main() {
	// 設定
	config := &AuthConfig{
		EnabledProviders:  []string{"oauth2", "jwt", "apikey"},
		DefaultProvider:   "jwt",
		ParallelTimeout:   5 * time.Second,
		CacheExpiration:   15 * time.Minute,
		MaxCacheSize:      1000,
		RequireAllSuccess: false,
		FallbackOrder:     []string{"jwt", "oauth2", "apikey"},
		RetryAttempts:     3,
		RetryDelay:        100 * time.Millisecond,
	}

	// 複数認証システム作成
	authSystem := NewMultiAuthSystem(config)

	// OAuth2プロバイダー（モック）
	oauth2Provider := NewOAuth2Provider("oauth2", "client123", "secret456", "https://oauth2.example.com", 3*time.Second)
	authSystem.RegisterProvider("oauth2", oauth2Provider)

	// JWTプロバイダー
	jwtSecret := []byte("test-secret-key")
	jwtProvider := NewJWTProvider("jwt", jwtSecret, "test-issuer", "test-audience", 2*time.Second)
	authSystem.RegisterProvider("jwt", jwtProvider)

	// APIキープロバイダー
	apiKeyProvider := NewAPIKeyProvider("apikey", "X-API-Key", "ak_", 1*time.Second)
	apiKeyProvider.AddAPIKey("ak_test123", "user123", "testuser", "test@example.com", []string{"user", "admin"}, time.Now().Add(24*time.Hour))
	apiKeyProvider.AddAPIKey("ak_admin456", "admin456", "adminuser", "admin@example.com", []string{"admin"}, time.Now().Add(24*time.Hour))
	authSystem.RegisterProvider("apikey", apiKeyProvider)

	// HTTPハンドラー設定
	http.HandleFunc("/auth", authSystem.HTTPHandler())
	http.HandleFunc("/metrics", authSystem.MetricsHandler())
	http.HandleFunc("/generate-jwt", authSystem.TestJWTGenerator())

	// ヘルスチェックハンドラー
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string]string{"status": "healthy"}); err != nil {
			log.Printf("Failed to encode health response: %v", err)
		}
	})

	// テスト用ページ
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		html := `
<!DOCTYPE html>
<html>
<head>
    <title>Multi-Auth System Test</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        .container { max-width: 800px; margin: 0 auto; }
        .section { margin: 20px 0; padding: 15px; border: 1px solid #ddd; }
        button { padding: 10px 20px; margin: 5px; }
        input[type="text"] { width: 300px; padding: 5px; }
        textarea { width: 100%; height: 100px; }
        .result { background: #f5f5f5; padding: 10px; margin: 10px 0; }
    </style>
</head>
<body>
    <div class="container">
        <h1>Multi-Auth System Test</h1>
        
        <div class="section">
            <h3>Generate Test JWT</h3>
            <input type="text" id="jwtUsername" placeholder="Username" value="testuser">
            <input type="text" id="jwtEmail" placeholder="Email" value="test@example.com">
            <button onclick="generateJWT()">Generate JWT</button>
            <div id="jwtResult" class="result"></div>
        </div>
        
        <div class="section">
            <h3>Test Authentication</h3>
            <textarea id="authToken" placeholder="Enter token here (JWT, OAuth2, or API Key)"></textarea><br>
            <button onclick="testAuth()">Test Authentication</button>
            <div id="authResult" class="result"></div>
        </div>
        
        <div class="section">
            <h3>Sample Tokens</h3>
            <p><strong>API Key:</strong> ak_test123</p>
            <p><strong>OAuth2 Token:</strong> oauth2_sample_token_123</p>
            <p><strong>JWT:</strong> Generate above or use the generate-jwt endpoint</p>
        </div>
        
        <div class="section">
            <h3>Metrics</h3>
            <button onclick="getMetrics()">Get Metrics</button>
            <div id="metricsResult" class="result"></div>
        </div>
    </div>
    
    <script>
        function generateJWT() {
            const username = document.getElementById('jwtUsername').value;
            const email = document.getElementById('jwtEmail').value;
            
            fetch('/generate-jwt', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ username: username, email: email })
            })
            .then(response => response.json())
            .then(data => {
                document.getElementById('jwtResult').innerHTML = 
                    '<strong>Generated JWT:</strong><br>' + 
                    '<code>' + data.token + '</code>';
                document.getElementById('authToken').value = data.token;
            })
            .catch(error => {
                document.getElementById('jwtResult').innerHTML = 'Error: ' + error;
            });
        }
        
        function testAuth() {
            const token = document.getElementById('authToken').value;
            if (!token) {
                alert('Please enter a token');
                return;
            }
            
            fetch('/auth', {
                method: 'GET',
                headers: { 'Authorization': 'Bearer ' + token }
            })
            .then(response => response.json())
            .then(data => {
                document.getElementById('authResult').innerHTML = 
                    '<strong>Authentication Result:</strong><br>' +
                    '<pre>' + JSON.stringify(data, null, 2) + '</pre>';
            })
            .catch(error => {
                document.getElementById('authResult').innerHTML = 'Error: ' + error;
            });
        }
        
        function getMetrics() {
            fetch('/metrics')
            .then(response => response.json())
            .then(data => {
                document.getElementById('metricsResult').innerHTML = 
                    '<strong>System Metrics:</strong><br>' +
                    '<pre>' + JSON.stringify(data, null, 2) + '</pre>';
            })
            .catch(error => {
                document.getElementById('metricsResult').innerHTML = 'Error: ' + error;
            });
        }
    </script>
</body>
</html>
`
		w.Header().Set("Content-Type", "text/html")
		if _, err := w.Write([]byte(html)); err != nil {
			log.Printf("Failed to write HTML response: %v", err)
		}
	})

	log.Println("=== Multi-Auth System (Standard Library Only) ===")
	log.Println("Starting multi-auth server on :8080")
	log.Println("Endpoints:")
	log.Println("  GET  /auth - Authenticate with Bearer token or API key")
	log.Println("  GET  /metrics - Get authentication metrics")
	log.Println("  POST /generate-jwt - Generate test JWT token")
	log.Println("  GET  /health - Health check")
	log.Println("  GET  / - Test web interface")
	log.Println()
	log.Println("Supported authentication methods:")
	log.Println("  - JWT (HS256 with HMAC-SHA256)")
	log.Println("  - OAuth2 (mock implementation)")
	log.Println("  - API Key (ak_test123, ak_admin456)")

	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatalf("Server failed to start: %v", err)
	}
}
