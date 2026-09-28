package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"maps"
	"sync"
	"testing"
	"time"
)

func newTestAuthSystem(t *testing.T) (*MultiAuthSystem, *APIKeyProvider) {
	t.Helper()

	config := &AuthConfig{
		EnabledProviders: []string{"jwt", "apikey"},
		DefaultProvider:  "jwt",
		ParallelTimeout:  time.Second,
		CacheExpiration:  time.Minute,
		MaxCacheSize:     100,
		FallbackOrder:    []string{"jwt", "apikey"},
	}

	system := NewMultiAuthSystem(config)

	jwtProvider := NewJWTProvider("jwt", []byte("test-secret"), "test-issuer", "test-audience", time.Second)
	system.RegisterProvider("jwt", jwtProvider)

	apiKeyProvider := NewAPIKeyProvider("apikey", "X-API-Key", "ak_", time.Second)
	// 登録はprefix付きでもprefixなしでもよい
	apiKeyProvider.AddAPIKey("ak_test", "user1", "tester", "tester@example.com", []string{"user"}, time.Now().Add(time.Hour))
	system.RegisterProvider("apikey", apiKeyProvider)

	return system, apiKeyProvider
}

// signTestJWT テスト用にHS256署名付きJWTを組み立てます
func signTestJWT(t *testing.T, secret []byte, payload JWTPayload) string {
	t.Helper()

	header := JWTHeader{Algorithm: "HS256", Type: "JWT"}
	headerJSON, err := json.Marshal(header)
	if err != nil {
		t.Fatalf("marshal header: %v", err)
	}
	claims := map[string]any{}
	standard, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	if err := json.Unmarshal(standard, &claims); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	maps.Copy(claims, payload.Custom)
	payloadJSON, err := json.Marshal(claims)
	if err != nil {
		t.Fatalf("marshal claims: %v", err)
	}

	message := base64.RawURLEncoding.EncodeToString(headerJSON) + "." + base64.RawURLEncoding.EncodeToString(payloadJSON)

	h := hmac.New(sha256.New, secret)
	h.Write([]byte(message))
	signature := base64.RawURLEncoding.EncodeToString(h.Sum(nil))

	return message + "." + signature
}

// TestAuthenticateParallelJWTAndCache JWTでの認証成功と、2回目がキャッシュから
// 返ることを確認します。
func TestAuthenticateParallelJWTAndCache(t *testing.T) {
	system, _ := newTestAuthSystem(t)

	now := time.Now()
	token := signTestJWT(t, []byte("test-secret"), JWTPayload{
		Issuer:    "test-issuer",
		Subject:   "user1",
		Audience:  "test-audience",
		ExpiresAt: now.Add(time.Hour).Unix(),
		IssuedAt:  now.Unix(),
		Username:  "tester",
	})

	result, err := system.AuthenticateParallel(t.Context(), token)
	if err != nil {
		t.Fatalf("AuthenticateParallel failed: %v", err)
	}
	if !result.Success {
		t.Fatal("expected successful authentication")
	}
	if result.Provider != "jwt" {
		t.Errorf("expected jwt provider, got %s", result.Provider)
	}

	if got := system.metrics.cacheMisses.Load(); got != 1 {
		t.Errorf("expected 1 cache miss, got %d", got)
	}

	// 2回目はキャッシュから返るはず
	result2, err := system.AuthenticateParallel(t.Context(), token)
	if err != nil {
		t.Fatalf("second AuthenticateParallel failed: %v", err)
	}
	if !result2.Success {
		t.Fatal("expected cached result to be successful")
	}
	if got := system.metrics.cacheHits.Load(); got != 1 {
		t.Errorf("expected 1 cache hit, got %d", got)
	}
}

// TestAuthenticateParallelAPIKey APIキーでの認証成功を確認します
func TestAuthenticateParallelAPIKey(t *testing.T) {
	system, _ := newTestAuthSystem(t)

	result, err := system.AuthenticateParallel(t.Context(), "ak_test")
	if err != nil {
		t.Fatalf("AuthenticateParallel failed: %v", err)
	}
	if !result.Success {
		t.Fatal("expected successful authentication")
	}
	if result.UserID != "user1" {
		t.Errorf("expected user1, got %s", result.UserID)
	}
}

// TestAuthenticateParallelInvalidToken すべてのプロバイダーが失敗した場合にエラーになることを確認します
func TestAuthenticateParallelInvalidToken(t *testing.T) {
	system, _ := newTestAuthSystem(t)

	_, err := system.AuthenticateParallel(t.Context(), "not-a-valid-token")
	if err == nil {
		t.Fatal("expected error for invalid token")
	}
}

// TestConcurrentAuthenticateParallel 複数goroutineから同時にAuthenticateParallelを
// 呼んでもpanic・raceが起きないことを確認する回帰テスト。
func TestConcurrentAuthenticateParallel(t *testing.T) {
	system, _ := newTestAuthSystem(t)

	const numGoroutines = 30
	var wg sync.WaitGroup
	errs := make([]error, numGoroutines)

	for i := range numGoroutines {
		wg.Go(func() {
			_, err := system.AuthenticateParallel(t.Context(), "ak_test")
			errs[i] = err
		})
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("goroutine %d: unexpected error: %v", i, err)
		}
	}

	if got := system.metrics.totalRequests.Load(); got != int64(numGoroutines) {
		t.Errorf("expected %d total requests, got %d", numGoroutines, got)
	}
}

// TestJWTCustomClaims 標準以外のクレームがClaimsに入り、統計が累計されることを確認します
func TestJWTCustomClaims(t *testing.T) {
	provider := NewJWTProvider("jwt", []byte("test-secret"), "test-issuer", "test-audience", time.Second)
	now := time.Now()
	token := signTestJWT(t, []byte("test-secret"), JWTPayload{
		Issuer:    "test-issuer",
		Audience:  "test-audience",
		ExpiresAt: now.Add(time.Hour).Unix(),
		IssuedAt:  now.Unix(),
		Custom:    map[string]any{"tenant": "acme"},
	})

	for range 3 {
		result, err := provider.Authenticate(t.Context(), token)
		if err != nil {
			t.Fatalf("Authenticate: %v", err)
		}
		if got := result.Claims["tenant"]; got != "acme" {
			t.Fatalf("Claims[tenant] = %v, want acme", got)
		}
		if _, ok := result.Claims["iss"]; ok {
			t.Fatal("standard claim iss must not be copied into Claims")
		}
	}
	if _, err := provider.Authenticate(t.Context(), "broken"); err == nil {
		t.Fatal("expected error for malformed token")
	}

	if got := provider.stats.totalRequests.Load(); got != 4 {
		t.Errorf("totalRequests = %d, want 4", got)
	}
	if got := provider.stats.errorCount.Load(); got != 1 {
		t.Errorf("errorCount = %d, want 1", got)
	}
	if info := provider.GetProviderInfo(); info.SuccessRate != 75 {
		t.Errorf("SuccessRate = %v, want 75", info.SuccessRate)
	}
}
