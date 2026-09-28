package security

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// JWTSecret JWT署名用のシークレットキーです。環境変数JWT_SECRETから読み、未設定なら起動ごとに乱数で作る
var JWTSecret = loadJWTSecret()

func loadJWTSecret() []byte {
	if s := os.Getenv("JWT_SECRET"); s != "" {
		return []byte(s)
	}
	// 未設定のまま固定値で動かすと誰でもトークンを偽造できるので、プロセス内だけで有効な鍵を使う
	log.Printf("JWT_SECRETが未設定のため、起動ごとの乱数鍵を使います（発行したトークンは再起動で無効になります）")
	return []byte(rand.Text())
}

// CreateTLSConfig プロダクション環境向けのTLS設定の土台を作成します。
// 暗号スイートと鍵交換の曲線は指定しない。CurvePreferencesを空にしておくと、
// Go 1.24以降は耐量子のハイブリッド鍵交換X25519MLKEM768が既定で使われる
func CreateTLSConfig() *tls.Config {
	return &tls.Config{MinVersion: tls.VersionTLS12}
}

// ServerTLSCredentials サーバー証明書と秘密鍵のPEMファイルからTLS認証情報を作成します
func ServerTLSCredentials(certFile, keyFile string) (credentials.TransportCredentials, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("サーバー証明書の読み込みエラー: %w", err)
	}
	config := CreateTLSConfig()
	config.Certificates = []tls.Certificate{cert}
	return credentials.NewTLS(config), nil
}

// ClientTLSCredentials サーバー証明書を検証するCA証明書のPEMファイルからTLS認証情報を作成します
func ClientTLSCredentials(caFile string) (credentials.TransportCredentials, error) {
	pem, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("CA証明書の読み込みエラー: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, errors.New("CA証明書のPEMを解釈できません")
	}
	config := CreateTLSConfig()
	config.RootCAs = pool
	return credentials.NewTLS(config), nil
}

// extractToken gRPCコンテキストからJWTトークンを抽出します
func extractToken(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}

	authHeaders := md.Get("authorization")
	if len(authHeaders) == 0 {
		return ""
	}

	authHeader := authHeaders[0]
	if !strings.HasPrefix(strings.ToLower(authHeader), "bearer ") {
		return ""
	}

	return authHeader[7:] // "Bearer "の後の部分
}

// JWTClaims JWT内のクレーム情報を定義します
type JWTClaims struct {
	UserID string `json:"user_id"`
	Role   string `json:"role"`
	jwt.RegisteredClaims
}

// validateJWT JWTトークンの検証を行います
func validateJWT(tokenString string) (*JWTClaims, error) {
	if tokenString == "" {
		return nil, errors.New("トークンが空です")
	}

	token, err := jwt.ParseWithClaims(tokenString, &JWTClaims{}, func(token *jwt.Token) (any, error) {
		// 署名メソッドの検証
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("予期しない署名メソッド: %v", token.Header["alg"])
		}
		return JWTSecret, nil
	})

	if err != nil {
		return nil, fmt.Errorf("トークン解析エラー: %w", err)
	}

	if claims, ok := token.Claims.(*JWTClaims); ok && token.Valid {
		return claims, nil
	}

	return nil, errors.New("無効なトークンです")
}

// contextKey 他パッケージの文字列キーと衝突しないためのコンテキストキー型です
type contextKey int

const (
	userIDKey contextKey = iota
	userRoleKey
)

func withUser(ctx context.Context, claims *JWTClaims) context.Context {
	ctx = context.WithValue(ctx, userIDKey, claims.UserID)
	return context.WithValue(ctx, userRoleKey, claims.Role)
}

// UserFromContext 認証インターセプターが格納したユーザーIDとロールを取り出します
func UserFromContext(ctx context.Context) (userID, role string, ok bool) {
	userID, ok = ctx.Value(userIDKey).(string)
	if !ok {
		return "", "", false
	}
	role, _ = ctx.Value(userRoleKey).(string)
	return userID, role, true
}

// AuthInterceptor JWT認証インターセプターを提供します
func AuthInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		// 認証不要のエンドポイント（ヘルスチェック等）
		if isPublicEndpoint(info.FullMethod) {
			return handler(ctx, req)
		}

		// JWTトークン検証
		token := extractToken(ctx)
		claims, err := validateJWT(token)
		if err != nil {
			return nil, status.Errorf(codes.Unauthenticated, "認証が必要です: %v", err)
		}

		ctx = withUser(ctx, claims)

		return handler(ctx, req)
	}
}

// StreamAuthInterceptor ストリーミングRPC用のJWT認証インターセプターを提供します
func StreamAuthInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		// 認証不要のエンドポイント
		if isPublicEndpoint(info.FullMethod) {
			return handler(srv, ss)
		}

		// JWTトークン検証
		token := extractToken(ss.Context())
		claims, err := validateJWT(token)
		if err != nil {
			return status.Errorf(codes.Unauthenticated, "認証が必要です: %v", err)
		}

		wrappedStream := &wrappedServerStream{
			ServerStream: ss,
			ctx:          withUser(ss.Context(), claims),
		}

		return handler(srv, wrappedStream)
	}
}

// wrappedServerStream コンテキストを持つServerStreamのラッパー
type wrappedServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedServerStream) Context() context.Context {
	return w.ctx
}

// isPublicEndpoint 認証不要のエンドポイントかどうかを判定します
func isPublicEndpoint(fullMethod string) bool {
	publicEndpoints := []string{
		"/grpc.health.v1.Health/Check",
		"/grpcservice.UserService/GetPublicInfo", // 例: 公開情報取得
	}

	return slices.Contains(publicEndpoints, fullMethod)
}

// GenerateJWT JWTトークンを生成します（テスト用）
func GenerateJWT(userID, role string) (string, error) {
	claims := &JWTClaims{
		UserID:    userID,
		Role:      role,
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(24 * time.Hour)), // 24時間
		IssuedAt:  jwt.NewNumericDate(time.Now()),
		NotBefore: jwt.NewNumericDate(time.Now()),
		Issuer:    "grpc-concurrent-programming",
		Subject:   userID,
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(JWTSecret)
}

// RoleBasedAuthInterceptor ロールベース認証インターセプターを提供します
func RoleBasedAuthInterceptor(requiredRoles []string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		// 認証不要のエンドポイント
		if isPublicEndpoint(info.FullMethod) {
			return handler(ctx, req)
		}

		// JWTトークン検証
		token := extractToken(ctx)
		claims, err := validateJWT(token)
		if err != nil {
			return nil, status.Errorf(codes.Unauthenticated, "認証が必要です: %v", err)
		}

		// ロール検証
		if !hasRequiredRole(claims.Role, requiredRoles) {
			return nil, status.Errorf(codes.PermissionDenied, "権限が不足しています")
		}

		ctx = withUser(ctx, claims)

		return handler(ctx, req)
	}
}

// hasRequiredRole ユーザーが必要なロールを持っているかを確認します
func hasRequiredRole(userRole string, requiredRoles []string) bool {
	return slices.Contains(requiredRoles, userRole)
}
