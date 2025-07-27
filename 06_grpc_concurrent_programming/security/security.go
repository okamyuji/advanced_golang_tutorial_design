package security

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// JWTSecret JWT署名用のシークレットキー（実際の運用では環境変数等から取得）
var JWTSecret = []byte("your-secret-key-change-in-production")

// createTLSConfig プロダクション環境向けのTLS設定を作成します
func CreateTLSConfig() *tls.Config {
	return &tls.Config{
		MinVersion: tls.VersionTLS12,
		CipherSuites: []uint16{
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305,
		},
		PreferServerCipherSuites: true,
		CurvePreferences: []tls.CurveID{
			tls.CurveP256,
			tls.X25519,
		},
	}
}

// CreateTLSCredentials TLS認証情報を作成します
func CreateTLSCredentials() credentials.TransportCredentials {
	return credentials.NewTLS(CreateTLSConfig())
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
		return nil, fmt.Errorf("トークンが空です")
	}

	token, err := jwt.ParseWithClaims(tokenString, &JWTClaims{}, func(token *jwt.Token) (interface{}, error) {
		// 署名メソッドの検証
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("予期しない署名メソッド: %v", token.Header["alg"])
		}
		return JWTSecret, nil
	})

	if err != nil {
		return nil, fmt.Errorf("トークン解析エラー: %v", err)
	}

	if claims, ok := token.Claims.(*JWTClaims); ok && token.Valid {
		return claims, nil
	}

	return nil, fmt.Errorf("無効なトークンです")
}

// AuthInterceptor JWT認証インターセプターを提供します
func AuthInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
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

		// コンテキストにユーザー情報を追加
		ctx = context.WithValue(ctx, "user_id", claims.UserID)
		ctx = context.WithValue(ctx, "user_role", claims.Role)

		return handler(ctx, req)
	}
}

// StreamAuthInterceptor ストリーミングRPC用のJWT認証インターセプターを提供します
func StreamAuthInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
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

		// コンテキストにユーザー情報を追加
		ctx := context.WithValue(ss.Context(), "user_id", claims.UserID)
		ctx = context.WithValue(ctx, "user_role", claims.Role)

		// ラップされたServerStreamを作成
		wrappedStream := &wrappedServerStream{
			ServerStream: ss,
			ctx:          ctx,
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

	for _, endpoint := range publicEndpoints {
		if fullMethod == endpoint {
			return true
		}
	}

	return false
}

// GenerateJWT JWTトークンを生成します（テスト用）
func GenerateJWT(userID, role string) (string, error) {
	claims := &JWTClaims{
		UserID: userID,
		Role:   role,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(24 * time.Hour)), // 24時間
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			NotBefore: jwt.NewNumericDate(time.Now()),
			Issuer:    "grpc-concurrent-programming",
			Subject:   userID,
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(JWTSecret)
}

// RoleBasedAuthInterceptor ロールベース認証インターセプターを提供します
func RoleBasedAuthInterceptor(requiredRoles []string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
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

		// コンテキストにユーザー情報を追加
		ctx = context.WithValue(ctx, "user_id", claims.UserID)
		ctx = context.WithValue(ctx, "user_role", claims.Role)

		return handler(ctx, req)
	}
}

// hasRequiredRole ユーザーが必要なロールを持っているかを確認します
func hasRequiredRole(userRole string, requiredRoles []string) bool {
	for _, role := range requiredRoles {
		if userRole == role {
			return true
		}
	}
	return false
}
