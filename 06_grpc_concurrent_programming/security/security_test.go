package security

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func incomingWithToken(t *testing.T, header string) context.Context {
	t.Helper()
	return metadata.NewIncomingContext(t.Context(), metadata.Pairs("authorization", header))
}

func TestValidateJWT_期限切れはjwtのエラーとして判定できる(t *testing.T) {
	claims := &JWTClaims{
		UserID:    "u1",
		Role:      "admin",
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(-time.Hour)),
	}
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(JWTSecret)
	if err != nil {
		t.Fatalf("署名エラー: %v", err)
	}

	_, err = validateJWT(token)
	if !errors.Is(err, jwt.ErrTokenExpired) {
		t.Fatalf("validateJWT のエラー = %v, want jwt.ErrTokenExpired を含む", err)
	}
}

func TestExtractToken_Bearerは大文字小文字を問わない(t *testing.T) {
	tests := []struct {
		header string
		want   string
	}{
		{"Bearer abc", "abc"},
		{"bearer abc", "abc"},
		{"Basic abc", ""},
		{"Bearer", ""},
	}
	for _, tt := range tests {
		if got := extractToken(incomingWithToken(t, tt.header)); got != tt.want {
			t.Errorf("extractToken(%q) = %q, want %q", tt.header, got, tt.want)
		}
	}
}

func TestAuthInterceptor_トークンの有無で結果が変わる(t *testing.T) {
	interceptor := AuthInterceptor()
	info := &grpc.UnaryServerInfo{FullMethod: "/grpcservice.UserService/GetUser"}
	var gotUserID, gotRole string
	var gotOK bool
	handler := func(ctx context.Context, req any) (any, error) {
		gotUserID, gotRole, gotOK = UserFromContext(ctx)
		return "ok", nil
	}

	if _, err := interceptor(t.Context(), nil, info, handler); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("トークンなしのコード = %v, want Unauthenticated", status.Code(err))
	}

	token, err := GenerateJWT("u42", "admin")
	if err != nil {
		t.Fatalf("JWT生成エラー: %v", err)
	}
	if _, err := interceptor(incomingWithToken(t, "Bearer "+token), nil, info, handler); err != nil {
		t.Fatalf("トークンありのエラー: %v", err)
	}
	if !gotOK || gotUserID != "u42" || gotRole != "admin" {
		t.Fatalf("UserFromContext = (%q, %q, %v), want (u42, admin, true)", gotUserID, gotRole, gotOK)
	}
}

func TestUserFromContext_未認証のコンテキストではfalse(t *testing.T) {
	// 他パッケージが同じ値の別の型のキーで入れた値を拾わないこと
	type foreignKey int
	ctx := context.WithValue(t.Context(), foreignKey(userIDKey), "偽物")
	if _, _, ok := UserFromContext(ctx); ok {
		t.Fatal("未認証のコンテキストで ok=true になった")
	}
}

func TestRoleBasedAuthInterceptor_ロール不足はPermissionDenied(t *testing.T) {
	interceptor := RoleBasedAuthInterceptor([]string{"admin"})
	info := &grpc.UnaryServerInfo{FullMethod: "/grpcservice.UserService/DeleteUser"}
	handler := func(ctx context.Context, req any) (any, error) { return "ok", nil }

	token, err := GenerateJWT("u1", "viewer")
	if err != nil {
		t.Fatalf("JWT生成エラー: %v", err)
	}
	_, err = interceptor(incomingWithToken(t, "Bearer "+token), nil, info, handler)
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("コード = %v, want PermissionDenied", status.Code(err))
	}
}

func TestCreateTLSConfig_TLS12以上を要求する(t *testing.T) {
	if got := CreateTLSConfig().MinVersion; got < 0x0303 {
		t.Fatalf("MinVersion = %x, want TLS1.2以上", got)
	}
}
