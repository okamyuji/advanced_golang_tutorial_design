package client

import (
	"net"
	"sync"
	"testing"
	"time"

	pb "grpc-concurrent-programming/proto"
	"grpc-concurrent-programming/security"
	"grpc-concurrent-programming/server"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// newTestClient 認証付きのテスト用サーバーを起動し、JWTを設定したクライアントを返します
func newTestClient(t *testing.T) *HighPerformanceGRPCClient {
	t.Helper()

	// Listenを先に済ませるので、Serveの開始を待たずに接続できる
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("リスナー作成エラー: %v", err)
	}
	srv, userServer, err := server.NewGRPCServer(server.ServerConfig{
		MaxWorkers:            4,
		MaxConcurrentStreams:  100,
		MaxReceiveMessageSize: 4 * 1024 * 1024,
		MaxSendMessageSize:    4 * 1024 * 1024,
		ConnectionTimeout:     10 * time.Second,
		KeepaliveTime:         30 * time.Second,
		KeepaliveTimeout:      5 * time.Second,
	})
	if err != nil {
		t.Fatalf("サーバー作成エラー: %v", err)
	}
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() {
		srv.Stop()
		userServer.Close()
	})

	token, err := security.GenerateJWT("test-user", "admin")
	if err != nil {
		t.Fatalf("JWT生成エラー: %v", err)
	}

	c, err := NewHighPerformanceGRPCClient(ClientConfig{
		ServerAddresses:         []string{lis.Addr().String()},
		MaxConnections:          1,
		MaxRetries:              1,
		InitialBackoff:          10 * time.Millisecond,
		MaxBackoff:              50 * time.Millisecond,
		BackoffMultiplier:       2,
		MaxReceiveMessageSize:   4 * 1024 * 1024,
		MaxSendMessageSize:      4 * 1024 * 1024,
		DefaultTimeout:          10 * time.Second,
		CircuitBreakerThreshold: 5,
		CircuitBreakerTimeout:   time.Second,
		AuthToken:               token,
	})
	if err != nil {
		t.Fatalf("クライアント作成エラー: %v", err)
	}
	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Errorf("Close エラー: %v", err)
		}
	})
	return c
}

func TestGetUser_認証付きで取得できる(t *testing.T) {
	c := newTestClient(t)

	user, err := c.GetUser(t.Context(), 1)
	if err != nil {
		t.Fatalf("GetUser エラー: %v", err)
	}
	if user.GetId() != 1 {
		t.Fatalf("Id = %d, want 1", user.GetId())
	}
}

func TestCreateUser_認証トークンを付けて送る(t *testing.T) {
	c := newTestClient(t)

	user, err := c.CreateUser(t.Context(), "作成", "create@example.com")
	if err != nil {
		t.Fatalf("CreateUser エラー: %v", err)
	}
	if user.GetName() != "作成" {
		t.Fatalf("Name = %q, want 作成", user.GetName())
	}
}

func TestListUsersStream_ページサイズ分を受け取る(t *testing.T) {
	c := newTestClient(t)

	users, errs := c.ListUsersStream(t.Context(), 10)
	count := 0
	for range users {
		count++
	}
	if err := <-errs; err != nil {
		t.Fatalf("ListUsersStream エラー: %v", err)
	}
	if count != 10 {
		t.Fatalf("受信件数 = %d, want 10", count)
	}
}

func TestBulkCreateUsersStream_不正な入力はエラー一覧に入る(t *testing.T) {
	c := newTestClient(t)

	resp, err := c.BulkCreateUsersStream(t.Context(), []*pb.CreateUserRequest{
		{Name: "a", Email: "a@example.com"},
		{Name: "", Email: "b@example.com"},
		{Name: "c", Email: "c@example.com"},
	})
	if err != nil {
		t.Fatalf("BulkCreateUsersStream エラー: %v", err)
	}
	if resp.GetTotalCreated() != 2 || len(resp.GetErrors()) != 1 {
		t.Fatalf("作成数=%d エラー数=%d, want 2/1", resp.GetTotalCreated(), len(resp.GetErrors()))
	}
}

func TestUserChatStream_エコーを受け取る(t *testing.T) {
	c := newTestClient(t)

	stream, err := c.UserChatStream(t.Context())
	if err != nil {
		t.Fatalf("UserChatStream エラー: %v", err)
	}
	if err := stream.Send(&pb.ChatMessage{Content: "こんにちは"}); err != nil {
		t.Fatalf("Send エラー: %v", err)
	}
	msg, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv エラー: %v", err)
	}
	if msg.GetContent() != "エコー: こんにちは" {
		t.Fatalf("Content = %q", msg.GetContent())
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("CloseSend エラー: %v", err)
	}
}

func TestIsRetryableError_ステータスコードで判定する(t *testing.T) {
	c := &HighPerformanceGRPCClient{}
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"Unavailable", status.Error(codes.Unavailable, "接続できません"), true},
		{"DeadlineExceeded", status.Error(codes.DeadlineExceeded, "リクエストタイムアウト"), true},
		{"説明文にtimeoutを含むNotFound", status.Error(codes.NotFound, "timeout user not found"), false},
		{"InvalidArgument", status.Error(codes.InvalidArgument, "無効なユーザーID"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := c.isRetryableError(tt.err); got != tt.want {
				t.Fatalf("isRetryableError(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

func TestCircuitBreaker_半開への遷移を並行に呼んでもデータ競合しない(t *testing.T) {
	cb := NewCircuitBreaker(1, time.Second)
	cb.OnFailure()
	// 待たずにtimeout経過後の状態を作る
	cb.lastFailTime = time.Now().Add(-time.Hour)

	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() { cb.CanExecute() })
	}
	wg.Wait()

	if got := cb.GetState(); got != CircuitBreakerHalfOpen {
		t.Fatalf("状態 = %v, want HalfOpen", got)
	}
}

func TestCircuitBreaker_閾値で開き成功で閉じる(t *testing.T) {
	cb := NewCircuitBreaker(2, time.Hour)
	cb.OnFailure()
	if !cb.CanExecute() {
		t.Fatal("閾値未満で実行不可になった")
	}
	cb.OnFailure()
	if cb.CanExecute() {
		t.Fatal("閾値到達後も実行可能のまま")
	}
	cb.OnSuccess()
	if got := cb.GetState(); got != CircuitBreakerClosed {
		t.Fatalf("状態 = %v, want Closed", got)
	}
}
