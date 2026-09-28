package server

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	pb "grpc-concurrent-programming/proto"
	"grpc-concurrent-programming/security"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// testConfig テスト用のサーバー設定です
func testConfig() ServerConfig {
	return ServerConfig{
		MaxWorkers:            4,
		MaxConcurrentStreams:  100,
		MaxReceiveMessageSize: 4 * 1024 * 1024,
		MaxSendMessageSize:    4 * 1024 * 1024,
		ConnectionTimeout:     10 * time.Second,
		KeepaliveTime:         30 * time.Second,
		KeepaliveTimeout:      5 * time.Second,
	}
}

// newUnitServer gRPCを通さずに呼ぶためのサーバーを作り、テスト終了時に止めます
func newUnitServer(t *testing.T) *HighPerformanceUserServer {
	t.Helper()
	s := NewHighPerformanceUserServer(testConfig())
	t.Cleanup(s.Close)
	return s
}

// startTestServer 127.0.0.1の空きポートでインターセプター付きのサーバーを起動し、生成したクライアントを返します
func startTestServer(t *testing.T) (pb.UserServiceClient, *HighPerformanceUserServer) {
	t.Helper()
	return startTestServerWith(t, testConfig())
}

func startTestServerWith(t *testing.T, config ServerConfig) (pb.UserServiceClient, *HighPerformanceUserServer) {
	t.Helper()

	// Listenを先に済ませるので、Serveの開始を待たずに接続できる
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("リスナー作成エラー: %v", err)
	}

	srv, userServer, err := NewGRPCServer(config)
	if err != nil {
		t.Fatalf("サーバー作成エラー: %v", err)
	}
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() {
		srv.Stop()
		userServer.Close()
	})

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("クライアント作成エラー: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	return pb.NewUserServiceClient(conn), userServer
}

// authContext JWTを付けた送信用コンテキストを作ります
func authContext(t *testing.T) context.Context {
	t.Helper()
	token, err := security.GenerateJWT("test-user", "admin")
	if err != nil {
		t.Fatalf("JWT生成エラー: %v", err)
	}
	return metadata.AppendToOutgoingContext(t.Context(), "authorization", "Bearer "+token)
}

func TestGetUser_トークンなしはUnauthenticatedでトークンありは取得できる(t *testing.T) {
	client, _ := startTestServer(t)

	_, err := client.GetUser(t.Context(), &pb.GetUserRequest{Id: 1})
	if got := status.Code(err); got != codes.Unauthenticated {
		t.Fatalf("トークンなしのコード = %v, want Unauthenticated", got)
	}

	resp, err := client.GetUser(authContext(t), &pb.GetUserRequest{Id: 1})
	if err != nil {
		t.Fatalf("GetUser エラー: %v", err)
	}
	if resp.User.GetName() != "User1" {
		t.Fatalf("Name = %q, want User1", resp.User.GetName())
	}
}

func TestGetUser_メトリクスは1リクエストにつき1回だけ数える(t *testing.T) {
	s := newUnitServer(t)

	if _, err := s.GetUser(t.Context(), &pb.GetUserRequest{Id: 0}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Id=0 のコード = %v, want InvalidArgument", status.Code(err))
	}
	if _, err := s.GetUser(t.Context(), &pb.GetUserRequest{Id: 99999}); status.Code(err) != codes.NotFound {
		t.Fatalf("Id=99999 のコード = %v, want NotFound", status.Code(err))
	}
	if _, err := s.GetUser(t.Context(), &pb.GetUserRequest{Id: 1}); err != nil {
		t.Fatalf("Id=1 のエラー: %v", err)
	}

	m := s.GetMetrics()
	if m.TotalRequests != 3 || m.SuccessfulRequests != 1 || m.FailedRequests != 2 || m.ActiveRequests != 0 {
		t.Fatalf("メトリクス = total:%d success:%d failed:%d active:%d, want 3/1/2/0",
			m.TotalRequests, m.SuccessfulRequests, m.FailedRequests, m.ActiveRequests)
	}
}

func TestWorkerPool_停止後の投入はエラーになる(t *testing.T) {
	wp := NewWorkerPool(2, 10)
	wp.Close()

	// 停止済みでもキューに空きがあるので、ctxを先に見ないと投入が受理されうる
	for range 50 {
		if err := wp.Submit(func() {}); err == nil {
			t.Fatal("停止後の Submit が成功した")
		}
	}
}

func TestWorkerPool_投入と停止を並行させてもpanicしない(t *testing.T) {
	for range 50 {
		wp := NewWorkerPool(2, 4)
		var wg sync.WaitGroup
		for range 4 {
			wg.Go(func() {
				for range 20 {
					_ = wp.Submit(func() {})
				}
			})
		}
		wg.Go(wp.Close)
		wg.Wait()

		if err := wp.Submit(func() {}); err == nil {
			t.Fatal("停止後の Submit が成功した")
		}
	}
}

// failingListStream Sendが常に失敗するListUsers用ストリームです
type failingListStream struct {
	grpc.ServerStream
	err error
}

func (f *failingListStream) Send(*pb.User) error { return f.err }

func TestListUsers_送信エラーを返す(t *testing.T) {
	s := newUnitServer(t)
	sendErr := errors.New("送信失敗")

	err := s.ListUsers(&pb.ListUsersRequest{PageSize: 10}, &failingListStream{err: sendErr})
	if !errors.Is(err, sendErr) {
		t.Fatalf("ListUsers のエラー = %v, want %v", err, sendErr)
	}
}

// recvErrorChatStream Recvが常に失敗するUserChat用ストリームです
type recvErrorChatStream struct {
	grpc.ServerStream
	ctx context.Context
	err error
}

func (r *recvErrorChatStream) Recv() (*pb.ChatMessage, error) { return nil, r.err }
func (r *recvErrorChatStream) Send(*pb.ChatMessage) error     { return nil }
func (r *recvErrorChatStream) Context() context.Context       { return r.ctx }

func TestUserChat_受信エラーを取りこぼさない(t *testing.T) {
	s := newUnitServer(t)
	recvErr := status.Error(codes.Internal, "受信失敗")

	for range 50 {
		err := s.UserChat(&recvErrorChatStream{ctx: t.Context(), err: recvErr})
		if status.Code(err) != codes.Internal {
			t.Fatalf("UserChat のエラー = %v, want Internal", err)
		}
	}
}

func TestWatchUserChanges_作成イベントを受け取る(t *testing.T) {
	client, _ := startTestServer(t)
	ctx, cancel := context.WithCancel(authContext(t))
	defer cancel()

	watch, err := client.WatchUserChanges(ctx, &pb.WatchUserChangesRequest{})
	if err != nil {
		t.Fatalf("WatchUserChanges エラー: %v", err)
	}
	// ヘッダー受信はハンドラーの開始後に届くので、以降の作成イベントは登録済みのストリームに配られる
	if _, err := watch.Header(); err != nil {
		t.Fatalf("ヘッダー受信エラー: %v", err)
	}

	for i := range 20 {
		if _, err := client.CreateUser(authContext(t), &pb.CreateUserRequest{Name: "watch", Email: "w@example.com"}); err != nil {
			t.Fatalf("CreateUser %d エラー: %v", i, err)
		}
	}

	for i := range 20 {
		ev, err := watch.Recv()
		if err != nil {
			t.Fatalf("イベント %d の受信エラー: %v", i, err)
		}
		if ev.GetType() != pb.UserEvent_CREATED || ev.GetUser().GetName() != "watch" {
			t.Fatalf("イベント %d = %v, want CREATED/watch", i, ev)
		}
	}
}

func TestCreateUser_並行に作成してもIDが重複しない(t *testing.T) {
	s := newUnitServer(t)
	var mu sync.Mutex
	ids := make(map[int64]struct{})
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 25 {
				resp, err := s.CreateUser(t.Context(), &pb.CreateUserRequest{Name: "n", Email: "e@example.com"})
				if err != nil {
					t.Errorf("CreateUser エラー: %v", err)
					return
				}
				mu.Lock()
				ids[resp.User.GetId()] = struct{}{}
				mu.Unlock()
			}
		})
	}
	wg.Wait()

	if len(ids) != 200 {
		t.Fatalf("一意なID数 = %d, want 200", len(ids))
	}
}

// TestNewGRPCServer_サイズと接続タイムアウトが0ならgRPCの既定値で動く
func TestNewGRPCServer_サイズと接続タイムアウトが0ならgRPCの既定値で動く(t *testing.T) {
	config := testConfig()
	config.MaxReceiveMessageSize = 0
	config.MaxSendMessageSize = 0
	config.ConnectionTimeout = 0
	client, _ := startTestServerWith(t, config)

	if _, err := client.GetUser(authContext(t), &pb.GetUserRequest{Id: 1}); err != nil {
		t.Fatalf("GetUser エラー: %v", err)
	}
}
