package server

import (
	"context"
	"fmt"
	"log"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"grpc-concurrent-programming/monitoring"
	pb "grpc-concurrent-programming/proto"
	"grpc-concurrent-programming/security"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

// ServerConfig 高性能gRPCサーバーの設定を定義します
type ServerConfig struct {
	Port                  int           `json:"port"`
	MaxWorkers            int           `json:"max_workers"`
	MaxConcurrentStreams  uint32        `json:"max_concurrent_streams"`
	MaxReceiveMessageSize int           `json:"max_receive_message_size"`
	MaxSendMessageSize    int           `json:"max_send_message_size"`
	ConnectionTimeout     time.Duration `json:"connection_timeout"`
	KeepaliveTime         time.Duration `json:"keepalive_time"`
	KeepaliveTimeout      time.Duration `json:"keepalive_timeout"`
	MaxConnectionIdle     time.Duration `json:"max_connection_idle"`
	MaxConnectionAge      time.Duration `json:"max_connection_age"`
	MaxConnectionAgeGrace time.Duration `json:"max_connection_age_grace"`
	EnableTLS             bool          `json:"enable_tls"`
}

// ServerMetrics サーバーのパフォーマンス指標を追跡します
type ServerMetrics struct {
	TotalRequests      int64 `json:"total_requests"`
	ActiveRequests     int64 `json:"active_requests"`
	SuccessfulRequests int64 `json:"successful_requests"`
	FailedRequests     int64 `json:"failed_requests"`
	AverageLatency     int64 `json:"average_latency_ns"`
	MaxLatency         int64 `json:"max_latency_ns"`
	ActiveStreams      int64 `json:"active_streams"`
	TotalStreams       int64 `json:"total_streams"`
}

// WorkerPool 並行処理のためのワーカープールを管理します
type WorkerPool struct {
	workers    chan struct{}
	jobQueue   chan func()
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	maxWorkers int
}

// NewWorkerPool 新しいワーカープールを作成します
func NewWorkerPool(maxWorkers, queueSize int) *WorkerPool {
	ctx, cancel := context.WithCancel(context.Background())

	wp := &WorkerPool{
		workers:    make(chan struct{}, maxWorkers),
		jobQueue:   make(chan func(), queueSize),
		ctx:        ctx,
		cancel:     cancel,
		maxWorkers: maxWorkers,
	}

	// ワーカーゴルーチン起動
	for i := 0; i < maxWorkers; i++ {
		wp.wg.Add(1)
		go wp.worker()
	}

	return wp
}

func (wp *WorkerPool) worker() {
	defer wp.wg.Done()

	for {
		select {
		case job := <-wp.jobQueue:
			wp.workers <- struct{}{} // ワーカー使用開始
			job()
			<-wp.workers // ワーカー使用終了
		case <-wp.ctx.Done():
			return
		}
	}
}

// Submit ワーカープールにジョブを送信します
func (wp *WorkerPool) Submit(job func()) error {
	select {
	case wp.jobQueue <- job:
		return nil
	case <-wp.ctx.Done():
		return fmt.Errorf("ワーカープールが停止されています")
	default:
		return fmt.Errorf("ワーカープールが満杯です")
	}
}

// Close ワーカープールを終了します
func (wp *WorkerPool) Close() {
	wp.cancel()
	wp.wg.Wait()
}

// StreamManager ストリーミング接続を管理します
type StreamManager struct {
	streams sync.Map
	events  chan UserEvent
	ctx     context.Context
	cancel  context.CancelFunc
}

// UserEvent ユーザーイベントを表します
type UserEvent struct {
	Type      string
	User      *pb.User
	Timestamp int64
}

// NewStreamManager 新しいストリーム管理者を作成します
func NewStreamManager() *StreamManager {
	ctx, cancel := context.WithCancel(context.Background())

	sm := &StreamManager{
		events: make(chan UserEvent, 1000),
		ctx:    ctx,
		cancel: cancel,
	}

	go sm.eventDistributor()

	return sm
}

func (sm *StreamManager) eventDistributor() {
	for {
		select {
		case event := <-sm.events:
			sm.streams.Range(func(key, value interface{}) bool {
				if stream, ok := value.(pb.UserService_WatchUserChangesServer); ok {
					grpcEvent := &pb.UserEvent{
						Type:      pb.UserEvent_EventType(pb.UserEvent_EventType_value[event.Type]),
						User:      event.User,
						Timestamp: event.Timestamp,
					}

					select {
					case <-stream.Context().Done():
						sm.streams.Delete(key)
					default:
						if err := stream.Send(grpcEvent); err != nil {
							sm.streams.Delete(key)
						}
					}
				}
				return true
			})
		case <-sm.ctx.Done():
			return
		}
	}
}

// RegisterStream ストリームを登録します
func (sm *StreamManager) RegisterStream(streamID string, stream pb.UserService_WatchUserChangesServer) {
	sm.streams.Store(streamID, stream)
}

// UnregisterStream ストリームの登録を解除します
func (sm *StreamManager) UnregisterStream(streamID string) {
	sm.streams.Delete(streamID)
}

// PublishEvent イベントを発行します
func (sm *StreamManager) PublishEvent(event UserEvent) {
	select {
	case sm.events <- event:
	default:
		// イベントキューが満杯の場合はドロップ
	}
}

// HighPerformanceUserServer 高性能ユーザーサーバーを実装します
type HighPerformanceUserServer struct {
	pb.UnimplementedUserServiceServer

	config     ServerConfig
	metrics    *ServerMetrics
	workerPool *WorkerPool
	userStore  sync.Map // 簡易ユーザーストア
	streamMgr  *StreamManager
}

// NewHighPerformanceUserServer 新しい高性能ユーザーサーバーを作成します
func NewHighPerformanceUserServer(config ServerConfig) *HighPerformanceUserServer {
	// CPUコア数に基づいてワーカー数を調整
	if config.MaxWorkers == 0 {
		config.MaxWorkers = runtime.NumCPU() * 4
	}

	server := &HighPerformanceUserServer{
		config:     config,
		metrics:    &ServerMetrics{},
		workerPool: NewWorkerPool(config.MaxWorkers, config.MaxWorkers*2),
		streamMgr:  NewStreamManager(),
	}

	// サンプルデータ投入
	server.initSampleData()

	return server
}

func (s *HighPerformanceUserServer) initSampleData() {
	for i := int64(1); i <= 1000; i++ {
		user := &pb.User{
			Id:        i,
			Name:      fmt.Sprintf("User%d", i),
			Email:     fmt.Sprintf("user%d@example.com", i),
			CreatedAt: time.Now().Unix(),
			UpdatedAt: time.Now().Unix(),
		}
		s.userStore.Store(i, user)
	}
}

// メトリクス更新ヘルパー
func (s *HighPerformanceUserServer) updateMetrics(start time.Time, success bool) {
	elapsed := time.Since(start).Nanoseconds()

	atomic.AddInt64(&s.metrics.TotalRequests, 1)
	atomic.AddInt64(&s.metrics.ActiveRequests, -1)

	if success {
		atomic.AddInt64(&s.metrics.SuccessfulRequests, 1)
	} else {
		atomic.AddInt64(&s.metrics.FailedRequests, 1)
	}

	// レイテンシー更新（簡易移動平均）
	currentAvg := atomic.LoadInt64(&s.metrics.AverageLatency)
	newAvg := (currentAvg + elapsed) / 2
	atomic.StoreInt64(&s.metrics.AverageLatency, newAvg)

	// 最大レイテンシー更新
	for {
		currentMax := atomic.LoadInt64(&s.metrics.MaxLatency)
		if elapsed <= currentMax {
			break
		}
		if atomic.CompareAndSwapInt64(&s.metrics.MaxLatency, currentMax, elapsed) {
			break
		}
	}
}

// GetUser Unary RPC実装です
func (s *HighPerformanceUserServer) GetUser(ctx context.Context, req *pb.GetUserRequest) (*pb.GetUserResponse, error) {
	start := time.Now()
	atomic.AddInt64(&s.metrics.ActiveRequests, 1)
	defer s.updateMetrics(start, true)

	// 入力検証
	if req.Id <= 0 {
		return nil, status.Errorf(codes.InvalidArgument, "無効なユーザーID: %d", req.Id)
	}

	// 非同期処理でワーカープールを使用
	resultChan := make(chan *pb.GetUserResponse, 1)
	errorChan := make(chan error, 1)

	job := func() {
		if userInterface, ok := s.userStore.Load(req.Id); ok {
			if user, ok := userInterface.(*pb.User); ok {
				resultChan <- &pb.GetUserResponse{User: user}
				return
			}
		}
		errorChan <- status.Errorf(codes.NotFound, "ユーザーが見つかりません: %d", req.Id)
	}

	if err := s.workerPool.Submit(job); err != nil {
		return nil, status.Errorf(codes.ResourceExhausted, "サーバーが過負荷状態です")
	}

	select {
	case result := <-resultChan:
		return result, nil
	case err := <-errorChan:
		s.updateMetrics(start, false)
		return nil, err
	case <-ctx.Done():
		s.updateMetrics(start, false)
		return nil, status.Errorf(codes.Canceled, "リクエストがキャンセルされました")
	case <-time.After(5 * time.Second):
		s.updateMetrics(start, false)
		return nil, status.Errorf(codes.DeadlineExceeded, "リクエストタイムアウト")
	}
}

// CreateUser Unary RPC実装です
func (s *HighPerformanceUserServer) CreateUser(ctx context.Context, req *pb.CreateUserRequest) (*pb.CreateUserResponse, error) {
	start := time.Now()
	atomic.AddInt64(&s.metrics.ActiveRequests, 1)
	defer s.updateMetrics(start, true)

	// 入力検証
	if req.Name == "" || req.Email == "" {
		return nil, status.Errorf(codes.InvalidArgument, "名前とメールアドレスは必須です")
	}

	resultChan := make(chan *pb.CreateUserResponse, 1)
	errorChan := make(chan error, 1)

	job := func() {
		// 新しいIDを生成（実際のアプリケーションではデータベースから取得）
		newID := time.Now().UnixNano()

		user := &pb.User{
			Id:        newID,
			Name:      req.Name,
			Email:     req.Email,
			CreatedAt: time.Now().Unix(),
			UpdatedAt: time.Now().Unix(),
		}

		s.userStore.Store(newID, user)

		// イベント発行
		s.streamMgr.PublishEvent(UserEvent{
			Type:      "CREATED",
			User:      user,
			Timestamp: time.Now().Unix(),
		})

		resultChan <- &pb.CreateUserResponse{User: user}
	}

	if err := s.workerPool.Submit(job); err != nil {
		return nil, status.Errorf(codes.ResourceExhausted, "サーバーが過負荷状態です")
	}

	select {
	case result := <-resultChan:
		return result, nil
	case err := <-errorChan:
		s.updateMetrics(start, false)
		return nil, err
	case <-ctx.Done():
		s.updateMetrics(start, false)
		return nil, status.Errorf(codes.Canceled, "リクエストがキャンセルされました")
	}
}

// ListUsers Server Streaming RPC実装です
func (s *HighPerformanceUserServer) ListUsers(req *pb.ListUsersRequest, stream pb.UserService_ListUsersServer) error {
	atomic.AddInt64(&s.metrics.ActiveStreams, 1)
	atomic.AddInt64(&s.metrics.TotalStreams, 1)
	defer atomic.AddInt64(&s.metrics.ActiveStreams, -1)

	pageSize := req.PageSize
	if pageSize <= 0 || pageSize > 100 {
		pageSize = 50 // デフォルトページサイズ
	}

	sentCount := int32(0)

	s.userStore.Range(func(key, value interface{}) bool {
		if sentCount >= pageSize {
			return false
		}

		if user, ok := value.(*pb.User); ok {
			if err := stream.Send(user); err != nil {
				return false
			}
			sentCount++
		}

		// 背圧制御：少し待機してCPU使用率を調整
		if sentCount%10 == 0 {
			time.Sleep(1 * time.Millisecond)
		}

		return true
	})

	return nil
}

// WatchUserChanges Server Streaming RPC実装です
func (s *HighPerformanceUserServer) WatchUserChanges(req *pb.WatchUserChangesRequest, stream pb.UserService_WatchUserChangesServer) error {
	atomic.AddInt64(&s.metrics.ActiveStreams, 1)
	atomic.AddInt64(&s.metrics.TotalStreams, 1)
	defer atomic.AddInt64(&s.metrics.ActiveStreams, -1)

	streamID := fmt.Sprintf("stream_%d", time.Now().UnixNano())
	s.streamMgr.RegisterStream(streamID, stream)
	defer s.streamMgr.UnregisterStream(streamID)

	// ストリームが終了するまで待機
	<-stream.Context().Done()

	return nil
}

// BulkCreateUsers Client Streaming RPC実装です
func (s *HighPerformanceUserServer) BulkCreateUsers(stream pb.UserService_BulkCreateUsersServer) error {
	atomic.AddInt64(&s.metrics.ActiveStreams, 1)
	atomic.AddInt64(&s.metrics.TotalStreams, 1)
	defer atomic.AddInt64(&s.metrics.ActiveStreams, -1)

	var createdUsers []*pb.User
	var errors []string
	totalCreated := int32(0)

	for {
		req, err := stream.Recv()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return status.Errorf(codes.Internal, "ストリーム受信エラー: %v", err)
		}

		// 入力検証
		if req.Name == "" || req.Email == "" {
			errors = append(errors, "名前とメールアドレスは必須です")
			continue
		}

		// ユーザー作成
		newID := time.Now().UnixNano() + int64(totalCreated)
		user := &pb.User{
			Id:        newID,
			Name:      req.Name,
			Email:     req.Email,
			CreatedAt: time.Now().Unix(),
			UpdatedAt: time.Now().Unix(),
		}

		s.userStore.Store(newID, user)
		createdUsers = append(createdUsers, user)
		totalCreated++

		// イベント発行
		s.streamMgr.PublishEvent(UserEvent{
			Type:      "CREATED",
			User:      user,
			Timestamp: time.Now().Unix(),
		})

		// 背圧制御
		if totalCreated%100 == 0 {
			time.Sleep(10 * time.Millisecond)
		}
	}

	response := &pb.BulkCreateUsersResponse{
		Users:        createdUsers,
		TotalCreated: totalCreated,
		Errors:       errors,
	}

	return stream.SendAndClose(response)
}

// UserChat Bidirectional Streaming RPC実装です
func (s *HighPerformanceUserServer) UserChat(stream pb.UserService_UserChatServer) error {
	atomic.AddInt64(&s.metrics.ActiveStreams, 1)
	atomic.AddInt64(&s.metrics.TotalStreams, 1)
	defer atomic.AddInt64(&s.metrics.ActiveStreams, -1)

	// 受信ゴルーチン
	messageChan := make(chan *pb.ChatMessage, 100)
	errorChan := make(chan error, 1)

	go func() {
		defer close(messageChan)
		for {
			msg, err := stream.Recv()
			if err != nil {
				if err.Error() != "EOF" {
					errorChan <- err
				}
				return
			}
			messageChan <- msg
		}
	}()

	// メッセージ処理ループ
	for {
		select {
		case msg := <-messageChan:
			if msg == nil {
				return nil // ストリーム終了
			}

			// エコー応答（実際のアプリケーションではビジネスロジックを実装）
			response := &pb.ChatMessage{
				UserId:    0, // システムユーザー
				Content:   fmt.Sprintf("エコー: %s", msg.Content),
				Timestamp: time.Now().Unix(),
				MessageId: fmt.Sprintf("echo_%d", time.Now().UnixNano()),
			}

			if err := stream.Send(response); err != nil {
				return err
			}

		case err := <-errorChan:
			return err

		case <-stream.Context().Done():
			return nil
		}
	}
}

// GetMetrics メトリクス取得します
func (s *HighPerformanceUserServer) GetMetrics() ServerMetrics {
	return ServerMetrics{
		TotalRequests:      atomic.LoadInt64(&s.metrics.TotalRequests),
		ActiveRequests:     atomic.LoadInt64(&s.metrics.ActiveRequests),
		SuccessfulRequests: atomic.LoadInt64(&s.metrics.SuccessfulRequests),
		FailedRequests:     atomic.LoadInt64(&s.metrics.FailedRequests),
		AverageLatency:     atomic.LoadInt64(&s.metrics.AverageLatency),
		MaxLatency:         atomic.LoadInt64(&s.metrics.MaxLatency),
		ActiveStreams:      atomic.LoadInt64(&s.metrics.ActiveStreams),
		TotalStreams:       atomic.LoadInt64(&s.metrics.TotalStreams),
	}
}

// StartHighPerformanceServer 高性能サーバーを起動します
func StartHighPerformanceServer(config ServerConfig) error {
	// Prometheusメトリクス初期化
	monitoring.InitMetrics()

	// メトリクスサーバーを別ゴルーチンで起動
	go func() {
		log.Printf("メトリクスサーバーを開始しています。ポート: 9090")
		if err := monitoring.StartMetricsServer(9090); err != nil {
			log.Printf("メトリクスサーバーエラー: %v", err)
		}
	}()

	// gRPCサーバー設定
	opts := []grpc.ServerOption{
		grpc.MaxConcurrentStreams(config.MaxConcurrentStreams),
		grpc.MaxRecvMsgSize(config.MaxReceiveMessageSize),
		grpc.MaxSendMsgSize(config.MaxSendMessageSize),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    config.KeepaliveTime,
			Timeout: config.KeepaliveTimeout,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             config.KeepaliveTime / 2,
			PermitWithoutStream: true,
		}),
		grpc.ConnectionTimeout(config.ConnectionTimeout),
		// セキュリティインターセプターを追加
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
			monitoring.MetricsInterceptor(),
			security.AuthInterceptor(),
		)),
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(
			monitoring.StreamMetricsInterceptor(),
			security.StreamAuthInterceptor(),
		)),
	}

	// TLS設定を追加（プロダクション環境の場合）
	if config.EnableTLS {
		creds := security.CreateTLSCredentials()
		opts = append(opts, grpc.Creds(creds))
		log.Printf("TLSが有効化されています")
	}

	server := grpc.NewServer(opts...)
	userServer := NewHighPerformanceUserServer(config)

	pb.RegisterUserServiceServer(server, userServer)

	// メトリクス監視ゴルーチン
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		for range ticker.C {
			metrics := userServer.GetMetrics()
			log.Printf("メトリクス - 総リクエスト: %d, アクティブ: %d, 成功: %d, 失敗: %d, 平均レイテンシー: %dms, アクティブストリーム: %d",
				metrics.TotalRequests,
				metrics.ActiveRequests,
				metrics.SuccessfulRequests,
				metrics.FailedRequests,
				metrics.AverageLatency/1000000, // nsをmsに変換
				metrics.ActiveStreams,
			)
		}
	}()

	// リスナー作成
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", config.Port))
	if err != nil {
		return fmt.Errorf("リスナー作成エラー: %w", err)
	}

	log.Printf("高性能gRPCサーバーを開始しました。ポート: %d", config.Port)
	log.Printf("最大ワーカー数: %d, 最大同時ストリーム数: %d", config.MaxWorkers, config.MaxConcurrentStreams)
	log.Printf("TLS有効: %v", config.EnableTLS)
	log.Printf("メトリクスURL: http://localhost:9090/metrics")

	return server.Serve(lis)
}
