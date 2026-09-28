package server

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"grpc-concurrent-programming/monitoring"
	pb "grpc-concurrent-programming/proto"
	"grpc-concurrent-programming/security"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// sampleUserCount 起動時に投入するサンプルユーザー数
const sampleUserCount = 1000

// ErrWorkerPoolClosed 停止済みのワーカープールに投入したときのエラーです
var ErrWorkerPoolClosed = errors.New("ワーカープールが停止されています")

// ErrWorkerPoolFull キューが満杯のときのエラーです
var ErrWorkerPoolFull = errors.New("ワーカープールが満杯です")

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
	TLSCertFile           string        `json:"tls_cert_file"` // EnableTLSのときに使うサーバー証明書（PEM）
	TLSKeyFile            string        `json:"tls_key_file"`  // EnableTLSのときに使う秘密鍵（PEM）
}

// ServerMetrics サーバーのパフォーマンス指標を追跡します
type ServerMetrics struct {
	TotalRequests      atomic.Int64
	ActiveRequests     atomic.Int64
	SuccessfulRequests atomic.Int64
	FailedRequests     atomic.Int64
	AverageLatency     atomic.Int64
	MaxLatency         atomic.Int64
	ActiveStreams      atomic.Int64
	TotalStreams       atomic.Int64
}

// ServerMetricsSnapshot ある時点のServerMetricsの値を保持します
type ServerMetricsSnapshot struct {
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
	for range maxWorkers {
		wp.wg.Go(wp.worker)
	}

	return wp
}

func (wp *WorkerPool) worker() {
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
	// キューに空きがあると下のselectは停止後でも送信を選びうるので、先に停止を確かめる
	if wp.ctx.Err() != nil {
		return ErrWorkerPoolClosed
	}

	select {
	case wp.jobQueue <- job:
		return nil
	case <-wp.ctx.Done():
		return ErrWorkerPoolClosed
	default:
		return ErrWorkerPoolFull
	}
}

// Close ワーカープールを終了します
func (wp *WorkerPool) Close() {
	wp.cancel()
	wp.wg.Wait()
}

// StreamManager ストリーミング接続を管理します
type StreamManager struct {
	subscribers sync.Map // key: chan *pb.UserEvent
	events      chan UserEvent
	ctx         context.Context
	cancel      context.CancelFunc
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
			grpcEvent := &pb.UserEvent{
				Type:      pb.UserEvent_EventType(pb.UserEvent_EventType_value[event.Type]),
				User:      event.User,
				Timestamp: event.Timestamp,
			}
			// 1つのストリームのSendを複数のgoroutineから呼ぶのは安全でないので、ここでは購読チャネルに渡すだけにし、Sendは各ハンドラーのgoroutineが呼ぶ
			sm.subscribers.Range(func(key, _ any) bool {
				select {
				case key.(chan *pb.UserEvent) <- grpcEvent:
				default:
					// 受信が追いつかない購読者の分はドロップ
				}
				return true
			})
		case <-sm.ctx.Done():
			return
		}
	}
}

// Subscribe イベントを受け取るチャネルと、購読を解除する関数を返します
func (sm *StreamManager) Subscribe() (<-chan *pb.UserEvent, func()) {
	ch := make(chan *pb.UserEvent, 100)
	sm.subscribers.Store(ch, struct{}{})
	return ch, func() { sm.subscribers.Delete(ch) }
}

// PublishEvent イベントを発行します
func (sm *StreamManager) PublishEvent(event UserEvent) {
	select {
	case sm.events <- event:
	default:
		// イベントキューが満杯の場合はドロップ
	}
}

// Close イベント配信を停止します
func (sm *StreamManager) Close() {
	sm.cancel()
}

// HighPerformanceUserServer 高性能ユーザーサーバーを実装します
type HighPerformanceUserServer struct {
	pb.UnimplementedUserServiceServer

	config     ServerConfig
	metrics    *ServerMetrics
	workerPool *WorkerPool
	userStore  sync.Map // 簡易ユーザーストア
	nextID     atomic.Int64
	streamMgr  *StreamManager
}

// NewHighPerformanceUserServer 新しい高性能ユーザーサーバーを作成します
func NewHighPerformanceUserServer(config ServerConfig) *HighPerformanceUserServer {
	// コンテナのCPU制限を反映するGOMAXPROCSに基づいてワーカー数を調整
	config.MaxWorkers = cmp.Or(config.MaxWorkers, runtime.GOMAXPROCS(0)*4)

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

// Close ワーカープールとイベント配信を停止します
func (s *HighPerformanceUserServer) Close() {
	s.workerPool.Close()
	s.streamMgr.Close()
}

func (s *HighPerformanceUserServer) initSampleData() {
	for n := range int64(sampleUserCount) {
		id := n + 1
		user := &pb.User{
			Id:        id,
			Name:      fmt.Sprintf("User%d", id),
			Email:     fmt.Sprintf("user%d@example.com", id),
			CreatedAt: time.Now().Unix(),
			UpdatedAt: time.Now().Unix(),
		}
		s.userStore.Store(id, user)
	}
	s.nextID.Store(sampleUserCount)
}

// newUserID 並行に呼んでも重複しないユーザーIDを払い出します（実際のアプリケーションではデータベースから取得）
func (s *HighPerformanceUserServer) newUserID() int64 {
	return s.nextID.Add(1)
}

// メトリクス更新ヘルパー
func (s *HighPerformanceUserServer) updateMetrics(start time.Time, success bool) {
	elapsed := time.Since(start).Nanoseconds()

	s.metrics.TotalRequests.Add(1)
	s.metrics.ActiveRequests.Add(-1)

	if success {
		s.metrics.SuccessfulRequests.Add(1)
	} else {
		s.metrics.FailedRequests.Add(1)
	}

	// レイテンシー更新（簡易移動平均）
	currentAvg := s.metrics.AverageLatency.Load()
	s.metrics.AverageLatency.Store((currentAvg + elapsed) / 2)

	// 最大レイテンシー更新
	for {
		currentMax := s.metrics.MaxLatency.Load()
		if elapsed <= currentMax || s.metrics.MaxLatency.CompareAndSwap(currentMax, elapsed) {
			break
		}
	}
}

// GetUser Unary RPC実装です
func (s *HighPerformanceUserServer) GetUser(ctx context.Context, req *pb.GetUserRequest) (_ *pb.GetUserResponse, err error) {
	start := time.Now()
	s.metrics.ActiveRequests.Add(1)
	defer func() { s.updateMetrics(start, err == nil) }()

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
		return nil, err
	case <-ctx.Done():
		return nil, status.Errorf(codes.Canceled, "リクエストがキャンセルされました")
	case <-time.After(5 * time.Second):
		return nil, status.Errorf(codes.DeadlineExceeded, "リクエストタイムアウト")
	}
}

// CreateUser Unary RPC実装です
func (s *HighPerformanceUserServer) CreateUser(ctx context.Context, req *pb.CreateUserRequest) (_ *pb.CreateUserResponse, err error) {
	start := time.Now()
	s.metrics.ActiveRequests.Add(1)
	defer func() { s.updateMetrics(start, err == nil) }()

	// 入力検証
	if req.Name == "" || req.Email == "" {
		return nil, status.Errorf(codes.InvalidArgument, "名前とメールアドレスは必須です")
	}

	resultChan := make(chan *pb.CreateUserResponse, 1)

	job := func() {
		newID := s.newUserID()

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
	case <-ctx.Done():
		return nil, status.Errorf(codes.Canceled, "リクエストがキャンセルされました")
	}
}

// ListUsers Server Streaming RPC実装です
func (s *HighPerformanceUserServer) ListUsers(req *pb.ListUsersRequest, stream pb.UserService_ListUsersServer) error {
	s.metrics.ActiveStreams.Add(1)
	s.metrics.TotalStreams.Add(1)
	defer s.metrics.ActiveStreams.Add(-1)

	pageSize := req.PageSize
	if pageSize <= 0 || pageSize > 100 {
		pageSize = 50 // デフォルトページサイズ
	}

	sentCount := int32(0)
	var sendErr error

	s.userStore.Range(func(key, value any) bool {
		if sentCount >= pageSize {
			return false
		}

		if user, ok := value.(*pb.User); ok {
			if sendErr = stream.Send(user); sendErr != nil {
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

	return sendErr
}

// WatchUserChanges Server Streaming RPC実装です
func (s *HighPerformanceUserServer) WatchUserChanges(req *pb.WatchUserChangesRequest, stream pb.UserService_WatchUserChangesServer) error {
	s.metrics.ActiveStreams.Add(1)
	s.metrics.TotalStreams.Add(1)
	defer s.metrics.ActiveStreams.Add(-1)

	events, unsubscribe := s.streamMgr.Subscribe()
	defer unsubscribe()

	// 購読の開始をクライアントに知らせる。以降に発行されたイベントはこのストリームに届く
	if err := stream.SendHeader(metadata.MD{}); err != nil {
		return err
	}

	for {
		select {
		case event := <-events:
			if err := stream.Send(event); err != nil {
				return err
			}
		case <-stream.Context().Done():
			return nil
		case <-s.streamMgr.ctx.Done():
			return status.Error(codes.Unavailable, "サーバーが停止しています")
		}
	}
}

// BulkCreateUsers Client Streaming RPC実装です
func (s *HighPerformanceUserServer) BulkCreateUsers(stream pb.UserService_BulkCreateUsersServer) error {
	s.metrics.ActiveStreams.Add(1)
	s.metrics.TotalStreams.Add(1)
	defer s.metrics.ActiveStreams.Add(-1)

	var createdUsers []*pb.User
	var validationErrors []string
	totalCreated := int32(0)

	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return status.Errorf(codes.Internal, "ストリーム受信エラー: %v", err)
		}

		// 入力検証
		if req.Name == "" || req.Email == "" {
			validationErrors = append(validationErrors, "名前とメールアドレスは必須です")
			continue
		}

		// ユーザー作成
		newID := s.newUserID()
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
		Errors:       validationErrors,
	}

	return stream.SendAndClose(response)
}

// UserChat Bidirectional Streaming RPC実装です
func (s *HighPerformanceUserServer) UserChat(stream pb.UserService_UserChatServer) error {
	s.metrics.ActiveStreams.Add(1)
	s.metrics.TotalStreams.Add(1)
	defer s.metrics.ActiveStreams.Add(-1)

	// 受信ゴルーチン
	messageChan := make(chan *pb.ChatMessage, 100)
	errorChan := make(chan error, 1)

	go func() {
		// errorChanへの送信はcloseより先に済むので、close後にerrorChanを見れば受信エラーを取りこぼさない
		defer close(messageChan)
		for {
			msg, err := stream.Recv()
			if err != nil {
				if !errors.Is(err, io.EOF) {
					errorChan <- err
				}
				return
			}
			select {
			case messageChan <- msg:
			case <-stream.Context().Done():
				return
			}
		}
	}()

	// メッセージ処理ループ
	for {
		select {
		case msg, ok := <-messageChan:
			if !ok {
				select {
				case err := <-errorChan:
					return err
				default:
					return nil // ストリーム終了
				}
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

		case <-stream.Context().Done():
			return nil
		}
	}
}

// GetMetrics メトリクス取得します
func (s *HighPerformanceUserServer) GetMetrics() ServerMetricsSnapshot {
	return ServerMetricsSnapshot{
		TotalRequests:      s.metrics.TotalRequests.Load(),
		ActiveRequests:     s.metrics.ActiveRequests.Load(),
		SuccessfulRequests: s.metrics.SuccessfulRequests.Load(),
		FailedRequests:     s.metrics.FailedRequests.Load(),
		AverageLatency:     s.metrics.AverageLatency.Load(),
		MaxLatency:         s.metrics.MaxLatency.Load(),
		ActiveStreams:      s.metrics.ActiveStreams.Load(),
		TotalStreams:       s.metrics.TotalStreams.Load(),
	}
}

// NewGRPCServer 認証とメトリクスのインターセプターを組み込んだgRPCサーバーを作成し、ユーザーサービスを登録します
func NewGRPCServer(config ServerConfig) (*grpc.Server, *HighPerformanceUserServer, error) {
	opts := []grpc.ServerOption{
		grpc.MaxConcurrentStreams(config.MaxConcurrentStreams),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:                  config.KeepaliveTime,
			Timeout:               config.KeepaliveTimeout,
			MaxConnectionIdle:     config.MaxConnectionIdle,
			MaxConnectionAge:      config.MaxConnectionAge,
			MaxConnectionAgeGrace: config.MaxConnectionAgeGrace,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             config.KeepaliveTime / 2,
			PermitWithoutStream: true,
		}),
		// 先に渡したインターセプターほど外側で動く
		grpc.ChainUnaryInterceptor(
			monitoring.MetricsInterceptor(),
			security.AuthInterceptor(),
		),
		grpc.ChainStreamInterceptor(
			monitoring.StreamMetricsInterceptor(),
			security.StreamAuthInterceptor(),
		),
	}
	// 0を渡すと、サイズは上限0バイト、ConnectionTimeoutは即時タイムアウトになるので、未設定ならgRPCの既定値に任せる
	if config.MaxReceiveMessageSize > 0 {
		opts = append(opts, grpc.MaxRecvMsgSize(config.MaxReceiveMessageSize))
	}
	if config.MaxSendMessageSize > 0 {
		opts = append(opts, grpc.MaxSendMsgSize(config.MaxSendMessageSize))
	}
	if config.ConnectionTimeout > 0 {
		opts = append(opts, grpc.ConnectionTimeout(config.ConnectionTimeout))
	}

	// TLS設定を追加（プロダクション環境の場合）
	if config.EnableTLS {
		creds, err := security.ServerTLSCredentials(config.TLSCertFile, config.TLSKeyFile)
		if err != nil {
			return nil, nil, err
		}
		opts = append(opts, grpc.Creds(creds))
	}

	srv := grpc.NewServer(opts...)
	userServer := NewHighPerformanceUserServer(config)
	pb.RegisterUserServiceServer(srv, userServer)

	return srv, userServer, nil
}

// StartHighPerformanceServer 高性能サーバーを起動します
func StartHighPerformanceServer(config ServerConfig) error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", config.Port))
	if err != nil {
		return fmt.Errorf("リスナー作成エラー: %w", err)
	}
	return Serve(lis, config)
}

// Serve 作成済みのリスナーで高性能サーバーを動かします。呼び出し側はServeを待たずにlisへ接続できます
func Serve(lis net.Listener, config ServerConfig) error {
	// Prometheusメトリクス初期化
	monitoring.InitMetrics()

	// メトリクスサーバーを別ゴルーチンで起動
	go func() {
		log.Printf("メトリクスサーバーを開始しています。ポート: 9090")
		if err := monitoring.StartMetricsServer(9090); err != nil {
			log.Printf("メトリクスサーバーエラー: %v", err)
		}
	}()

	srv, userServer, err := NewGRPCServer(config)
	if err != nil {
		return err
	}
	defer userServer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go logMetrics(ctx, userServer)

	log.Printf("高性能gRPCサーバーを開始しました。アドレス: %s", lis.Addr())
	log.Printf("最大ワーカー数: %d, 最大同時ストリーム数: %d", userServer.config.MaxWorkers, config.MaxConcurrentStreams)
	log.Printf("TLS有効: %v", config.EnableTLS)
	log.Printf("メトリクスURL: http://localhost:9090/metrics")

	return srv.Serve(lis)
}

// logMetrics ctxが終わるまで10秒ごとにメトリクスをログに出します
func logMetrics(ctx context.Context, userServer *HighPerformanceUserServer) {
	tick := time.Tick(10 * time.Second)
	for {
		select {
		case <-tick:
			metrics := userServer.GetMetrics()
			log.Printf("メトリクス - 総リクエスト: %d, アクティブ: %d, 成功: %d, 失敗: %d, 平均レイテンシー: %dms, アクティブストリーム: %d",
				metrics.TotalRequests,
				metrics.ActiveRequests,
				metrics.SuccessfulRequests,
				metrics.FailedRequests,
				metrics.AverageLatency/1000000, // nsをmsに変換
				metrics.ActiveStreams,
			)
		case <-ctx.Done():
			return
		}
	}
}
