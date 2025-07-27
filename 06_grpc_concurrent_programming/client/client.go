package client

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	pb "grpc-concurrent-programming/proto"
	"grpc-concurrent-programming/security"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
)

// ClientConfig gRPCクライアントの設定を定義します
type ClientConfig struct {
	ServerAddresses         []string      `json:"server_addresses"`
	MaxConnections          int           `json:"max_connections"`
	MaxRetries              int           `json:"max_retries"`
	InitialBackoff          time.Duration `json:"initial_backoff"`
	MaxBackoff              time.Duration `json:"max_backoff"`
	BackoffMultiplier       float64       `json:"backoff_multiplier"`
	KeepaliveTime           time.Duration `json:"keepalive_time"`
	KeepaliveTimeout        time.Duration `json:"keepalive_timeout"`
	PermitWithoutStream     bool          `json:"permit_without_stream"`
	MaxReceiveMessageSize   int           `json:"max_receive_message_size"`
	MaxSendMessageSize      int           `json:"max_send_message_size"`
	DefaultTimeout          time.Duration `json:"default_timeout"`
	CircuitBreakerThreshold int           `json:"circuit_breaker_threshold"`
	CircuitBreakerTimeout   time.Duration `json:"circuit_breaker_timeout"`
	EnableTLS               bool          `json:"enable_tls"`
	AuthToken               string        `json:"auth_token"` // JWT認証トークン
}

// ClientMetrics クライアントのパフォーマンス指標を追跡します
type ClientMetrics struct {
	TotalRequests       int64 `json:"total_requests"`
	SuccessfulRequests  int64 `json:"successful_requests"`
	FailedRequests      int64 `json:"failed_requests"`
	AverageLatency      int64 `json:"average_latency_ns"`
	MaxLatency          int64 `json:"max_latency_ns"`
	ActiveConnections   int64 `json:"active_connections"`
	CircuitBreakerTrips int64 `json:"circuit_breaker_trips"`
}

// ConnectionPool 接続プールを管理します
type ConnectionPool struct {
	connections []*grpc.ClientConn
	clients     []pb.UserServiceClient
	current     int64
	config      ClientConfig
	mutex       sync.RWMutex
}

// NewConnectionPool 新しい接続プールを作成します
func NewConnectionPool(config ClientConfig) (*ConnectionPool, error) {
	pool := &ConnectionPool{
		config: config,
	}

	for _, addr := range config.ServerAddresses {
		for i := 0; i < config.MaxConnections; i++ {
			conn, client, err := pool.createConnection(addr)
			if err != nil {
				pool.Close()
				return nil, fmt.Errorf("接続作成エラー (%s): %w", addr, err)
			}

			pool.connections = append(pool.connections, conn)
			pool.clients = append(pool.clients, client)
		}
	}

	return pool, nil
}

func (cp *ConnectionPool) createConnection(address string) (*grpc.ClientConn, pb.UserServiceClient, error) {
	opts := []grpc.DialOption{
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                cp.config.KeepaliveTime,
			Timeout:             cp.config.KeepaliveTimeout,
			PermitWithoutStream: cp.config.PermitWithoutStream,
		}),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(cp.config.MaxReceiveMessageSize),
			grpc.MaxCallSendMsgSize(cp.config.MaxSendMessageSize),
		),
	}

	// TLS設定
	if cp.config.EnableTLS {
		creds := security.CreateTLSCredentials()
		opts = append(opts, grpc.WithTransportCredentials(creds))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	conn, err := grpc.Dial(address, opts...)
	if err != nil {
		return nil, nil, err
	}

	client := pb.NewUserServiceClient(conn)

	return conn, client, nil
}

// GetClient ラウンドロビンでクライアントを取得します
func (cp *ConnectionPool) GetClient() pb.UserServiceClient {
	cp.mutex.RLock()
	defer cp.mutex.RUnlock()

	if len(cp.clients) == 0 {
		return nil
	}

	// ラウンドロビンによる負荷分散
	index := atomic.AddInt64(&cp.current, 1) % int64(len(cp.clients))
	return cp.clients[index]
}

// Close 接続プールを終了します
func (cp *ConnectionPool) Close() error {
	cp.mutex.Lock()
	defer cp.mutex.Unlock()

	var errors []error
	for _, conn := range cp.connections {
		if err := conn.Close(); err != nil {
			errors = append(errors, err)
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("接続終了エラー: %v", errors)
	}

	return nil
}

// CircuitBreaker サーキットブレーカーパターンを実装します
type CircuitBreaker struct {
	threshold    int
	timeout      time.Duration
	failures     int64
	lastFailTime time.Time
	state        CircuitBreakerState
	mutex        sync.RWMutex
}

// CircuitBreakerState サーキットブレーカーの状態を表します
type CircuitBreakerState int

const (
	CircuitBreakerClosed CircuitBreakerState = iota
	CircuitBreakerOpen
	CircuitBreakerHalfOpen
)

// NewCircuitBreaker 新しいサーキットブレーカーを作成します
func NewCircuitBreaker(threshold int, timeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		threshold: threshold,
		timeout:   timeout,
		state:     CircuitBreakerClosed,
	}
}

// CanExecute 実行可能かどうかを判定します
func (cb *CircuitBreaker) CanExecute() bool {
	cb.mutex.RLock()
	defer cb.mutex.RUnlock()

	switch cb.state {
	case CircuitBreakerClosed:
		return true
	case CircuitBreakerOpen:
		if time.Since(cb.lastFailTime) > cb.timeout {
			cb.setState(CircuitBreakerHalfOpen)
			return true
		}
		return false
	case CircuitBreakerHalfOpen:
		return true
	default:
		return false
	}
}

// OnSuccess 成功時の処理を行います
func (cb *CircuitBreaker) OnSuccess() {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	cb.failures = 0
	cb.setState(CircuitBreakerClosed)
}

// OnFailure 失敗時の処理を行います
func (cb *CircuitBreaker) OnFailure() {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	cb.failures++
	cb.lastFailTime = time.Now()

	if cb.failures >= int64(cb.threshold) {
		cb.setState(CircuitBreakerOpen)
	}
}

func (cb *CircuitBreaker) setState(state CircuitBreakerState) {
	cb.state = state
}

// GetState 現在の状態を取得します
func (cb *CircuitBreaker) GetState() CircuitBreakerState {
	cb.mutex.RLock()
	defer cb.mutex.RUnlock()
	return cb.state
}

// RetryPolicy 再試行ポリシーを定義します
type RetryPolicy struct {
	maxRetries     int
	initialBackoff time.Duration
	maxBackoff     time.Duration
	multiplier     float64
}

// HighPerformanceGRPCClient 高性能gRPCクライアントを実装します
type HighPerformanceGRPCClient struct {
	pool           *ConnectionPool
	config         ClientConfig
	metrics        *ClientMetrics
	circuitBreaker *CircuitBreaker
	retryPolicy    *RetryPolicy
}

// NewHighPerformanceGRPCClient 新しい高性能gRPCクライアントを作成します
func NewHighPerformanceGRPCClient(config ClientConfig) (*HighPerformanceGRPCClient, error) {
	pool, err := NewConnectionPool(config)
	if err != nil {
		return nil, err
	}

	client := &HighPerformanceGRPCClient{
		pool:           pool,
		config:         config,
		metrics:        &ClientMetrics{},
		circuitBreaker: NewCircuitBreaker(config.CircuitBreakerThreshold, config.CircuitBreakerTimeout),
		retryPolicy: &RetryPolicy{
			maxRetries:     config.MaxRetries,
			initialBackoff: config.InitialBackoff,
			maxBackoff:     config.MaxBackoff,
			multiplier:     config.BackoffMultiplier,
		},
	}

	return client, nil
}

// executeWithRetry 再試行機能付きで操作を実行します
func (c *HighPerformanceGRPCClient) executeWithRetry(ctx context.Context, operation func() error) error {
	if !c.circuitBreaker.CanExecute() {
		atomic.AddInt64(&c.metrics.CircuitBreakerTrips, 1)
		return fmt.Errorf("サーキットブレーカーが開いています")
	}

	var lastError error
	backoff := c.retryPolicy.initialBackoff

	for attempt := 0; attempt <= c.retryPolicy.maxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-time.After(backoff):
				backoff = time.Duration(float64(backoff) * c.retryPolicy.multiplier)
				if backoff > c.retryPolicy.maxBackoff {
					backoff = c.retryPolicy.maxBackoff
				}
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		err := operation()
		if err == nil {
			c.circuitBreaker.OnSuccess()
			return nil
		}

		lastError = err

		// 再試行不可能なエラーの場合は即座に失敗
		if !c.isRetryableError(err) {
			break
		}
	}

	c.circuitBreaker.OnFailure()
	return lastError
}

func (c *HighPerformanceGRPCClient) isRetryableError(err error) bool {
	// gRPCステータスコードに基づいて再試行可能性を判定
	errorStr := err.Error()
	retryableKeywords := []string{
		"unavailable",
		"timeout",
		"deadline exceeded",
		"connection refused",
	}

	for _, keyword := range retryableKeywords {
		if strings.Contains(strings.ToLower(errorStr), keyword) {
			return true
		}
	}

	return false
}

// GetUser Unary RPC呼び出しを行います
func (c *HighPerformanceGRPCClient) GetUser(ctx context.Context, userID int64) (*pb.User, error) {
	start := time.Now()
	atomic.AddInt64(&c.metrics.TotalRequests, 1)

	var result *pb.User
	var resultErr error

	operation := func() error {
		client := c.pool.GetClient()
		if client == nil {
			return fmt.Errorf("利用可能なクライアントがありません")
		}

		ctxWithTimeout, cancel := context.WithTimeout(ctx, c.config.DefaultTimeout)
		defer cancel()

		// JWT認証ヘッダーを追加
		if c.config.AuthToken != "" {
			md := metadata.Pairs("authorization", "Bearer "+c.config.AuthToken)
			ctxWithTimeout = metadata.NewOutgoingContext(ctxWithTimeout, md)
		}

		response, err := client.GetUser(ctxWithTimeout, &pb.GetUserRequest{Id: userID})
		if err != nil {
			return err
		}

		result = response.User
		return nil
	}

	if err := c.executeWithRetry(ctx, operation); err != nil {
		atomic.AddInt64(&c.metrics.FailedRequests, 1)
		resultErr = err
	} else {
		atomic.AddInt64(&c.metrics.SuccessfulRequests, 1)
	}

	c.updateLatencyMetrics(start)

	return result, resultErr
}

// CreateUser Unary RPC呼び出しを行います
func (c *HighPerformanceGRPCClient) CreateUser(ctx context.Context, name, email string) (*pb.User, error) {
	start := time.Now()
	atomic.AddInt64(&c.metrics.TotalRequests, 1)

	var result *pb.User
	var resultErr error

	operation := func() error {
		client := c.pool.GetClient()
		if client == nil {
			return fmt.Errorf("利用可能なクライアントがありません")
		}

		ctxWithTimeout, cancel := context.WithTimeout(ctx, c.config.DefaultTimeout)
		defer cancel()

		response, err := client.CreateUser(ctxWithTimeout, &pb.CreateUserRequest{
			Name:  name,
			Email: email,
		})
		if err != nil {
			return err
		}

		result = response.User
		return nil
	}

	if err := c.executeWithRetry(ctx, operation); err != nil {
		atomic.AddInt64(&c.metrics.FailedRequests, 1)
		resultErr = err
	} else {
		atomic.AddInt64(&c.metrics.SuccessfulRequests, 1)
	}

	c.updateLatencyMetrics(start)

	return result, resultErr
}

// ListUsersStream Server Streaming RPC呼び出しを行います
func (c *HighPerformanceGRPCClient) ListUsersStream(ctx context.Context, pageSize int32) (<-chan *pb.User, <-chan error) {
	userChan := make(chan *pb.User, 100)
	errorChan := make(chan error, 1)

	go func() {
		defer close(userChan)
		defer close(errorChan)

		operation := func() error {
			client := c.pool.GetClient()
			if client == nil {
				return fmt.Errorf("利用可能なクライアントがありません")
			}

			stream, err := client.ListUsers(ctx, &pb.ListUsersRequest{PageSize: pageSize})
			if err != nil {
				return err
			}

			for {
				user, err := stream.Recv()
				if err != nil {
					if err.Error() == "EOF" {
						return nil
					}
					return err
				}

				select {
				case userChan <- user:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}

		if err := c.executeWithRetry(ctx, operation); err != nil {
			errorChan <- err
		}
	}()

	return userChan, errorChan
}

// BulkCreateUsersStream Client Streaming RPC呼び出しを行います
func (c *HighPerformanceGRPCClient) BulkCreateUsersStream(ctx context.Context, users []*pb.CreateUserRequest) (*pb.BulkCreateUsersResponse, error) {
	var result *pb.BulkCreateUsersResponse
	var resultErr error

	operation := func() error {
		client := c.pool.GetClient()
		if client == nil {
			return fmt.Errorf("利用可能なクライアントがありません")
		}

		stream, err := client.BulkCreateUsers(ctx)
		if err != nil {
			return err
		}

		// ユーザーデータを送信
		for _, user := range users {
			if err := stream.Send(user); err != nil {
				return err
			}
		}

		response, err := stream.CloseAndRecv()
		if err != nil {
			return err
		}

		result = response
		return nil
	}

	if err := c.executeWithRetry(ctx, operation); err != nil {
		resultErr = err
	}

	return result, resultErr
}

// UserChatStream Bidirectional Streaming RPC呼び出しを行います
func (c *HighPerformanceGRPCClient) UserChatStream(ctx context.Context) (pb.UserService_UserChatClient, error) {
	client := c.pool.GetClient()
	if client == nil {
		return nil, fmt.Errorf("利用可能なクライアントがありません")
	}

	return client.UserChat(ctx)
}

func (c *HighPerformanceGRPCClient) updateLatencyMetrics(start time.Time) {
	elapsed := time.Since(start).Nanoseconds()

	// 平均レイテンシー更新（簡易移動平均）
	currentAvg := atomic.LoadInt64(&c.metrics.AverageLatency)
	newAvg := (currentAvg + elapsed) / 2
	atomic.StoreInt64(&c.metrics.AverageLatency, newAvg)

	// 最大レイテンシー更新
	for {
		currentMax := atomic.LoadInt64(&c.metrics.MaxLatency)
		if elapsed <= currentMax {
			break
		}
		if atomic.CompareAndSwapInt64(&c.metrics.MaxLatency, currentMax, elapsed) {
			break
		}
	}
}

// GetMetrics メトリクスを取得します
func (c *HighPerformanceGRPCClient) GetMetrics() ClientMetrics {
	return ClientMetrics{
		TotalRequests:       atomic.LoadInt64(&c.metrics.TotalRequests),
		SuccessfulRequests:  atomic.LoadInt64(&c.metrics.SuccessfulRequests),
		FailedRequests:      atomic.LoadInt64(&c.metrics.FailedRequests),
		AverageLatency:      atomic.LoadInt64(&c.metrics.AverageLatency),
		MaxLatency:          atomic.LoadInt64(&c.metrics.MaxLatency),
		ActiveConnections:   int64(len(c.pool.connections)),
		CircuitBreakerTrips: atomic.LoadInt64(&c.metrics.CircuitBreakerTrips),
	}
}

// Close クライアントを終了します
func (c *HighPerformanceGRPCClient) Close() error {
	return c.pool.Close()
}
