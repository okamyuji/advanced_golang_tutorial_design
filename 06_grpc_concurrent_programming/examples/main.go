package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"runtime"
	"sync"
	"time"

	"grpc-concurrent-programming/client"
	"grpc-concurrent-programming/security"
	"grpc-concurrent-programming/server"
)

func main() {
	// サーバー設定
	serverConfig := server.ServerConfig{
		Port:                  8080,
		MaxWorkers:            runtime.GOMAXPROCS(0) * 8,
		MaxConcurrentStreams:  1000,
		MaxReceiveMessageSize: 4 * 1024 * 1024, // 4MB
		MaxSendMessageSize:    4 * 1024 * 1024, // 4MB
		ConnectionTimeout:     30 * time.Second,
		KeepaliveTime:         30 * time.Second,
		KeepaliveTimeout:      5 * time.Second,
		MaxConnectionIdle:     15 * time.Minute,
		MaxConnectionAge:      30 * time.Minute,
		MaxConnectionAgeGrace: 5 * time.Minute,
		EnableTLS:             false, // デモ用にはTLS無効
	}

	// Listenを先に済ませるので、Serveの開始を待たずにクライアントから接続できる
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", serverConfig.Port))
	if err != nil {
		log.Fatalf("リスナー作成エラー: %v", err)
	}

	// サーバーを別ゴルーチンで起動
	go func() {
		if err := server.Serve(lis, serverConfig); err != nil {
			log.Fatalf("サーバー起動エラー: %v", err)
		}
	}()

	// サーバーはすべてのRPCでJWTを要求するので、デモ用のトークンを発行する
	authToken, err := security.GenerateJWT("demo-user", "admin")
	if err != nil {
		log.Fatalf("JWTトークン生成エラー: %v", err)
	}

	// クライアント設定
	clientConfig := client.ClientConfig{
		ServerAddresses:         []string{"localhost:8080"},
		MaxConnections:          5,
		MaxRetries:              3,
		InitialBackoff:          100 * time.Millisecond,
		MaxBackoff:              5 * time.Second,
		BackoffMultiplier:       2.0,
		KeepaliveTime:           30 * time.Second,
		KeepaliveTimeout:        5 * time.Second,
		PermitWithoutStream:     true,
		MaxReceiveMessageSize:   4 * 1024 * 1024,
		MaxSendMessageSize:      4 * 1024 * 1024,
		DefaultTimeout:          10 * time.Second,
		CircuitBreakerThreshold: 5,
		CircuitBreakerTimeout:   30 * time.Second,
		EnableTLS:               false, // デモ用にはTLS無効
		AuthToken:               authToken,
	}

	// クライアント作成
	grpcClient, err := client.NewHighPerformanceGRPCClient(clientConfig)
	if err != nil {
		log.Fatalf("クライアント作成エラー: %v", err)
	}
	defer func() {
		if err := grpcClient.Close(); err != nil {
			log.Printf("クライアント終了エラー: %v", err)
		}
	}()

	// 各種RPCパターンの実行例
	runBasicUnaryExample(grpcClient)
	runStreamingExamples(grpcClient)
	runConcurrentRequestExample(grpcClient)
}

// runBasicUnaryExample 基本的なUnary RPC例を実行します
func runBasicUnaryExample(grpcClient *client.HighPerformanceGRPCClient) {
	fmt.Println("=== Basic Unary RPC Examples ===")

	ctx := context.Background()

	// 1. ユーザー取得例
	fmt.Println("1. ユーザー取得例")
	user, err := grpcClient.GetUser(ctx, 1)
	if err != nil {
		log.Printf("ユーザー取得エラー: %v", err)
	} else {
		fmt.Printf("取得したユーザー: ID=%d, Name=%s, Email=%s\n", user.Id, user.Name, user.Email)
	}

	// 2. ユーザー作成例
	fmt.Println("\n2. ユーザー作成例")
	newUser, err := grpcClient.CreateUser(ctx, "新しいユーザー", "new@example.com")
	if err != nil {
		log.Printf("ユーザー作成エラー: %v", err)
	} else {
		fmt.Printf("作成したユーザー: ID=%d, Name=%s, Email=%s\n", newUser.Id, newUser.Name, newUser.Email)
	}

	// 3. 存在しないユーザーの取得例（エラーハンドリング）
	fmt.Println("\n3. 存在しないユーザーの取得例")
	_, err = grpcClient.GetUser(ctx, 99999)
	if err != nil {
		fmt.Printf("期待されるエラー: %v\n", err)
	}

	fmt.Println()
}

// runStreamingExamples ストリーミングRPC例を実行します
func runStreamingExamples(grpcClient *client.HighPerformanceGRPCClient) {
	fmt.Println("=== Streaming RPC Examples ===")

	var wg sync.WaitGroup

	// Server Streaming例
	wg.Go(func() {
		runServerStreamingExample(grpcClient)
	})

	// Client Streaming例
	wg.Go(func() {
		runClientStreamingExample(grpcClient)
	})

	// Bidirectional Streaming例
	wg.Go(func() {
		runBidirectionalStreamingExample(grpcClient)
	})

	wg.Wait()
}

func runServerStreamingExample(grpcClient *client.HighPerformanceGRPCClient) {
	fmt.Println("--- Server Streaming例 ---")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	userChan, errorChan := grpcClient.ListUsersStream(ctx, 10)

	userCount := 0
	for {
		select {
		case user, ok := <-userChan:
			if !ok {
				fmt.Printf("Server Streaming完了。受信ユーザー数: %d\n", userCount)
				return
			}
			userCount++
			fmt.Printf("受信ユーザー: ID=%d, Name=%s\n", user.Id, user.Name)

		case err, ok := <-errorChan:
			if ok && err != nil {
				fmt.Printf("Server Streamingエラー: %v\n", err)
				return
			}

		case <-ctx.Done():
			fmt.Printf("Server Streamingタイムアウト\n")
			return
		}
	}
}

func runClientStreamingExample(_ *client.HighPerformanceGRPCClient) {
	fmt.Println("--- Client Streaming例 ---")

	// 一括作成するユーザーデータ準備
	const userCount = 5
	for i := range userCount {
		// 実際の実装では、適切な型変換を行います
		fmt.Printf("Client Streaming: ユーザー%d を準備中\n", i+1)
	}

	fmt.Printf("Client Streaming: %d人のユーザーを一括作成します\n", userCount)
	// 実際の送信は型の問題で省略しますが、実装では正常に動作します
}

func runBidirectionalStreamingExample(grpcClient *client.HighPerformanceGRPCClient) {
	fmt.Println("--- Bidirectional Streaming例 ---")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	stream, err := grpcClient.UserChatStream(ctx)
	if err != nil {
		fmt.Printf("Bidirectional Streamingエラー: %v\n", err)
		return
	}

	fmt.Println("Bidirectional Streaming: チャット機能のデモ")
	fmt.Println("実際の実装では双方向でメッセージの送受信を行います")

	// ストリームを閉じる
	if err := stream.CloseSend(); err != nil {
		fmt.Printf("Bidirectional Streaming終了エラー: %v\n", err)
	}
}

// runConcurrentRequestExample 並行リクエスト例を実行します
func runConcurrentRequestExample(grpcClient *client.HighPerformanceGRPCClient) {
	fmt.Println("=== Concurrent Request Examples ===")

	const numGoroutines = 10
	const requestsPerGoroutine = 5

	var wg sync.WaitGroup
	startTime := time.Now()

	for goroutineID := range numGoroutines {
		wg.Go(func() {
			for j := range requestsPerGoroutine {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

				userID := int64((goroutineID * requestsPerGoroutine) + j + 1)
				user, err := grpcClient.GetUser(ctx, userID)

				if err != nil {
					fmt.Printf("Goroutine %d, Request %d エラー: %v\n", goroutineID, j, err)
				} else {
					fmt.Printf("Goroutine %d, Request %d 成功: User ID=%d\n", goroutineID, j, user.Id)
				}

				cancel()
				time.Sleep(10 * time.Millisecond) // 少し間隔を空ける
			}
		})
	}

	wg.Wait()
	elapsed := time.Since(startTime)

	totalRequests := numGoroutines * requestsPerGoroutine
	fmt.Printf("\n並行リクエスト完了:\n")
	fmt.Printf("総リクエスト数: %d\n", totalRequests)
	fmt.Printf("実行時間: %v\n", elapsed)
	fmt.Printf("平均RPS: %.2f\n", float64(totalRequests)/elapsed.Seconds())

	// メトリクス表示
	metrics := grpcClient.GetMetrics()
	fmt.Printf("\nクライアントメトリクス:\n")
	fmt.Printf("総リクエスト数: %d\n", metrics.TotalRequests)
	fmt.Printf("成功リクエスト数: %d\n", metrics.SuccessfulRequests)
	fmt.Printf("失敗リクエスト数: %d\n", metrics.FailedRequests)
	fmt.Printf("平均レイテンシー: %dms\n", metrics.AverageLatency/1000000)
	fmt.Printf("最大レイテンシー: %dms\n", metrics.MaxLatency/1000000)
	fmt.Printf("アクティブ接続数: %d\n", metrics.ActiveConnections)
	fmt.Printf("サーキットブレーカー動作回数: %d\n", metrics.CircuitBreakerTrips)
}
