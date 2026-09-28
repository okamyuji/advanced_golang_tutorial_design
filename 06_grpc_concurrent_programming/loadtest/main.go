package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"grpc-concurrent-programming/client"
	"grpc-concurrent-programming/security"
	"grpc-concurrent-programming/server"
)

// LoadTestConfig 負荷テストの設定を定義します
type LoadTestConfig struct {
	ServerAddress     string        `json:"server_address"`
	ConcurrentClients int           `json:"concurrent_clients"`
	RequestsPerClient int           `json:"requests_per_client"`
	TestDuration      time.Duration `json:"test_duration"`
	RampUpDuration    time.Duration `json:"ramp_up_duration"`
}

// LoadTestResult 負荷テストの結果を格納します
type LoadTestResult struct {
	TotalRequests      int64         `json:"total_requests"`
	SuccessfulRequests int64         `json:"successful_requests"`
	FailedRequests     int64         `json:"failed_requests"`
	AverageLatency     time.Duration `json:"average_latency"`
	MaxLatency         time.Duration `json:"max_latency"`
	MinLatency         time.Duration `json:"min_latency"`
	RequestsPerSecond  float64       `json:"requests_per_second"`
	TestDuration       time.Duration `json:"test_duration"`
	P95Latency         time.Duration `json:"p95_latency"`
	P99Latency         time.Duration `json:"p99_latency"`
}

func main() {
	// サーバー設定と起動
	serverConfig := server.ServerConfig{
		Port:                  8080,
		MaxWorkers:            runtime.GOMAXPROCS(0) * 16, // 負荷テスト用に多めに設定
		MaxConcurrentStreams:  2000,                       // 高負荷対応
		MaxReceiveMessageSize: 4 * 1024 * 1024,            // 4MB
		MaxSendMessageSize:    4 * 1024 * 1024,            // 4MB
		ConnectionTimeout:     30 * time.Second,
		KeepaliveTime:         30 * time.Second,
		KeepaliveTimeout:      5 * time.Second,
		MaxConnectionIdle:     15 * time.Minute,
		MaxConnectionAge:      30 * time.Minute,
		MaxConnectionAgeGrace: 5 * time.Minute,
		EnableTLS:             false, // 負荷テスト用にTLS無効
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

	// 複数の負荷テストシナリオを実行
	runLoadTestSuite()
}

func runLoadTestSuite() {
	fmt.Println("=== gRPC高負荷テストスイート開始 ===")

	// シナリオ1: 低負荷テスト
	fmt.Println("\n--- シナリオ1: 低負荷テスト ---")
	runLoadTestScenario("低負荷", LoadTestConfig{
		ServerAddress:     "localhost:8080",
		ConcurrentClients: 10,
		RequestsPerClient: 100,
		TestDuration:      30 * time.Second,
		RampUpDuration:    5 * time.Second,
	})

	// シナリオ2: 中負荷テスト
	fmt.Println("\n--- シナリオ2: 中負荷テスト ---")
	runLoadTestScenario("中負荷", LoadTestConfig{
		ServerAddress:     "localhost:8080",
		ConcurrentClients: 50,
		RequestsPerClient: 200,
		TestDuration:      60 * time.Second,
		RampUpDuration:    10 * time.Second,
	})

	// シナリオ3: 高負荷テスト
	fmt.Println("\n--- シナリオ3: 高負荷テスト ---")
	runLoadTestScenario("高負荷", LoadTestConfig{
		ServerAddress:     "localhost:8080",
		ConcurrentClients: 100,
		RequestsPerClient: 500,
		TestDuration:      120 * time.Second,
		RampUpDuration:    15 * time.Second,
	})

	// シナリオ4: 極限負荷テスト
	fmt.Println("\n--- シナリオ4: 極限負荷テスト ---")
	runLoadTestScenario("極限負荷", LoadTestConfig{
		ServerAddress:     "localhost:8080",
		ConcurrentClients: 200,
		RequestsPerClient: 1000,
		TestDuration:      180 * time.Second,
		RampUpDuration:    20 * time.Second,
	})

	fmt.Println("\n=== gRPC高負荷テストスイート完了 ===")
}

func runLoadTestScenario(scenarioName string, config LoadTestConfig) {
	fmt.Printf("%s負荷テスト開始...\n", scenarioName)
	fmt.Printf("設定: 同時クライアント数=%d, クライアント毎リクエスト数=%d, テスト時間=%v\n",
		config.ConcurrentClients, config.RequestsPerClient, config.TestDuration)

	result := executeLoadTest(config)
	printLoadTestResult(scenarioName, result)

	// テスト間のクールダウン
	fmt.Println("クールダウン中...")
	time.Sleep(10 * time.Second)
}

func executeLoadTest(config LoadTestConfig) LoadTestResult {
	// JWT認証トークンを生成（負荷テスト用）
	authToken, err := security.GenerateJWT("loadtest-user", "admin")
	if err != nil {
		log.Fatalf("JWTトークン生成エラー: %v", err)
	}

	// クライアント設定
	clientConfig := client.ClientConfig{
		ServerAddresses:         []string{config.ServerAddress},
		MaxConnections:          20, // 負荷テスト用に多めの接続
		MaxRetries:              2,  // 負荷テスト中は再試行を少なめに
		InitialBackoff:          50 * time.Millisecond,
		MaxBackoff:              2 * time.Second,
		BackoffMultiplier:       1.5,
		KeepaliveTime:           30 * time.Second,
		KeepaliveTimeout:        5 * time.Second,
		PermitWithoutStream:     true,
		MaxReceiveMessageSize:   4 * 1024 * 1024,
		MaxSendMessageSize:      4 * 1024 * 1024,
		DefaultTimeout:          10 * time.Second,
		CircuitBreakerThreshold: 10, // 負荷テスト用に閾値を上げる
		CircuitBreakerTimeout:   30 * time.Second,
		EnableTLS:               false,     // 負荷テスト用にTLS無効
		AuthToken:               authToken, // JWT認証トークン
	}

	var totalRequests atomic.Int64
	var successfulRequests atomic.Int64
	var failedRequests atomic.Int64
	var totalLatency atomic.Int64

	latencies := make([]time.Duration, 0, config.ConcurrentClients*config.RequestsPerClient)
	latenciesMutex := sync.Mutex{}

	startTime := time.Now()
	var wg sync.WaitGroup

	// リアルタイム統計表示用ゴルーチン
	stopStats := make(chan struct{})
	go func() {
		tick := time.Tick(5 * time.Second)

		for {
			select {
			case <-tick:
				current := totalRequests.Load()
				successful := successfulRequests.Load()
				failed := failedRequests.Load()
				elapsed := time.Since(startTime)
				currentRPS := float64(current) / elapsed.Seconds()

				fmt.Printf("[%v] 進捗: リクエスト=%d, 成功=%d, 失敗=%d, RPS=%.1f\n",
					elapsed.Round(time.Second), current, successful, failed, currentRPS)

			case <-stopStats:
				return
			}
		}
	}()

	// 段階的にクライアントを起動（ランプアップ）
	var clientInterval time.Duration
	if config.ConcurrentClients > 0 {
		clientInterval = config.RampUpDuration / time.Duration(config.ConcurrentClients)
	}

	for clientID := range config.ConcurrentClients {
		wg.Go(func() {
			// クライアント作成
			grpcClient, err := client.NewHighPerformanceGRPCClient(clientConfig)
			if err != nil {
				log.Printf("クライアント作成エラー (ID: %d): %v", clientID, err)
				return
			}
			defer func() {
				if err := grpcClient.Close(); err != nil {
					log.Printf("クライアント終了エラー (ID: %d): %v", clientID, err)
				}
			}()

			// テスト期間中リクエストを継続送信
			endTime := startTime.Add(config.TestDuration)
			requestCount := 0

			for time.Now().Before(endTime) && requestCount < config.RequestsPerClient {
				requestStart := time.Now()

				// GetUserリクエスト実行
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				userID := int64((requestCount % 1000) + 1) // サンプルデータの範囲内
				_, err := grpcClient.GetUser(ctx, userID)
				cancel()

				requestDuration := time.Since(requestStart)

				totalRequests.Add(1)

				if err != nil {
					failedRequests.Add(1)
				} else {
					successfulRequests.Add(1)
				}

				// レイテンシー統計更新
				latencyNs := requestDuration.Nanoseconds()
				totalLatency.Add(latencyNs)

				// パーセンタイル計算用にレイテンシーを保存
				latenciesMutex.Lock()
				latencies = append(latencies, requestDuration)
				latenciesMutex.Unlock()

				requestCount++

				// 負荷調整（短い間隔でリクエスト）
				time.Sleep(100 * time.Microsecond)
			}
		})

		// ランプアップ間隔
		if clientID < config.ConcurrentClients-1 {
			time.Sleep(clientInterval)
		}
	}

	wg.Wait()
	close(stopStats)
	actualTestDuration := time.Since(startTime)

	// 統計計算
	totalReq := totalRequests.Load()
	successReq := successfulRequests.Load()
	failedReq := failedRequests.Load()
	var avgLatency time.Duration
	if totalReq > 0 {
		avgLatency = time.Duration(totalLatency.Load() / totalReq)
	}

	// パーセンタイル計算
	slices.Sort(latencies)

	var minLatency, maxLatency, p95Latency, p99Latency time.Duration
	if len(latencies) > 0 {
		minLatency = latencies[0]
		maxLatency = latencies[len(latencies)-1]

		p95Latency = latencies[min(int(float64(len(latencies))*0.95), len(latencies)-1)]
		p99Latency = latencies[min(int(float64(len(latencies))*0.99), len(latencies)-1)]
	}

	return LoadTestResult{
		TotalRequests:      totalReq,
		SuccessfulRequests: successReq,
		FailedRequests:     failedReq,
		AverageLatency:     avgLatency,
		MaxLatency:         maxLatency,
		MinLatency:         minLatency,
		RequestsPerSecond:  float64(totalReq) / actualTestDuration.Seconds(),
		TestDuration:       actualTestDuration,
		P95Latency:         p95Latency,
		P99Latency:         p99Latency,
	}
}

func printLoadTestResult(scenarioName string, result LoadTestResult) {
	fmt.Printf("\n=== %s負荷テスト結果 ===\n", scenarioName)
	fmt.Printf("実行時間: %v\n", result.TestDuration)
	fmt.Printf("総リクエスト数: %d\n", result.TotalRequests)
	fmt.Printf("成功リクエスト数: %d\n", result.SuccessfulRequests)
	fmt.Printf("失敗リクエスト数: %d\n", result.FailedRequests)

	var successRate float64
	if result.TotalRequests > 0 {
		successRate = float64(result.SuccessfulRequests) / float64(result.TotalRequests) * 100
	}
	fmt.Printf("成功率: %.2f%%\n", successRate)
	fmt.Printf("スループット: %.2f RPS\n", result.RequestsPerSecond)

	fmt.Printf("レイテンシー統計:\n")
	fmt.Printf("  平均: %v\n", result.AverageLatency)
	fmt.Printf("  最小: %v\n", result.MinLatency)
	fmt.Printf("  最大: %v\n", result.MaxLatency)
	fmt.Printf("  P95: %v\n", result.P95Latency)
	fmt.Printf("  P99: %v\n", result.P99Latency)

	// パフォーマンス評価
	fmt.Printf("パフォーマンス評価:\n")
	if result.RequestsPerSecond >= 1000 {
		fmt.Printf("  スループット: 優秀 (%.0f RPS)\n", result.RequestsPerSecond)
	} else if result.RequestsPerSecond >= 500 {
		fmt.Printf("  スループット: 良好 (%.0f RPS)\n", result.RequestsPerSecond)
	} else {
		fmt.Printf("  スループット: 要改善 (%.0f RPS)\n", result.RequestsPerSecond)
	}

	if result.P99Latency <= 50*time.Millisecond {
		fmt.Printf("  レイテンシー: 優秀 (P99: %v)\n", result.P99Latency)
	} else if result.P99Latency <= 100*time.Millisecond {
		fmt.Printf("  レイテンシー: 良好 (P99: %v)\n", result.P99Latency)
	} else {
		fmt.Printf("  レイテンシー: 要改善 (P99: %v)\n", result.P99Latency)
	}

	if successRate >= 99.9 {
		fmt.Printf("  可用性: 優秀 (%.2f%%)\n", successRate)
	} else if successRate >= 99.0 {
		fmt.Printf("  可用性: 良好 (%.2f%%)\n", successRate)
	} else {
		fmt.Printf("  可用性: 要改善 (%.2f%%)\n", successRate)
	}

	fmt.Println()
}
