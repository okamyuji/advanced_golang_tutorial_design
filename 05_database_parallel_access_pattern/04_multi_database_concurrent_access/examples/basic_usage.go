package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"multi-db/internal/config"
	"multi-db/internal/manager"
	"multi-db/internal/models"
)

// ConcurrentAccessExample 並行アクセスの使用例を示すデモ
func main() {
	fmt.Println("🚀 マルチデータベース並行アクセス使用例")
	fmt.Println("=====================================")

	// ワーカープール設定
	poolConfig := manager.WorkerPoolConfig{
		MaxWorkers:      runtime.GOMAXPROCS(0) * 2,
		QueueSize:       100,
		TimeoutDuration: 30 * time.Second,
	}

	// DBマネージャー初期化
	dbManager := manager.NewConcurrentDBManager(poolConfig)
	defer func() {
		if err := dbManager.Close(); err != nil {
			log.Printf("DBマネージャー終了エラー: %v", err)
		}
	}()

	// データベース設定（実際の環境に合わせて調整）
	postgresConfig := config.DatabaseConfig{
		Host:            "localhost",
		Port:            5432,
		Username:        "postgres",
		Password:        "postgres",
		Database:        "testdb",
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: 1 * time.Hour,
		ConnMaxIdleTime: 30 * time.Minute,
		RetryAttempts:   3,
		RetryDelay:      100 * time.Millisecond,
	}

	mysqlConfig := config.DatabaseConfig{
		Host:            "localhost",
		Port:            3306,
		Username:        "testuser",
		Password:        "testpass",
		Database:        "testdb",
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: 45 * time.Minute,
		ConnMaxIdleTime: 15 * time.Minute,
		RetryAttempts:   3,
		RetryDelay:      100 * time.Millisecond,
	}

	// データベース登録
	if err := dbManager.RegisterDatabase("PostgreSQL", postgresConfig); err != nil {
		log.Printf("PostgreSQL接続エラー: %v", err)
	} else {
		fmt.Println("✅ PostgreSQL接続成功")
	}

	if err := dbManager.RegisterDatabase("MySQL", mysqlConfig); err != nil {
		log.Printf("MySQL接続エラー: %v", err)
	} else {
		fmt.Println("✅ MySQL接続成功")
	}

	// 使用例の実行
	runBasicQueryExample(dbManager)
	runParallelQueryExample(dbManager)
	runBulkOperationExample(dbManager)
	runTransactionExample(dbManager)
	runPerformanceExample(dbManager)

	// 最終メトリクス表示
	showFinalMetrics(dbManager)
}

// 基本的なクエリ実行例
func runBasicQueryExample(dbManager *manager.ConcurrentDBManager) {
	fmt.Println("\n📝 基本的なクエリ実行例")
	fmt.Println("=====================")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// PostgreSQLでユーザー検索
	resultChan := dbManager.ExecuteQuery(ctx, "PostgreSQL",
		"SELECT id, name, email FROM users WHERE age BETWEEN $1 AND $2 LIMIT 5",
		25, 35)

	result := <-resultChan
	if result.Error != nil {
		fmt.Printf("❌ PostgreSQLクエリエラー: %v\n", result.Error)
	} else {
		fmt.Printf("✅ PostgreSQL結果: %d件取得 (レスポンス時間: %v)\n",
			len(result.Data), result.Elapsed)

		// 結果の一部を表示（最初の3件のみ）
		for _, row := range result.Data[:min(3, len(result.Data))] {
			fmt.Printf("   ID: %v, Name: %v, Email: %v\n",
				row["id"], row["name"], row["email"])
		}
	}
}

// 並列クエリ実行例
func runParallelQueryExample(dbManager *manager.ConcurrentDBManager) {
	fmt.Println("\n🔄 並列クエリ実行例")
	fmt.Println("=================")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 複数データベースで同じクエリを並列実行
	queries := map[string]string{
		"PostgreSQL": "SELECT COUNT(*) as user_count FROM users",
		"MySQL":      "SELECT COUNT(*) as user_count FROM users",
	}

	start := time.Now()
	results := dbManager.ExecuteParallelQueries(ctx, queries)
	elapsed := time.Since(start)

	fmt.Printf("並列実行時間: %v\n", elapsed)

	for dbType, result := range results {
		if result.Error != nil {
			fmt.Printf("❌ %s エラー: %v\n", dbType, result.Error)
		} else {
			fmt.Printf("✅ %s: %v (レスポンス時間: %v)\n",
				dbType, result.Data[0], result.Elapsed)
		}
	}
}

// バルク操作例
func runBulkOperationExample(dbManager *manager.ConcurrentDBManager) {
	fmt.Println("\n📦 バルク操作例")
	fmt.Println("=============")

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// テストユーザーデータ作成
	users := make([]*models.User, 100)
	for i := range 100 {
		users[i] = &models.User{
			Name:  fmt.Sprintf("BulkUser%d", i),
			Email: fmt.Sprintf("bulk%d@example.com", i),
			Age:   25 + (i % 30),
		}
	}

	// PostgreSQLでバルク挿入
	start := time.Now()
	resultChan := dbManager.ExecuteBulkOperation(ctx, "PostgreSQL", users)
	result := <-resultChan
	elapsed := time.Since(start)

	if result.Error != nil {
		fmt.Printf("❌ バルク挿入エラー: %v\n", result.Error)
	} else {
		fmt.Printf("✅ バルク挿入成功: %d件 (実行時間: %v, スループット: %.2f件/秒)\n",
			len(users), elapsed, float64(len(users))/elapsed.Seconds())
	}
}

// トランザクション例
func runTransactionExample(dbManager *manager.ConcurrentDBManager) {
	fmt.Println("\n💳 トランザクション例")
	fmt.Println("===================")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// トランザクション実行
	resultChan := dbManager.ExecuteTransaction(ctx, "PostgreSQL", func(tx *sql.Tx) error {
		// 2件のINSERTを同じトランザクションで行う。どちらかが失敗すれば両方とも取り消される
		users := []*models.User{
			{Name: "TransactionUser1", Email: "tx1@example.com", Age: 28},
			{Name: "TransactionUser2", Email: "tx2@example.com", Age: 32},
		}
		for i, u := range users {
			if _, err := tx.ExecContext(ctx, "INSERT INTO users (name, email, age) VALUES ($1, $2, $3)", u.Name, u.Email, u.Age); err != nil {
				return fmt.Errorf("ユーザー%d作成エラー: %w", i+1, err)
			}
		}
		return nil
	})

	result := <-resultChan
	if result.Error != nil {
		fmt.Printf("❌ トランザクションエラー: %v\n", result.Error)
	} else {
		fmt.Printf("✅ トランザクション成功 (実行時間: %v)\n", result.Elapsed)
	}
}

// パフォーマンステスト例
func runPerformanceExample(dbManager *manager.ConcurrentDBManager) {
	fmt.Println("\n⚡ パフォーマンステスト例")
	fmt.Println("=======================")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	concurrentClients := 10
	requestsPerClient := 20

	fmt.Printf("同時実行数: %d, クライアント毎リクエスト数: %d\n",
		concurrentClients, requestsPerClient)

	var wg sync.WaitGroup
	var totalRequestsCounter, successfulRequestsCounter, totalLatencyCounter atomic.Int64

	start := time.Now()

	// 並行クライアント実行
	for range concurrentClients {
		wg.Go(func() {
			for j := range requestsPerClient {
				requestStart := time.Now()

				// PostgreSQLクエリ実行
				resultChan := dbManager.ExecuteQuery(ctx, "PostgreSQL",
					"SELECT COUNT(*) FROM users WHERE age > $1", 20+j)

				result := <-resultChan
				requestDuration := time.Since(requestStart)

				totalRequestsCounter.Add(1)
				totalLatencyCounter.Add(requestDuration.Nanoseconds())

				if result.Error == nil {
					successfulRequestsCounter.Add(1)
				}

				// 短い間隔でリクエスト
				time.Sleep(10 * time.Millisecond)
			}
		})
	}

	wg.Wait()
	elapsed := time.Since(start)
	totalRequests := totalRequestsCounter.Load()
	successfulRequests := successfulRequestsCounter.Load()

	// 結果計算
	avgLatency := time.Duration(totalLatencyCounter.Load() / totalRequests)
	successRate := float64(successfulRequests) / float64(totalRequests) * 100
	throughput := float64(totalRequests) / elapsed.Seconds()

	fmt.Printf("📊 パフォーマンス結果:\n")
	fmt.Printf("  総リクエスト数: %d\n", totalRequests)
	fmt.Printf("  成功リクエスト数: %d\n", successfulRequests)
	fmt.Printf("  成功率: %.2f%%\n", successRate)
	fmt.Printf("  平均レスポンス時間: %v\n", avgLatency)
	fmt.Printf("  スループット: %.2f RPS\n", throughput)
	fmt.Printf("  総実行時間: %v\n", elapsed)
}

// 最終メトリクス表示
func showFinalMetrics(dbManager *manager.ConcurrentDBManager) {
	fmt.Println("\n📈 最終メトリクス")
	fmt.Println("===============")

	// アプリケーションメトリクス
	metrics := dbManager.GetMetrics()
	fmt.Printf("📋 アプリケーション統計:\n")
	fmt.Printf("  総タスク数: %d\n", metrics.TotalTasks)
	fmt.Printf("  成功タスク数: %d\n", metrics.SuccessTasks)
	fmt.Printf("  失敗タスク数: %d\n", metrics.FailedTasks)
	fmt.Printf("  平均レイテンシー: %v\n", metrics.AverageLatency)
	fmt.Printf("  最大レイテンシー: %v\n", metrics.MaxLatency)
	fmt.Printf("  最小レイテンシー: %v\n", metrics.MinLatency)
	fmt.Printf("  アクティブタスク数: %d\n", metrics.ActiveTasks)

	// データベース統計
	dbStats := dbManager.GetDatabaseStats()
	fmt.Printf("\n🗄️ データベース接続統計:\n")
	for dbType, stats := range dbStats {
		fmt.Printf("  %s:\n", dbType)
		fmt.Printf("    最大接続数: %d\n", stats.MaxOpenConnections)
		fmt.Printf("    開いている接続数: %d\n", stats.OpenConnections)
		fmt.Printf("    使用中接続数: %d\n", stats.InUse)
		fmt.Printf("    アイドル接続数: %d\n", stats.Idle)
		fmt.Printf("    待機回数: %d\n", stats.WaitCount)
		fmt.Printf("    待機時間: %v\n", stats.WaitDuration)
	}

	// ヘルスチェック
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	healthResults := dbManager.HealthCheck(ctx)
	fmt.Printf("\n💚 ヘルスチェック結果:\n")
	for dbType, err := range healthResults {
		if err != nil {
			fmt.Printf("  %s: ❌ %v\n", dbType, err)
		} else {
			fmt.Printf("  %s: ✅ 正常\n", dbType)
		}
	}

	fmt.Println("\n🎉 使用例実行完了!")
	fmt.Println("\n次のステップ:")
	fmt.Println("  1. 本格的な負荷テスト: make load-test")
	fmt.Println("  2. パフォーマンステスト: make performance-test")
	fmt.Println("  3. 監視ダッシュボード: http://localhost:3000")
	fmt.Println("  4. メトリクス確認: http://localhost:8081/metrics")
}
