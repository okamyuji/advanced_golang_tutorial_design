package main

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"multi-db/internal/config"
	"multi-db/internal/manager"
	"multi-db/internal/models"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	dbManager *manager.ConcurrentDBManager
	appConfig *config.Config

	// Prometheus メトリクス
	requestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "http_request_duration_seconds",
			Help: "HTTP request duration in seconds",
		},
		[]string{"method", "endpoint", "status"},
	)

	dbOperationDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "db_operation_duration_seconds",
			Help: "Database operation duration in seconds",
		},
		[]string{"db_type", "operation"},
	)

	activeConnections = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "db_active_connections",
			Help: "Number of active database connections",
		},
		[]string{"db_type"},
	)
)

func init() {
	// Prometheusメトリクス登録
	prometheus.MustRegister(requestDuration)
	prometheus.MustRegister(dbOperationDuration)
	prometheus.MustRegister(activeConnections)
}

func main() {
	log.Println("マルチデータベース並行アクセスシステム起動中...")

	// 設定読み込み
	var err error
	appConfig, err = config.LoadConfig("configs/config.yaml")
	if err != nil {
		log.Fatalf("設定読み込みエラー: %v", err)
	}

	// DBマネージャー初期化
	poolConfig := manager.WorkerPoolConfig{
		MaxWorkers:      runtime.GOMAXPROCS(0) * 4,
		QueueSize:       1000,
		TimeoutDuration: 30 * time.Second,
	}

	dbManager = manager.NewConcurrentDBManager(poolConfig)
	defer func() {
		if err := dbManager.Close(); err != nil {
			log.Printf("DBマネージャー終了エラー: %v", err)
		}
	}()

	// データベース接続設定
	if err := setupDatabases(); err != nil {
		log.Fatalf("データベース設定エラー: %v", err)
	}

	// HTTPサーバー起動
	router := setupRoutes()

	server := &http.Server{
		Addr:         fmt.Sprintf(":%d", appConfig.Server.Port),
		Handler:      router,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// メトリクス更新ゴルーチン
	go startMetricsCollector()

	// サーバー起動
	go func() {
		log.Printf("HTTPサーバー起動: ポート %d", appConfig.Server.Port)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("HTTPサーバーエラー: %v", err)
		}
	}()

	// メトリクスサーバー起動
	go func() {
		metricsServer := &http.Server{
			Addr:    ":8081",
			Handler: promhttp.Handler(),
		}
		log.Println("Prometheusメトリクスサーバー起動: ポート 8081")
		if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("メトリクスサーバーエラー: %v", err)
		}
	}()

	// シグナルハンドリング
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("シャットダウン開始...")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Printf("サーバーシャットダウンエラー: %v", err)
	}

	log.Println("シャットダウン完了")
}

func setupDatabases() error {
	// PostgreSQL接続
	if appConfig.Databases.PostgreSQL.Enabled {
		if err := dbManager.RegisterDatabase("PostgreSQL", appConfig.Databases.PostgreSQL.DatabaseConfig); err != nil {
			log.Printf("PostgreSQL接続エラー: %v", err)
		}
	}

	// MySQL接続
	if appConfig.Databases.MySQL.Enabled {
		if err := dbManager.RegisterDatabase("MySQL", appConfig.Databases.MySQL.DatabaseConfig); err != nil {
			log.Printf("MySQL接続エラー: %v", err)
		}
	}

	// MSSQL接続
	if appConfig.Databases.MSSQL.Enabled {
		if err := dbManager.RegisterDatabase("MSSQL", appConfig.Databases.MSSQL.DatabaseConfig); err != nil {
			log.Printf("MSSQL接続エラー: %v", err)
		}
	}

	// Oracle接続
	if appConfig.Databases.Oracle.Enabled {
		if err := dbManager.RegisterDatabase("Oracle", appConfig.Databases.Oracle.DatabaseConfig); err != nil {
			log.Printf("Oracle接続エラー: %v", err)
		}
	}

	// 登録されたデータベース確認
	databases := dbManager.GetRegisteredDatabases()
	if len(databases) == 0 {
		return fmt.Errorf("利用可能なデータベースがありません")
	}

	log.Printf("登録済みデータベース: %v", databases)
	return nil
}

func setupRoutes() http.Handler {
	mux := http.NewServeMux()

	// API ルート（メソッド違いは ServeMux が 405 を返す）
	mux.HandleFunc("GET /api/v1/users/search", corsAndLogging(searchUsersHandler))
	mux.HandleFunc("POST /api/v1/users/bulk", corsAndLogging(bulkCreateUsersHandler))
	mux.HandleFunc("POST /api/v1/users/{$}", corsAndLogging(createUserHandler))
	mux.HandleFunc("GET /api/v1/users/{id}", corsAndLogging(getUserHandler))
	mux.HandleFunc("POST /api/v1/databases/query", corsAndLogging(executeQueryHandler))
	mux.HandleFunc("POST /api/v1/databases/parallel", corsAndLogging(executeParallelQueriesHandler))
	mux.HandleFunc("GET /api/v1/health", corsAndLogging(healthCheckHandler))
	mux.HandleFunc("GET /api/v1/metrics", corsAndLogging(getMetricsHandler))
	mux.HandleFunc("GET /api/v1/stats", corsAndLogging(getDatabaseStatsHandler))
	mux.HandleFunc("POST /api/v1/loadtest", corsAndLogging(loadTestHandler))
	// CORS のプリフライト。corsAndLogging が OPTIONS をヘッダーだけ付けて返す
	mux.HandleFunc("OPTIONS /api/v1/", corsAndLogging(http.NotFound))

	// 静的ファイル。GET に限定しないと、API パスへの想定外メソッドが 405 でなく 404 になる
	mux.Handle("GET /", http.FileServer(http.Dir("./static/")))

	return mux
}

// resolveDBType クエリパラメータ db_type、なければ登録済みの先頭を返す。
// 見つからないときはエラー応答を書いて false を返す
func resolveDBType(w http.ResponseWriter, r *http.Request) (string, bool) {
	if dbType := r.URL.Query().Get("db_type"); dbType != "" {
		return dbType, true
	}
	availableDBs := dbManager.GetRegisteredDatabases()
	if len(availableDBs) == 0 {
		http.Error(w, "利用可能なデータベースがありません", http.StatusInternalServerError)
		return "", false
	}
	return availableDBs[0], true
}

// placeholder データベースごとの n 番目（1始まり）のバインド変数表記を返す
func placeholder(dbType string, n int) string {
	switch dbType {
	// MSSQLアダプターは"mssql"ドライバーで開くので"?"を使う。"@pN"は"sqlserver"ドライバーの書き方で、ここでは通らない
	case "MySQL", "MSSQL":
		return "?"
	case "Oracle":
		return ":" + strconv.Itoa(n)
	default:
		return "$" + strconv.Itoa(n)
	}
}

// writeJSON JSON を書き出し、失敗したらログに残す（ヘッダー送信後なので応答は変えられない）
func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("JSONエンコードエラー: %v", err)
	}
}

// getUserHandler GET /api/v1/users/{id}
func getUserHandler(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		http.Error(w, "無効なユーザーID", http.StatusBadRequest)
		return
	}

	dbType, ok := resolveDBType(w, r)
	if !ok {
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	resultChan := dbManager.ExecuteQuery(ctx, dbType,
		"SELECT id, name, email, age, created_at, updated_at FROM users WHERE id = "+placeholder(dbType, 1), id)

	result := <-resultChan
	if result.Error != nil {
		http.Error(w, result.Error.Error(), http.StatusInternalServerError)
		return
	}

	if len(result.Data) == 0 {
		http.Error(w, "ユーザーが見つかりません", http.StatusNotFound)
		return
	}

	// データ変換（簡略化）
	userData := result.Data[0]
	user := &models.User{}

	if id, ok := userData["id"].(int64); ok {
		user.ID = id
	}
	if name, ok := userData["name"].(string); ok {
		user.Name = name
	}
	if email, ok := userData["email"].(string); ok {
		user.Email = email
	}
	if age, ok := userData["age"].(int64); ok {
		user.Age = int(age)
	}

	writeJSON(w, models.UserResponse{
		User:    user,
		Message: "ユーザーが見つかりました",
		Success: true,
	})
}

// CORSとロギングの組み合わせミドルウェア
func corsAndLogging(handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		// CORS設定
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		// レスポンスライターをラップして状態コードを取得
		lrw := &loggingResponseWriter{ResponseWriter: w, statusCode: http.StatusOK}

		handler(lrw, r)

		duration := time.Since(start).Seconds()

		// Prometheusメトリクス記録
		requestDuration.WithLabelValues(
			r.Method,
			r.URL.Path,
			strconv.Itoa(lrw.statusCode),
		).Observe(duration)

		log.Printf("%s %s %d %v", r.Method, r.URL.Path, lrw.statusCode, time.Since(start))
	}
}

type loggingResponseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (lrw *loggingResponseWriter) WriteHeader(code int) {
	lrw.statusCode = code
	lrw.ResponseWriter.WriteHeader(code)
}

// ハンドラー関数
func createUserHandler(w http.ResponseWriter, r *http.Request) {
	var req models.CreateUserRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "無効なリクエストボディ", http.StatusBadRequest)
		return
	}

	dbType, ok := resolveDBType(w, r)
	if !ok {
		return
	}

	user := req.ToUser()
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	// アダプター専用のCreateUserメソッドを使用
	resultChan := dbManager.CreateUser(ctx, dbType, user)
	result := <-resultChan
	if result.Error != nil {
		http.Error(w, result.Error.Error(), http.StatusInternalServerError)
		return
	}

	// 作成されたユーザー情報を取得
	if len(result.Data) > 0 {
		userData := result.Data[0]
		if id, ok := userData["id"].(int64); ok {
			user.ID = id
		}
		if createdAt, ok := userData["created_at"].(string); ok {
			if t, err := time.Parse("2006-01-02 15:04:05", createdAt); err == nil {
				user.CreatedAt = t
			}
		}
		if updatedAt, ok := userData["updated_at"].(string); ok {
			if t, err := time.Parse("2006-01-02 15:04:05", updatedAt); err == nil {
				user.UpdatedAt = t
			}
		}
	}

	writeJSON(w, models.UserResponse{
		User:    user,
		Message: "ユーザーが正常に作成されました",
		Success: true,
	})
}

func searchUsersHandler(w http.ResponseWriter, r *http.Request) {
	dbType, ok := resolveDBType(w, r)
	if !ok {
		return
	}

	query := r.URL.Query()
	minAge, _ := strconv.Atoi(query.Get("min_age"))
	maxAge, _ := strconv.Atoi(query.Get("max_age"))
	maxAge = cmp.Or(maxAge, 150)

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// アダプター固有のGetUsersByAgeRangeメソッドを使用
	resultChan := dbManager.GetUsersByAgeRange(ctx, dbType, minAge, maxAge)

	result := <-resultChan
	if result.Error != nil {
		http.Error(w, result.Error.Error(), http.StatusInternalServerError)
		return
	}

	users := result.Users
	if users == nil {
		users = []*models.User{} // 0件でもJSONをnullでなく[]にする
	}

	writeJSON(w, models.UsersResponse{
		Users:   users,
		Total:   int64(len(users)),
		Success: true,
		Message: fmt.Sprintf("%d件のユーザーが見つかりました", len(users)),
	})
}

func bulkCreateUsersHandler(w http.ResponseWriter, r *http.Request) {
	var users []*models.CreateUserRequest
	if err := json.NewDecoder(r.Body).Decode(&users); err != nil {
		http.Error(w, "無効なリクエストボディ", http.StatusBadRequest)
		return
	}

	dbType, ok := resolveDBType(w, r)
	if !ok {
		return
	}

	// リクエストをUserモデルに変換
	userModels := make([]*models.User, len(users))
	for i, req := range users {
		userModels[i] = req.ToUser()
	}

	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()

	start := time.Now()
	resultChan := dbManager.ExecuteBulkOperation(ctx, dbType, userModels)
	result := <-resultChan

	duration := time.Since(start).String()

	var bulkResult models.BulkOperationResult
	if result.Error != nil {
		bulkResult = models.BulkOperationResult{
			TotalProcessed: len(users),
			Successful:     0,
			Failed:         len(users),
			Errors:         []string{result.Error.Error()},
			Duration:       duration,
		}
	} else {
		bulkResult = models.BulkOperationResult{
			TotalProcessed: len(users),
			Successful:     len(users),
			Failed:         0,
			Duration:       duration,
		}
	}

	writeJSON(w, bulkResult)
}

// executeQueryHandler 任意の SQL をリクエストボディから受け取って実行するエンドポイント。
// SQL インジェクションの温床となるため無効化している。
// 本番用途では GetUsersByAgeRange のようにパラメータ化された専用ハンドラを追加する。
func executeQueryHandler(w http.ResponseWriter, _ *http.Request) {
	http.Error(w, "任意 SQL 実行エンドポイントは無効化されています", http.StatusGone)
}

// executeParallelQueriesHandler 任意の SQL を受け取って並列実行するデモ用エンドポイント。
// executeQueryHandler と同じ理由で無効化している。
func executeParallelQueriesHandler(w http.ResponseWriter, _ *http.Request) {
	http.Error(w, "任意 SQL 並列実行エンドポイントは無効化されています", http.StatusGone)
}

func healthCheckHandler(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	healthResults := dbManager.HealthCheck(ctx)

	status := "healthy"
	for _, err := range healthResults {
		if err != nil {
			status = "unhealthy"
			break
		}
	}

	response := models.HealthStatus{
		Service:   "multi-database-concurrent-access",
		Status:    status,
		Timestamp: time.Now(),
		Databases: make(map[string]string),
		Uptime:    time.Since(time.Now()), // 簡略化
		Version:   "1.0.0",
	}

	for dbType, err := range healthResults {
		if err != nil {
			response.Databases[dbType] = "unhealthy: " + err.Error()
		} else {
			response.Databases[dbType] = "healthy"
		}
	}

	writeJSON(w, response)
}

func getMetricsHandler(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, dbManager.GetMetrics())
}

func getDatabaseStatsHandler(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, dbManager.GetDatabaseStats())
}

func loadTestHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DBType            string `json:"db_type"`
		ConcurrentClients int    `json:"concurrent_clients"`
		RequestsPerClient int    `json:"requests_per_client"`
		TestDuration      string `json:"test_duration"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "無効なリクエストボディ", http.StatusBadRequest)
		return
	}

	// デフォルト値設定
	req.ConcurrentClients = cmp.Or(req.ConcurrentClients, 10)
	req.RequestsPerClient = cmp.Or(req.RequestsPerClient, 100)

	duration, err := time.ParseDuration(req.TestDuration)
	if err != nil {
		duration = 1 * time.Minute
	}

	// 負荷テスト実行
	result := runLoadTest(req.DBType, req.ConcurrentClients, req.RequestsPerClient, duration)

	writeJSON(w, result)
}

func runLoadTest(dbType string, concurrentClients, requestsPerClient int, duration time.Duration) map[string]any {
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	var wg sync.WaitGroup
	var totalRequests, successfulRequests atomic.Int64
	start := time.Now()
	query := "SELECT id, name, email FROM users WHERE id = " + placeholder(dbType, 1)

	for range concurrentClients {
		wg.Go(func() {
			for j := range requestsPerClient {
				totalRequests.Add(1)

				resultChan := dbManager.ExecuteQuery(ctx, dbType, query, int64(j%1000+1))

				select {
				case result := <-resultChan:
					if result.Error == nil {
						successfulRequests.Add(1)
					}
				case <-ctx.Done():
					return
				}

				// 短い間隔
				time.Sleep(1 * time.Millisecond)
			}
		})
	}

	wg.Wait()
	elapsed := time.Since(start)
	total, successful := totalRequests.Load(), successfulRequests.Load()

	return map[string]any{
		"total_requests":      total,
		"successful_requests": successful,
		"failed_requests":     total - successful,
		"success_rate":        float64(successful) / float64(total) * 100,
		"duration":            elapsed.String(),
		"requests_per_second": float64(total) / elapsed.Seconds(),
		"concurrent_clients":  concurrentClients,
		"requests_per_client": requestsPerClient,
	}
}

func startMetricsCollector() {
	for range time.Tick(10 * time.Second) {
		// データベース接続数更新
		stats := dbManager.GetDatabaseStats()
		for dbType, stat := range stats {
			activeConnections.WithLabelValues(dbType).Set(float64(stat.OpenConnections))
		}

		// メトリクス出力
		metrics := dbManager.GetMetrics()
		log.Printf("メトリクス - 総タスク: %d, 成功: %d, 失敗: %d, 平均レイテンシー: %v, アクティブタスク: %d",
			metrics.TotalTasks,
			metrics.SuccessTasks,
			metrics.FailedTasks,
			metrics.AverageLatency,
			metrics.ActiveTasks,
		)
	}
}
