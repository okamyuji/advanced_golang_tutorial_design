# gRPC並行プログラミング - 高負荷対応実装

このプロジェクトは、Goを使用した高性能gRPCサーバーとクライアントの実装例を提供します。プロダクション環境での使用を想定した実務レベルの並行プログラミングパターン、セキュリティ機能、監視機能を実装しています。

**✅ 実装済み機能**: JWT認証、TLS暗号化、Prometheusメトリクス、サーキットブレーカー、接続プール、ワーカープール、4種類のRPCパターン

## 🎯 特徴

### 高性能サーバー

- **ワーカープール**: CPUコア数に基づく動的ワーカー管理
- **ストリーム管理**: 4種類のRPCパターン完全対応
- **リアルタイムメトリクス**: パフォーマンス監視とアラート
- **グレースフルシャットダウン**: 安全な終了処理
- **JWT認証**: トークンベース認証システム
- **TLS暗号化**: プロダクション対応の暗号化通信

### 高性能クライアント

- **接続プール**: ラウンドロビン負荷分散
- **サーキットブレーカー**: 障害時の自動遮断と回復
- **再試行機能**: 指数バックオフによる適切な再試行
- **コネクション管理**: Keepalive最適化

### セキュリティ機能

- **JWT認証**: 認証インターセプターによる自動トークン検証
- **ロールベース認可**: 柔軟な権限管理システム
- **TLS暗号化**: TLS 1.2以上による通信暗号化
- **セキュアな設定**: プロダクション環境向け最適化

### 監視・メトリクス

- **Prometheusメトリクス**: リクエスト数、レイテンシー、エラー率の監視
- **HTTPメトリクスサーバー**: `/metrics`エンドポイントでメトリクス公開
- **リアルタイム監視**: アクティブ接続数、ストリーム数の追跡
- **カスタムメトリクス**: ビジネスロジック固有の監視項目

### 負荷テスト機能

- **段階的負荷増加**: ランプアップによる現実的なテスト
- **詳細メトリクス**: P95/P99レイテンシー測定
- **複数シナリオ**: 低負荷から極限負荷まで対応

## 📁 プロジェクト構成

```text
06_grpc_concurrent_programming/
├── proto/                  # プロトコル定義
│   ├── user_service.proto
│   ├── user_service.pb.go
│   └── user_service_grpc.pb.go
├── server/                 # サーバー実装
│   └── server.go
├── client/                 # クライアント実装
│   └── client.go
├── security/               # セキュリティ機能
│   └── security.go
├── monitoring/             # メトリクス監視
│   └── metrics.go
├── examples/               # 使用例
│   └── main.go
├── loadtest/              # 負荷テスト
│   └── main.go
├── Makefile               # ビルド自動化
├── go.mod
└── README.md
```

## 🚀 実行方法

### 1. 依存関係のインストール

```bash
cd 06_grpc_concurrent_programming

# 推奨: Makefileを使用した初期化
make init  # deps + proto + build を一括実行

# または個別実行
make deps  # 依存関係インストール
go mod tidy
```

### 2. プロトコルバッファの生成

```bash
# Makefileを使用した自動生成
make proto

# 手動生成の場合
protoc --go_out=. --go_opt=paths=source_relative \
    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
    proto/user_service.proto
```

### 3. ビルド

```bash
# 全コンポーネントをビルド
make build

# 特定コンポーネントをビルド
make build-server
make build-client
```

### 4. 基本使用例の実行

```bash
# 基本的な使用例を実行
go run examples/main.go

# または
make run-examples
```

### 5. 負荷テストの実行

```bash
# 段階的負荷テスト（推奨）
make run-loadtest

# 直接実行
go run loadtest/main.go

# 負荷テスト詳細:
# - 低負荷: 10クライアント x 100リクエスト
# - 中負荷: 50クライアント x 200リクエスト  
# - 高負荷: 100クライアント x 300リクエスト
# - 極限負荷: 200クライアント x 500リクエスト
```

### 6. メトリクス監視

```bash
# サーバー起動後、メトリクスを確認
curl http://localhost:9090/metrics

# ヘルスチェック
curl http://localhost:9090/health
```

## 📊 パフォーマンス目標

| 指標 | 目標値 | 備考 |
|------|--------|------|
| スループット | 10,000+ RPS | 標準的なハードウェアで |
| レイテンシー(P99) | < 50ms | Unary RPC |
| 可用性 | 99.9%+ | サーキットブレーカー機能 |
| 同時接続数 | 1,000+ | 接続プール最適化 |

### 実際のパフォーマンス

基本構成（MacBook Pro M1）での測定結果：

- 並行クライアント: 10
- 1クライアントあたりリクエスト数: 5  
- 平均RPS: 900+
- レイテンシー: 1-8ms
- サーキットブレーカー: 適切に動作

## 🔧 設定例

### サーバー設定

```go
serverConfig := server.ServerConfig{
    Port:                   8080,
    MaxWorkers:             runtime.NumCPU() * 8,
    MaxConcurrentStreams:   1000,
    MaxReceiveMessageSize:  4 * 1024 * 1024, // 4MB
    MaxSendMessageSize:     4 * 1024 * 1024, // 4MB
    ConnectionTimeout:      30 * time.Second,
    KeepaliveTime:          30 * time.Second,
    KeepaliveTimeout:       5 * time.Second,
    MaxConnectionIdle:      15 * time.Minute,
    MaxConnectionAge:       30 * time.Minute,
    MaxConnectionAgeGrace:  5 * time.Minute,
    EnableTLS:              true, // プロダクション環境では必須
}
```

### クライアント設定

```go
clientConfig := client.ClientConfig{
    ServerAddresses:         []string{"localhost:8080"},
    MaxConnections:          10,
    MaxRetries:              3,
    InitialBackoff:          100 * time.Millisecond,
    MaxBackoff:              5 * time.Second,
    BackoffMultiplier:       2.0,
    DefaultTimeout:          10 * time.Second,
    CircuitBreakerThreshold: 5,
    CircuitBreakerTimeout:   30 * time.Second,
    KeepaliveTime:           30 * time.Second,
    KeepaliveTimeout:        5 * time.Second,
    PermitWithoutStream:     true,
    MaxReceiveMessageSize:   4 * 1024 * 1024,
    MaxSendMessageSize:      4 * 1024 * 1024,
    EnableTLS:               true, // プロダクション環境では必須
    AuthToken:               "Bearer <JWT_TOKEN>", // JWT認証トークン
}
```

## 🎨 RPC パターン

### 1. Unary RPC

```go
// ユーザー取得
user, err := client.GetUser(ctx, userID)

// ユーザー作成
user, err := client.CreateUser(ctx, "名前", "email@example.com")
```

### 2. Server Streaming RPC

```go
// ユーザーリスト取得（ストリーミング）
userChan, errorChan := client.ListUsersStream(ctx, 50)
for user := range userChan {
    fmt.Printf("User: %v\n", user)
}
```

### 3. Client Streaming RPC

```go
// 一括ユーザー作成
users := []*pb.CreateUserRequest{...}
result, err := client.BulkCreateUsersStream(ctx, users)
```

### 4. Bidirectional Streaming RPC

```go
// リアルタイムチャット
stream, err := client.UserChatStream(ctx)
// 送信と受信を並行して処理
```

## 🔐 セキュリティ機能

### JWT認証

```go
// JWTトークンの生成（テスト用）
token, err := security.GenerateJWT("user123", "admin")
if err != nil {
    log.Fatal(err)
}

// クライアント設定にトークンを設定
clientConfig.AuthToken = token
```

### TLS設定

```go
// サーバーでTLSを有効化
serverConfig.EnableTLS = true

// クライアントでTLSを有効化
clientConfig.EnableTLS = true
```

### 認証不要エンドポイント

- `/grpc.health.v1.Health/Check` - ヘルスチェック
- カスタム公開エンドポイント（設定可能）

## 📈 監視とメトリクス

### Prometheusメトリクス

#### gRPCメトリクス

- `grpc_requests_total` - 総リクエスト数（メソッド、ステータス別）
- `grpc_request_duration_seconds` - リクエスト処理時間
- `grpc_active_connections` - アクティブ接続数
- `grpc_active_streams` - アクティブストリーム数（メソッド別）
- `grpc_errors_total` - エラー数（メソッド、エラーコード別）
- `grpc_request_size_bytes` - リクエストサイズ
- `grpc_response_size_bytes` - レスポンスサイズ

#### サーバーメトリクス

- 総リクエスト数
- アクティブリクエスト数
- 成功/失敗率
- 平均/最大レイテンシー
- アクティブストリーム数

#### クライアントメトリクス

- 接続プール状態
- サーキットブレーカー動作回数
- 再試行統計
- レイテンシー分布

### メトリクス確認方法

```bash
# メトリクス一覧取得
curl http://localhost:9090/metrics

# 特定メトリクスの確認
curl http://localhost:9090/metrics | grep grpc_requests_total

# ヘルスチェック
curl http://localhost:9090/health
```

## 🛡️ エラーハンドリング

### サーキットブレーカー

```go
// 閾値を超えた失敗でサーキットブレーカーが動作
if !circuitBreaker.CanExecute() {
    return fmt.Errorf("サーキットブレーカーが開いています")
}
```

### 再試行機能

```go
// 指数バックオフによる再試行
for attempt := 0; attempt <= maxRetries; attempt++ {
    backoff := time.Duration(attempt) * initialBackoff
    // リクエスト実行
}
```

## 🏗️ プロダクション考慮事項

### セキュリティ

- ✅ **TLS暗号化**: TLS 1.2以上での通信暗号化
- ✅ **JWT認証**: トークンベース認証システム
- ✅ **ロールベース認可**: 柔軟な権限管理
- 🔄 **レート制限**: 実装推奨（将来的な拡張）
- 🔄 **証明書管理**: Let's Encrypt等の自動化

### 監視・ログ

- ✅ **Prometheusメトリクス**: 包括的なメトリクス収集
- ✅ **HTTPメトリクスサーバー**: 監視システム連携
- 🔄 **分散トレーシング**: Jaeger/Zipkin統合
- 🔄 **構造化ログ**: JSON形式でのログ出力
- 🔄 **ログ集約**: ELKスタック等との連携

### 運用・スケーリング

- ✅ **グレースフルシャットダウン**: 安全な停止処理
- ✅ **ヘルスチェック**: 可用性監視
- 🔄 **水平スケーリング**: Kubernetes対応
- 🔄 **ロードバランサー**: 負荷分散設定
- 🔄 **サービスディスカバリー**: 動的サービス発見

### パフォーマンス最適化

- ✅ **接続プール**: 効率的な接続管理
- ✅ **ワーカープール**: 並行処理最適化
- ✅ **サーキットブレーカー**: 障害時の自動保護
- ✅ **Keepalive最適化**: 接続維持設定
- 🔄 **キャッシュ層**: Redis等の活用

## 🧪 テスト

### 単体テスト

```bash
# 全テスト実行
make test

# カバレッジ付きテスト
make test-coverage
```

### 負荷テスト

```bash
# 段階的負荷テスト
make loadtest

# カスタム負荷テスト
go run loadtest/main.go -duration=60s -concurrent=100
```

## 📦 デプロイメント

### Docker対応

```bash
# Dockerイメージビルド
make docker-build

# コンテナ起動
make docker-run
```

### Kubernetes対応

```yaml
# deployment.yaml例
apiVersion: apps/v1
kind: Deployment
metadata:
  name: grpc-server
spec:
  replicas: 3
  selector:
    matchLabels:
      app: grpc-server
  template:
    metadata:
      labels:
        app: grpc-server
    spec:
      containers:
      - name: grpc-server
        image: grpc-concurrent-programming:latest
        ports:
        - containerPort: 8080
        - containerPort: 9090
        env:
        - name: ENABLE_TLS
          value: "true"
```
