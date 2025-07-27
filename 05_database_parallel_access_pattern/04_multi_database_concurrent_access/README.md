# マルチデータベース並行アクセスシステム

高負荷環境に対応したマルチデータベース並行アクセス実装のためのGolang並行プログラミングサンプルです。PostgreSQL、MySQL、MSSQL、Oracleの4つの主要RDBに対応し、ゴルーチンとチャネルを活用した効率的な並行処理を実現します。

**✅ 検証済み実績**: Apple M3 Pro (12コア・18GBメモリ) 環境で**5,000並行リクエスト処理・失敗率0%・455 RPS**の高性能を実現。

## 🚀 主要機能

### 高性能並行処理

- **ワーカープール**: 48並行処理（12コア×4）M3 Pro最適化済み
- **マルチデータベース対応**: PostgreSQL/MySQL/MSSQL/Oracle
- **コネクションプール**: 合計300接続（PostgreSQL:70/MySQL:100/MSSQL:50/Oracle:80）
- **負荷分散**: ラウンドロビンによる均等分散

### 障害耐性とモニタリング

- **サーキットブレーカー**: 障害時の自動遮断・回復
- **再試行機能**: 指数バックオフ制御
- **リアルタイムメトリクス**: Prometheus/Grafana統合
- **ヘルスチェック**: 各データベースの状態監視

### 運用機能

- **負荷テスト**: 段階的負荷増加テスト機能
- **バッチ処理**: 大量データの効率的な処理
- **設定管理**: 環境変数対応のYAML設定
- **Docker対応**: 完全なコンテナ化環境

## 📊 パフォーマンス実績

| 指標 | 実績値 | 検証環境 |
|------|--------|----------|
| **スループット** | **455 RPS** | 5,000並行リクエスト |
| **失敗率** | **0%** | 全RDB安定稼働 |
| **処理時間** | **10.4秒** | 5,000リクエスト処理 |
| **接続プール** | **300接続** | 4DB合計、wait_count=0 |

### 段階別負荷テスト結果

| テストレベル | 並行数 | リクエスト数 | 実行時間 | スループット | 失敗率 |
|-------------|--------|-------------|----------|-------------|--------|
| 軽量 | 10 | 200 | < 1秒 | 200+ RPS | 0% |
| 中負荷 | 25 | 1,000 | 3.6秒 | 275 RPS | 0% |
| 実負荷 | 50 | 5,000 | 10.4秒 | 455 RPS | 0% |

## 🏗️ アーキテクチャ

```text
┌─────────────────────┐    ┌──────────────────────┐    ┌─────────────────────┐
│   アプリケーション   │ →  │  並行管理マネージャー  │ →  │   各DBアダプター     │
│ - REST API         │    │ - ワーカープール      │    │ - PostgreSQL        │
│ - 負荷テスト       │    │ - エラーハンドリング   │    │ - MySQL             │
│ - メトリクス       │    │ - メトリクス収集      │    │ - MSSQL             │
└─────────────────────┘    │ - 再試行制御         │    │ - Oracle            │
                          └──────────────────────┘    └─────────────────────┘
                                     │
                          ┌──────────────────────┐
                          │    監視・メトリクス   │
                          │ - Prometheus        │
                          │ - Grafana           │
                          │ - アラート          │
                          └──────────────────────┘
```

## 🛠️ 技術スタック

### 言語・フレームワーク

- **Go 1.24+**: 並行処理の核心実装
- **net/http**: 標準ライブラリによる軽量HTTPルーティング
- **Prometheus**: メトリクス収集
- **Grafana**: 可視化ダッシュボード

### データベース

- **PostgreSQL 15**: 高性能リレーショナルDB
- **MySQL 8.0**: 人気の高いオープンソースDB
- **Microsoft SQL Server**: エンタープライズDB
- **Oracle Database 23c XE**: 企業向け高機能DB

### インフラ・運用

- **Docker & Docker Compose**: コンテナ化
- **Prometheus**: メトリクス収集
- **Grafana**: ダッシュボード

## 🚀 クイックスタート

### 前提条件

- Docker & Docker Compose
- Go 1.24+
- Make（推奨）

### 1. リポジトリのクローン

```bash
cd /Users/yujiokamoto/devs/golang/advanced_golang_tutorial_design/05_database_parallel_access_pattern/04_multi_database_concurrent_access
```

### 2. 環境構築

```bash
# 開発環境セットアップ
make setup-dev

# 依存関係インストール
make deps
```

### 3. データベース起動

```bash
# 全データベースをDockerで起動
docker compose up -d postgres mysql mssql oracle

# データベース起動確認（60秒待機）
sleep 60
```

### 4. アプリケーション起動

```bash
# 開発モードで起動（ホットリロード）
make run-dev

# または通常起動
make run
```

### 5. アクセス確認

- **アプリケーション**: http://localhost:8080
- **API ドキュメント**: http://localhost:8080/api/v1
- **メトリクス**: http://localhost:8081
- **Prometheus**: http://localhost:9090
- **Grafana**: http://localhost:3000 (admin/admin)

## 📚 API使用例

### ユーザー作成

```bash
curl -X POST http://localhost:8080/api/v1/users?db_type=PostgreSQL \
  -H "Content-Type: application/json" \
  -d '{
    "name": "John Doe",
    "email": "john@example.com",
    "age": 30
  }'
```

### ユーザー検索

```bash
curl "http://localhost:8080/api/v1/users/search?db_type=MySQL&min_age=20&max_age=40"
```

### 並列クエリ実行

```bash
curl -X POST http://localhost:8080/api/v1/databases/parallel \
  -H "Content-Type: application/json" \
  -d '{
    "PostgreSQL": "SELECT COUNT(*) FROM users",
    "MySQL": "SELECT COUNT(*) FROM users",
    "MSSQL": "SELECT COUNT(*) FROM users"
  }'
```

### バッチユーザー作成

```bash
curl -X POST http://localhost:8080/api/v1/users/bulk?db_type=Oracle \
  -H "Content-Type: application/json" \
  -d '[
    {"name": "User1", "email": "user1@example.com", "age": 25},
    {"name": "User2", "email": "user2@example.com", "age": 30}
  ]'
```

## 🧪 テスト・負荷テスト

### 単体テスト

```bash
# 全テスト実行
make test

# ベンチマークテスト
make test-bench

# インテグレーションテスト
make test-integration
```

### 負荷テスト

```bash
# 軽負荷テスト
curl -X POST http://localhost:8080/api/v1/loadtest \
  -H "Content-Type: application/json" \
  -d '{
    "db_type": "PostgreSQL",
    "concurrent_clients": 10,
    "requests_per_client": 100,
    "test_duration": "1m"
  }'

# Makefileによる本格的な負荷テスト
make load-test
```

### パフォーマンステスト

```bash
# パフォーマンス測定
make performance-test
```

## ⚙️ 設定

### 環境変数

主要な環境変数で動作をカスタマイズできます：

```bash
# データベース接続
export DB_POSTGRES_HOST=localhost
export DB_POSTGRES_PORT=5432
export DB_POSTGRES_USER=postgres
export DB_POSTGRES_PASSWORD=postgres

export DB_MYSQL_HOST=localhost
export DB_MYSQL_PORT=3306
export DB_MYSQL_USER=testuser
export DB_MYSQL_PASSWORD=testpass

# アプリケーション設定
export APP_ENV=development
export CONFIG_PATH=configs/config.yaml
```

### 設定ファイル

`configs/config.yaml`で詳細な設定が可能：

```yaml
# ワーカープール設定
worker_pool:
  max_workers: 0  # 0の場合はCPU数×4
  queue_size: 1000
  timeout_duration: 30s

# データベース固有設定
databases:
  postgresql:
    enabled: true
    max_open_conns: 25
    max_idle_conns: 10
    conn_max_lifetime: 1h
```

## 📊 監視・メトリクス

### Prometheusメトリクス

システムは以下のメトリクスを自動収集：

- **リクエスト数**: `http_requests_total`
- **レスポンス時間**: `http_request_duration_seconds`
- **データベース接続数**: `db_active_connections`
- **エラー率**: `db_operation_errors_total`
- **スループット**: `db_operations_per_second`

### Grafanaダッシュボード

事前設定されたダッシュボードで以下を監視：

- システム全体のパフォーマンス
- データベース別の詳細メトリクス
- エラー率とレスポンス時間
- 接続プールの状態

## 🔧 開発

### コード品質

```bash
# コードフォーマット
make fmt

# リント実行
make lint

# 静的解析
make vet

# セキュリティチェック
make security

# 全チェック実行
make check
```

### デバッグ

```bash
# デバッグモードで起動
APP_ENV=development make run-dev

# プロファイリング有効化
go tool pprof http://localhost:6060/debug/pprof/profile
```

## 🐳 Docker

### 開発環境

```bash
# 全サービス起動
make all-up

# ログ確認
make docker-logs

# 状態確認
make docker-status
```

### プロダクション

```bash
# プロダクション用ビルド
make build-prod

# Dockerイメージ作成
make docker-build
```

## 📖 詳細ドキュメント

### アーキテクチャ詳細

- [並行処理設計](docs/concurrency-design.md)
- [データベースアダプター](docs/database-adapters.md)
- [パフォーマンス最適化](docs/performance-optimization.md)

### 運用ガイド

- [デプロイメント](docs/deployment.md)
- [監視・アラート](docs/monitoring.md)
- [トラブルシューティング](docs/troubleshooting.md)

### 開発ガイドライン

- コード品質: `make check`で全チェックパス
- テストカバレッジ: 80%以上を維持
- ドキュメント: 新機能には適切なドキュメントを追加

このプロジェクトは実務レベルのGolang並行プログラミング実装例として設計されており、高負荷環境でのマルチデータベースアクセスパターンを学習できます。
