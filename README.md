# Go並行プログラミング実践ガイド

本リポジトリは、Goにおける並行プログラミングの実践的な設計パターンとテクニックを学ぶためのサンプルコード集です。

## プロジェクト構成

各章は独立したディレクトリとして構成されており、理論から実践まで段階的に学習できるように設計されています。

### 第1章: 並行プログラミングの実践的基礎

**ディレクトリ**: `01_practical_foundations_of_concurrent_programming/`

- Goroutineとチャネルの基礎
- 基本的な同期制御パターン
- メモリ監視とGoroutine管理システム

### 第2章: ワーカープールパターンの実践応用

**ディレクトリ**: `02_practical_application_of_the_worker_pool_pattern/`

- Webスクレイピングシステム
- 基本的なワーカープール実装
- 画像処理の並行化
- 動的ワーカープール
- 優先度付きワーカープール
- サーキットブレーカー付きワーカープール

### 第3章: Fan-out/Fan-inとコンテキスト制御

**ディレクトリ**: `03_fan-out_fan-in_and_context_control/`

- Fan-out/Fan-inパイプライン
- コンテキスト制御システム
- ストリーム集約
- 適応的ロードバランサー
- 分散処理パイプライン
- リアルタイムデータ処理

### 第4章: Web処理のための並行プログラミング

**ディレクトリ**: `04_concurrent_programming_for_web_processing/`

- 並行HTTPサーバー
- WebSocket処理
- APIレート制限
- 大規模WebSocketサーバー
- 多重認証システム
- 非同期ロギングミドルウェア

### 第5章: データベース並行アクセスパターン

**ディレクトリ**: `05_database_parallel_access_pattern/`

- コネクションプール管理
- トランザクション制御
- バッチ処理
- 在庫管理システム
- 分散トランザクション
- 一括マイグレーション

### 第6章: エラーハンドリングと監視

**ディレクトリ**: `06_error_handling_and_monitoring/`

- サーキットブレーカー
- 自動回復システム
- 監視ダッシュボード
- 高度なサーキットブレーカー
- フォールバック管理
- Pub/Sub同期テスト

### 第7章: パフォーマンス最適化と本番運用

**ディレクトリ**: `07_performance_optimization_and_production_operations/`

- パフォーマンス最適化
- プロファイリングシステム
- 本番管理
- 高度な最適化
- 本番監視
- ゼロダウンタイム設定

## 技術スタック

- **言語**: Go 1.21+
- **データベース**: PostgreSQL (テスト用)
- **外部ライブラリ**: 
    - github.com/lib/pq (PostgreSQL driver)
    - github.com/testcontainers/testcontainers-go (統合テスト)
    - golang.org/x/time (レート制限)

## 実行方法

各章のディレクトリに移動して以下のコマンドを実行してください：

```bash
# ビルド
go build

# テスト実行
go test

# Lintチェック
golangci-lint run
```

## ディレクトリ構造

```text
.
├── 01_practical_foundations_of_concurrent_programming/
│   ├── 00_a_basic_goroutine_management_system/
│   ├── ans_01/ (メモリ監視システム)
│   ├── ans_02/
│   └── ans_03/
├── 02_practical_application_of_the_worker_pool_pattern/
│   ├── 01_web_scraper/
│   ├── 02_worker_pool/
│   ├── 03_image_processor/
│   ├── ans_01/ (動的ワーカープール)
│   ├── ans_02/ (優先度付きワーカープール)
│   └── ans_03/ (サーキットブレーカー付きプール)
├── 03_fan-out_fan-in_and_context_control/
│   ├── 01_fan_out_fan_in_pipeline/
│   ├── 02_context_control_system/
│   ├── 03_stream_aggregation/
│   ├── ans_01/ (適応的ロードバランサー)
│   ├── ans_02/ (分散処理パイプライン)
│   └── ans_03/ (リアルタイムデータ処理)
├── 04_concurrent_programming_for_web_processing/
│   ├── 01_concurrent_http_server/
│   ├── 02_websocket_handler/
│   ├── 03_api_rate_limiter/
│   ├── ans_01/ (大規模WebSocketサーバー)
│   ├── ans_02/ (多重認証システム)
│   └── ans_03/ (非同期ロギングミドルウェア)
├── 05_database_parallel_access_pattern/
│   ├── 01_connection_pool/
│   ├── 02_transaction_control/
│   ├── 03_batch_processing/
│   ├── ans_01/ (在庫管理システム)
│   ├── ans_02/ (分散トランザクション)
│   └── ans_03/ (一括マイグレーション)
├── 06_error_handling_and_monitoring/
│   ├── 01_circuit_breaker/
│   ├── 02_auto_healing/
│   ├── 03_monitoring_dashboard/
│   ├── ans_01/ (高度なサーキットブレーカー)
│   ├── ans_02/ (フォールバック管理)
│   └── ans_03/ (Pub/Sub同期テスト)
└── 07_performance_optimization_and_production_operations/
    ├── 01_performance_optimizer/
    ├── 02_profiling_system/
    ├── 03_production_manager/
    ├── ans_01/ (高度な最適化)
    ├── ans_02/ (本番監視)
    └── ans_03/ (ゼロダウンタイム設定)
```

## 学習の進め方

1. **基礎から応用へ**: 第1章から順番に学習することを推奨
2. **実践重視**: 各サンプルコードを実際に動かして理解を深める
3. **テスト駆動**: テストコードを読んで動作を理解する
4. **発展課題**: `ans_*/` ディレクトリの高度な実装例を参考にする

## 動作環境

- Go 1.24 以上
- Docker (統合テスト用)
- PostgreSQL (データベース関連の章)

## ライセンス

MIT License

## 貢献

本リポジトリへの貢献は歓迎します。Issue や Pull Request を通じてフィードバックをお寄せください。
