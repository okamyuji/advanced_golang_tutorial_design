# マルチデータベース並行アクセスシステム Makefile
.PHONY: help build run test clean docker-up docker-down docker-restart deps fmt lint vet security audit load-test

# デフォルトターゲット
.DEFAULT_GOAL := help

# 変数定義
BINARY_NAME=multi-db-server
DOCKER_COMPOSE_FILE=compose.yml
GO_FILES=$(shell find . -name "*.go" -type f)
PACKAGES=$(shell go list ./...)

# バージョン情報
VERSION := $(shell git describe --tags --always --dirty)
BUILD_TIME := $(shell date -u '+%Y-%m-%d_%H:%M:%S')
BUILD_USER := $(shell whoami)
BUILD_HOST := $(shell hostname)

# Linker flags
LDFLAGS=-ldflags "-X main.Version=$(VERSION) -X main.BuildTime=$(BUILD_TIME) -X main.BuildUser=$(BUILD_USER) -X main.BuildHost=$(BUILD_HOST)"

## ヘルプ表示
help:
	@echo "利用可能なコマンド:"
	@echo ""
	@echo "  build         アプリケーションをビルド"
	@echo "  run           アプリケーションを実行"
	@echo "  test          テストを実行"
	@echo "  clean         ビルド成果物をクリーンアップ"
	@echo ""
	@echo "  docker-up     Dockerコンテナを起動"
	@echo "  docker-down   Dockerコンテナを停止・削除"
	@echo "  docker-restart Dockerコンテナを再起動"
	@echo ""
	@echo "  deps          依存関係をインストール"
	@echo "  fmt           コードフォーマット"
	@echo "  lint          リント実行"
	@echo "  vet           静的解析実行"
	@echo "  security      セキュリティチェック"
	@echo "  audit         脆弱性チェック"
	@echo ""
	@echo "  load-test     負荷テスト実行"
	@echo ""

## 依存関係のインストール
deps:
	@echo "依存関係をインストール中..."
	go mod download
	go mod tidy
	@echo "✓ 依存関係のインストール完了"

## アプリケーションビルド
build: deps
	@echo "アプリケーションをビルド中..."
	CGO_ENABLED=1 go build $(LDFLAGS) -o bin/$(BINARY_NAME) ./cmd/main.go
	@echo "✓ ビルド完了: bin/$(BINARY_NAME)"

## 開発用ビルド（高速）
build-dev:
	@echo "開発用ビルド中..."
	go build -o bin/$(BINARY_NAME) ./cmd/main.go
	@echo "✓ 開発用ビルド完了"

## クロスプラットフォームビルド
build-all: deps
	@echo "クロスプラットフォームビルド中..."
	GOOS=linux GOARCH=amd64 CGO_ENABLED=1 go build $(LDFLAGS) -o bin/$(BINARY_NAME)-linux-amd64 ./cmd/main.go
	GOOS=darwin GOARCH=amd64 CGO_ENABLED=1 go build $(LDFLAGS) -o bin/$(BINARY_NAME)-darwin-amd64 ./cmd/main.go
	GOOS=windows GOARCH=amd64 CGO_ENABLED=1 go build $(LDFLAGS) -o bin/$(BINARY_NAME)-windows-amd64.exe ./cmd/main.go
	@echo "✓ クロスプラットフォームビルド完了"

## アプリケーション実行
run: build-dev
	@echo "アプリケーションを起動中..."
	./bin/$(BINARY_NAME)

## 開発モード実行（ホットリロード）
run-dev:
	@echo "開発モードで起動中..."
	@if command -v air > /dev/null; then \
		air; \
	else \
		echo "airがインストールされていません。go install github.com/cosmtrek/air@latest でインストールしてください"; \
		$(MAKE) run; \
	fi

## テスト実行
test:
	@echo "テストを実行中..."
	go test -v -race -coverprofile=coverage.out $(PACKAGES)
	go tool cover -html=coverage.out -o coverage.html
	@echo "✓ テスト完了。カバレッジレポート: coverage.html"

## ベンチマークテスト
test-bench:
	@echo "ベンチマークテストを実行中..."
	go test -bench=. -benchmem $(PACKAGES)
	@echo "✓ ベンチマークテスト完了"

## インテグレーションテスト
test-integration:
	@echo "インテグレーションテストを実行中..."
	docker compose -f $(DOCKER_COMPOSE_FILE) up -d postgres mysql mssql oracle
	sleep 30
	go test -tags=integration -v ./tests/integration/...
	docker compose -f $(DOCKER_COMPOSE_FILE) down
	@echo "✓ インテグレーションテスト完了"


## 負荷テスト
load-test: build
	@echo "負荷テストを実行中..."
	@if [ ! -f bin/$(BINARY_NAME) ]; then echo "アプリケーションがビルドされていません"; exit 1; fi
	@echo "Docker環境でデータベースを起動中..."
	docker compose -f $(DOCKER_COMPOSE_FILE) up -d postgres mysql mssql oracle
	@echo "データベース起動完了まで待機中..."
	sleep 60
	@echo "アプリケーションをバックグラウンドで起動中..."
	./bin/$(BINARY_NAME) &
	@echo "アプリケーション起動まで待機中..."
	sleep 10
	./scripts/load-test.sh
	@echo "アプリケーション停止中..."
	pkill -f $(BINARY_NAME) || true
	@echo "✓ 負荷テスト完了"

## コードフォーマット
fmt:
	@echo "コードフォーマット中..."
	go fmt $(PACKAGES)
	@echo "✓ フォーマット完了"

## リント実行
lint:
	@echo "リントを実行中..."
	@if command -v golangci-lint > /dev/null; then \
		golangci-lint run; \
	else \
		echo "golangci-lintがインストールされていません。https://golangci-lint.run/usage/install/ を参照してインストールしてください"; \
	fi
	@echo "✓ リント完了"

## Go vet実行
vet:
	@echo "静的解析を実行中..."
	go vet $(PACKAGES)
	@echo "✓ 静的解析完了"

## セキュリティチェック
security:
	@echo "セキュリティチェックを実行中..."
	@if command -v gosec > /dev/null; then \
		gosec ./...; \
	else \
		echo "gosecがインストールされていません。基本的なセキュリティチェックを実行中..."; \
		go vet -all ./...; \
		echo "go vetによる基本チェック完了"; \
	fi
	@echo "✓ セキュリティチェック完了"

## 脆弱性監査
audit:
	@echo "脆弱性監査を実行中..."
	@if command -v govulncheck > /dev/null; then \
		govulncheck ./...; \
	elif command -v nancy > /dev/null; then \
		go list -json -m all | nancy sleuth; \
	else \
		echo "govulncheckまたはnancyがインストールされていません。go install golang.org/x/vuln/cmd/govulncheck@latest でインストールしてください"; \
	fi
	@echo "✓ 脆弱性監査完了"

## Dockerコンテナ起動
docker-up:
	@echo "Dockerコンテナを起動中..."
	docker compose -f $(DOCKER_COMPOSE_FILE) up -d
	@echo "✓ Dockerコンテナ起動完了"
	@echo "サービス確認中..."
	docker compose -f $(DOCKER_COMPOSE_FILE) ps

## Dockerコンテナ停止・削除
docker-down:
	@echo "Dockerコンテナを停止・削除中..."
	docker compose -f $(DOCKER_COMPOSE_FILE) down -v
	@echo "✓ Dockerコンテナ停止・削除完了"

## Dockerコンテナ再起動
docker-restart:
	@echo "Dockerコンテナを再起動中..."
	docker compose -f $(DOCKER_COMPOSE_FILE) restart
	@echo "✓ Dockerコンテナ再起動完了"

## Dockerログ確認
docker-logs:
	@echo "Dockerコンテナのログを確認中..."
	docker compose -f $(DOCKER_COMPOSE_FILE) logs -f

## Dockerコンテナ状態確認
docker-status:
	@echo "Dockerコンテナの状態:"
	docker compose -f $(DOCKER_COMPOSE_FILE) ps

## データベース初期化
db-init: docker-up
	@echo "データベースを初期化中..."
	sleep 60  # データベース起動完了を待機
	@echo "✓ データベース初期化完了"

## データベースマイグレーション
db-migrate:
	@echo "データベースマイグレーションを実行中..."
	./scripts/migrate.sh
	@echo "✓ マイグレーション完了"

## データベースシード
db-seed:
	@echo "テストデータを投入中..."
	./scripts/seed.sh
	@echo "✓ テストデータ投入完了"

## データベースリセット
db-reset: docker-down db-init db-migrate db-seed
	@echo "✓ データベースリセット完了"

## クリーンアップ
clean:
	@echo "クリーンアップ中..."
	rm -rf bin/
	rm -rf coverage.*
	rm -rf *.log
	go clean -cache
	go clean -modcache
	@echo "✓ クリーンアップ完了"

## Docker環境クリーンアップ
clean-docker:
	@echo "Docker環境をクリーンアップ中..."
	docker compose -f $(DOCKER_COMPOSE_FILE) down -v --rmi all --remove-orphans
	docker system prune -f
	@echo "✓ Docker環境クリーンアップ完了"

## プロダクションビルド
build-prod: deps test lint vet security
	@echo "プロダクション用ビルド中..."
	CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo $(LDFLAGS) -o bin/$(BINARY_NAME) ./cmd/main.go
	@echo "✓ プロダクション用ビルド完了"

## Dockerイメージビルド
docker-build:
	@echo "Dockerイメージをビルド中..."
	docker build -t multi-db:$(VERSION) .
	docker tag multi-db:$(VERSION) multi-db:latest
	@echo "✓ Dockerイメージビルド完了"

## 開発環境セットアップ
setup-dev:
	@echo "開発環境をセットアップ中..."
	go install github.com/cosmtrek/air@latest
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	go install github.com/securecodewarrior/gosec/v2/cmd/gosec@latest
	go install github.com/sonatard/detekt@latest
	$(MAKE) deps
	@echo "✓ 開発環境セットアップ完了"

## CI/CD用ビルド
ci: deps fmt lint vet security test build
	@echo "✓ CI/CDビルド完了"

## リリース用パッケージ作成
release: build-all
	@echo "リリースパッケージを作成中..."
	mkdir -p release
	tar -czf release/$(BINARY_NAME)-$(VERSION)-linux-amd64.tar.gz -C bin $(BINARY_NAME)-linux-amd64
	tar -czf release/$(BINARY_NAME)-$(VERSION)-darwin-amd64.tar.gz -C bin $(BINARY_NAME)-darwin-amd64
	zip -j release/$(BINARY_NAME)-$(VERSION)-windows-amd64.zip bin/$(BINARY_NAME)-windows-amd64.exe
	@echo "✓ リリースパッケージ作成完了"

## 開発者向けチェック
check: fmt lint vet security test
	@echo "✓ 全チェック完了"

## モニタリングダッシュボード起動
monitoring-up:
	@echo "モニタリングダッシュボードを起動中..."
	docker compose -f $(DOCKER_COMPOSE_FILE) up -d prometheus grafana
	@echo "✓ モニタリングダッシュボード起動完了"
	@echo "Prometheus: http://localhost:9090"
	@echo "Grafana: http://localhost:3000 (admin/admin)"

## 全環境起動
all-up: docker-up monitoring-up
	@echo "✓ 全環境起動完了"
	@echo ""
	@echo "利用可能なサービス:"
	@echo "  アプリケーション: http://localhost:8080"
	@echo "  メトリクス: http://localhost:8081"
	@echo "  Prometheus: http://localhost:9090"
	@echo "  Grafana: http://localhost:3000"
	@echo ""

## バージョン情報表示
version:
	@echo "バージョン情報:"
	@echo "  Version: $(VERSION)"
	@echo "  Build Time: $(BUILD_TIME)"
	@echo "  Build User: $(BUILD_USER)"
	@echo "  Build Host: $(BUILD_HOST)"
