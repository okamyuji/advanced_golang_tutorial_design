# マルチステージビルドでGoアプリケーションをビルド
FROM golang:1.24-alpine AS builder

# 必要なパッケージインストール
RUN apk add --no-cache git gcc musl-dev

# 作業ディレクトリ設定
WORKDIR /app

# Go modules ファイルをコピー
COPY go.mod go.sum ./

# 依存関係ダウンロード
RUN go mod download

# ソースコードコピー
COPY . .

# アプリケーションビルド
RUN CGO_ENABLED=1 GOOS=linux go build -a -installsuffix cgo -o main ./cmd/main.go

# 本番用イメージ
FROM alpine:latest

# 必要なパッケージインストール
RUN apk --no-cache add ca-certificates tzdata

# 作業ディレクトリ設定
WORKDIR /root/

# ビルドされたバイナリコピー
COPY --from=builder /app/main .

# 設定ファイルコピー
COPY --from=builder /app/configs ./configs

# ポート公開
EXPOSE 8080 8081

# ヘルスチェック
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:8080/health || exit 1

# アプリケーション実行
CMD ["./main"]
