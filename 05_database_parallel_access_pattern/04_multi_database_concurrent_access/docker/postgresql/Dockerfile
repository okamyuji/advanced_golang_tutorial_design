FROM postgres:15-alpine

# 環境変数設定
ENV POSTGRES_DB=testdb
ENV POSTGRES_USER=postgres
ENV POSTGRES_PASSWORD=postgres
ENV POSTGRES_HOST_AUTH_METHOD=trust

# 初期化スクリプト用ディレクトリ作成
RUN mkdir -p /docker-entrypoint-initdb.d

# 初期化SQLスクリプトをコピー
COPY init-postgres.sql /docker-entrypoint-initdb.d/

# PostgreSQL設定の最適化
COPY postgresql.conf /etc/postgresql/postgresql.conf

# ヘルスチェック設定
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD pg_isready -U $POSTGRES_USER -d $POSTGRES_DB

# ポート公開
EXPOSE 5432

# 設定ファイルを指定してPostgreSQLを起動
CMD ["postgres", "-c", "config_file=/etc/postgresql/postgresql.conf"]
