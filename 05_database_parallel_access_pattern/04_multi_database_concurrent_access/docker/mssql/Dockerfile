FROM mcr.microsoft.com/mssql/server:2022-latest

# 環境変数設定
ENV ACCEPT_EULA=Y
ENV SA_PASSWORD=YourPassword123!
ENV MSSQL_PID=Express
ENV MSSQL_LCID=1033
ENV MSSQL_COLLATION=SQL_Latin1_General_CP1_CI_AS

# ユーザーをroot切り替え（一時的）
USER root

# 必要なツールのインストール
RUN apt-get update && apt-get install -y \
    curl \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# SQL Serverコマンドラインツールのインストール
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list > /etc/apt/sources.list.d/msprod.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y mssql-tools unixodbc-dev \
    && echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc

# カスタムスクリプト用ディレクトリ作成
RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

# 初期化スクリプトとエントリーポイントスクリプトをコピー
COPY init-mssql.sql ./
COPY entrypoint.sh ./
COPY configure-db.sh ./

# スクリプトに実行権限を付与
RUN chmod +x ./entrypoint.sh ./configure-db.sh

# データベースファイル用ディレクトリ作成
RUN mkdir -p /var/opt/mssql/data
RUN mkdir -p /var/opt/mssql/log
RUN mkdir -p /var/opt/mssql/backup

# SQL Serverユーザーに戻す
USER mssql

# ヘルスチェック設定
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P $SA_PASSWORD -Q "SELECT 1" -b -o /dev/null

# ポート公開
EXPOSE 1433

# エントリーポイント設定
ENTRYPOINT ["./entrypoint.sh"]
