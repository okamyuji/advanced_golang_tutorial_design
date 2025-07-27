#!/bin/bash

# MSSQL Server エントリーポイントスクリプト
# データベースの初期化とSQL Serverの起動を行う

set -e

echo "Starting MSSQL Server initialization..."

# SQL Serverをバックグラウンドで起動
echo "Starting SQL Server..."
/opt/mssql/bin/sqlservr &

# SQL Serverの起動を待機
echo "Waiting for SQL Server to start..."
sleep 30

# SQL Serverが起動するまで待機
for i in {1..50}; do
    if /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P $SA_PASSWORD -Q "SELECT 1" -C > /dev/null 2>&1; then
        echo "SQL Server is ready!"
        break
    fi
    echo "Waiting for SQL Server to be ready... ($i/50)"
    sleep 2
done

# データベース初期化スクリプトの実行
echo "Initializing database..."
if [ -f "/opt/mssql-tools/bin/init-mssql.sql" ]; then
    echo "Running database initialization script..."
    /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P $SA_PASSWORD -d master -i /opt/mssql-tools/bin/init-mssql.sql -C
    echo "Database initialization completed."
else
    echo "No initialization script found at /opt/mssql-tools/bin/init-mssql.sql"
fi

# データベース設定スクリプトの実行
if [ -f "/usr/src/app/configure-db.sh" ]; then
    echo "Running database configuration script..."
    bash /usr/src/app/configure-db.sh
    echo "Database configuration completed."
fi

echo "MSSQL Server initialization completed successfully!"

# フォアグラウンドでSQL Serverを実行
wait
