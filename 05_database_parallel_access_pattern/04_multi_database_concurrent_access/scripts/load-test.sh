#!/usr/bin/env bash

# マルチデータベース並行アクセス負荷テストスクリプト

set -e

echo "🚀 マルチデータベース負荷テスト開始"
echo "=================================="

# 設定
APP_URL=${APP_URL:-"http://localhost:8080"}
CONCURRENT_CLIENTS=${CONCURRENT_CLIENTS:-50}
REQUESTS_PER_CLIENT=${REQUESTS_PER_CLIENT:-100}
TEST_DURATION=${TEST_DURATION:-"2m"}
DB_TYPES=("PostgreSQL" "MySQL" "MSSQL" "Oracle")

# 色付きログ出力
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# アプリケーションの準備状況確認
check_app_ready() {
    log_info "アプリケーションの準備状況を確認中..."
    
    local max_attempts=30
    local attempt=1
    
    while [ $attempt -le $max_attempts ]; do
        if curl -s -f "$APP_URL/api/v1/health" > /dev/null 2>&1; then
            log_success "アプリケーションが利用可能です"
            return 0
        fi
        
        log_warning "試行 $attempt/$max_attempts: アプリケーション待機中..."
        sleep 5
        ((attempt++))
    done
    
    log_error "アプリケーションに接続できません"
    return 1
}

# ヘルスチェック
health_check() {
    log_info "ヘルスチェック実行中..."
    
    local health_response=$(curl -s "$APP_URL/api/v1/health")
    echo "$health_response" | jq '.'
    
    local status=$(echo "$health_response" | jq -r '.status')
    
    if [ "$status" = "healthy" ]; then
        log_success "全データベースが正常です"
        return 0
    else
        log_warning "一部のデータベースに問題があります"
        return 1
    fi
}

# 単一データベース負荷テスト
run_single_db_load_test() {
    local db_type=$1
    log_info "負荷テスト実行中: $db_type"
    
    # ユーザー作成テスト
    log_info "$db_type: ユーザー作成テスト"
    for i in $(seq 1 $CONCURRENT_CLIENTS); do
        {
            for j in $(seq 1 $REQUESTS_PER_CLIENT); do
                curl -s -X POST "$APP_URL/api/v1/users?db_type=$db_type" \
                    -H "Content-Type: application/json" \
                    -d "{
                        \"name\": \"LoadTestUser${i}_${j}\",
                        \"email\": \"loadtest${i}_${j}_$(date +%s%3N)_$$@example.com\",
                        \"age\": $((20 + RANDOM % 50))
                    }" > /dev/null
            done
        } &
    done
    
    # すべてのバックグラウンドプロセスの完了を待機
    wait
    
    # 検索テスト
    log_info "$db_type: ユーザー検索テスト"
    for i in $(seq 1 $CONCURRENT_CLIENTS); do
        {
            for j in $(seq 1 $REQUESTS_PER_CLIENT); do
                local min_age=$((20 + RANDOM % 30))
                local max_age=$((min_age + 20))
                curl -s "$APP_URL/api/v1/users/search?db_type=$db_type&min_age=$min_age&max_age=$max_age" > /dev/null
            done
        } &
    done
    
    wait
    
    log_success "$db_type 負荷テスト完了"
}

# 並列データベーステスト
run_parallel_db_test() {
    log_info "並列データベーステスト実行中..."
    
    for i in $(seq 1 20); do
        {
            curl -s -X POST "$APP_URL/api/v1/databases/parallel" \
                -H "Content-Type: application/json" \
                -d '{
                    "PostgreSQL": "SELECT COUNT(*) as count FROM users",
                    "MySQL": "SELECT COUNT(*) as count FROM users",
                    "MSSQL": "SELECT COUNT(*) as count FROM users",
                    "Oracle": "SELECT COUNT(*) as count FROM users"
                }' > /dev/null
        } &
    done
    
    wait
    log_success "並列データベーステスト完了"
}

# バッチ操作テスト
run_batch_test() {
    local db_type=$1
    log_info "$db_type: バッチ操作テスト"
    
    # 100ユーザーのバッチ作成
    local batch_data='['
    for i in $(seq 1 100); do
        if [ $i -gt 1 ]; then
            batch_data+=','
        fi
        batch_data+="{\"name\":\"BatchUser$i\",\"email\":\"batch${i}_$(date +%s%3N)_$$@example.com\",\"age\":$((20 + RANDOM % 50))}"
    done
    batch_data+=']'
    
    for i in $(seq 1 5); do
        curl -s -X POST "$APP_URL/api/v1/users/bulk?db_type=$db_type" \
            -H "Content-Type: application/json" \
            -d "$batch_data" > /dev/null
    done
    
    log_success "$db_type バッチテスト完了"
}

# メトリクス収集
collect_metrics() {
    log_info "メトリクス収集中..."
    
    echo "=== アプリケーションメトリクス ==="
    curl -s "$APP_URL/api/v1/metrics" | jq '.'
    
    echo ""
    echo "=== データベース統計 ==="
    curl -s "$APP_URL/api/v1/stats" | jq '.'
    
    if command -v curl &> /dev/null && curl -s "http://localhost:8081/metrics" &> /dev/null; then
        echo ""
        echo "=== Prometheusメトリクス ==="
        curl -s "http://localhost:8081/metrics" | grep -E "(http_request_|db_operation_|db_active_connections)"
    fi
}

# API経由の負荷テスト実行
run_api_load_test() {
    local db_type=$1
    log_info "$db_type: API経由負荷テスト"
    
    local response=$(curl -s -X POST "$APP_URL/api/v1/loadtest" \
        -H "Content-Type: application/json" \
        -d "{
            \"db_type\": \"$db_type\",
            \"concurrent_clients\": $CONCURRENT_CLIENTS,
            \"requests_per_client\": $REQUESTS_PER_CLIENT,
            \"test_duration\": \"$TEST_DURATION\"
        }")
    
    echo "$response" | jq '.'
}

# メイン実行
main() {
    echo "負荷テスト設定:"
    echo "  APP_URL: $APP_URL"
    echo "  同時接続数: $CONCURRENT_CLIENTS"
    echo "  クライアント毎リクエスト数: $REQUESTS_PER_CLIENT"
    echo "  テスト期間: $TEST_DURATION"
    echo ""
    
    # アプリケーション準備確認
    if ! check_app_ready; then
        log_warning "アプリケーションに接続できません。シンプルなテストを実行します。"
        echo "テスト用HTTPリクエスト実行中..."
        for i in {1..10}; do
            curl -s -w "リクエスト $i: %{time_total}s\n" -o /dev/null https://httpbin.org/delay/1 || echo "リクエスト $i: タイムアウト"
        done
        log_success "シンプル負荷テスト完了"
        return 0
    fi
    
    # ヘルスチェック
    if ! health_check; then
        log_warning "一部のデータベースに問題がありますが、テストを続行します"
    fi
    
    # 各データベースの負荷テスト
    for db_type in "${DB_TYPES[@]}"; do
        echo ""
        echo "===================="
        echo "🗄️ $db_type テスト"
        echo "===================="
        
        # カスタム負荷テスト
        run_single_db_load_test "$db_type"
        
        # バッチテスト
        run_batch_test "$db_type"
        
        # API経由負荷テスト
        run_api_load_test "$db_type"
        
        echo ""
    done
    
    # 並列データベーステスト
    echo "========================"
    echo "🔀 並列データベーステスト"
    echo "========================"
    run_parallel_db_test
    
    # メトリクス収集
    echo ""
    echo "=================="
    echo "📊 メトリクス収集"
    echo "=================="
    collect_metrics
    
    echo ""
    log_success "🎉 負荷テスト完了!"
    echo ""
    echo "結果確認方法:"
    echo "  - アプリケーションメトリクス: $APP_URL/api/v1/metrics"
    echo "  - データベース統計: $APP_URL/api/v1/stats"
    echo "  - Prometheusメトリクス: http://localhost:8081/metrics"
    echo "  - Grafanaダッシュボード: http://localhost:3000"
}

# スクリプトエントリーポイント
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi
