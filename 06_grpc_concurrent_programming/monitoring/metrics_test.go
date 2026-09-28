package monitoring

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestMetricsInterceptor_エラーをステータスコード別に数える(t *testing.T) {
	const method = "/test.Service/Fail"
	interceptor := MetricsInterceptor()
	info := &grpc.UnaryServerInfo{FullMethod: method}
	handler := func(ctx context.Context, req any) (any, error) {
		return nil, status.Error(codes.NotFound, "not found")
	}

	// カウンターはパッケージ変数で -count を重ねると累積するので、呼び出し前との差で判定する
	notFound := errorRate.WithLabelValues(method, codes.NotFound.String())
	completed := requestsTotal.WithLabelValues(method, "error")
	notFoundBefore, completedBefore := testutil.ToFloat64(notFound), testutil.ToFloat64(completed)

	if _, err := interceptor(t.Context(), nil, info, handler); status.Code(err) != codes.NotFound {
		t.Fatalf("コード = %v, want NotFound", status.Code(err))
	}

	if got := testutil.ToFloat64(notFound) - notFoundBefore; got != 1 {
		t.Fatalf("NotFound のエラー数の増分 = %v, want 1", got)
	}
	if got := testutil.ToFloat64(completed) - completedBefore; got != 1 {
		t.Fatalf("error の完了数の増分 = %v, want 1", got)
	}
}

func TestNewMux_ヘルスチェックはOKを返す(t *testing.T) {
	rec := httptest.NewRecorder()
	newMux().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))

	if rec.Code != http.StatusOK || rec.Body.String() != "OK" {
		t.Fatalf("応答 = %d %q, want 200 OK", rec.Code, rec.Body.String())
	}
}
