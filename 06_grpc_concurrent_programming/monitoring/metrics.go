package monitoring

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Prometheusメトリクス定義
var (
	// requestsTotal gRPCリクエストの総数
	requestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "grpc_requests_total",
			Help: "Total number of gRPC requests",
		},
		[]string{"method", "status"},
	)

	// requestDuration gRPCリクエストの処理時間
	requestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "grpc_request_duration_seconds",
			Help:    "Duration of gRPC requests",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method"},
	)

	// activeConnections アクティブ接続数
	activeConnections = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "grpc_active_connections",
			Help: "Number of active gRPC connections",
		},
	)

	// streamCount アクティブストリーム数
	streamCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "grpc_active_streams",
			Help: "Number of active gRPC streams",
		},
		[]string{"method"},
	)

	// errorRate エラー率
	errorRate = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "grpc_errors_total",
			Help: "Total number of gRPC errors",
		},
		[]string{"method", "code"},
	)

	// requestSize リクエストサイズ
	requestSize = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "grpc_request_size_bytes",
			Help:    "Size of gRPC requests in bytes",
			Buckets: prometheus.ExponentialBuckets(64, 4, 10),
		},
		[]string{"method"},
	)

	// responseSize レスポンスサイズ
	responseSize = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "grpc_response_size_bytes",
			Help:    "Size of gRPC responses in bytes",
			Buckets: prometheus.ExponentialBuckets(64, 4, 10),
		},
		[]string{"method"},
	)
)

// InitMetrics Prometheusメトリクスを初期化します
func InitMetrics() {
	prometheus.MustRegister(
		requestsTotal,
		requestDuration,
		activeConnections,
		streamCount,
		errorRate,
		requestSize,
		responseSize,
	)
}

// MetricsInterceptor Prometheusメトリクス収集用のインターセプターを提供します
func MetricsInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		startTime := time.Now()
		method := info.FullMethod

		// リクエスト数をインクリメント
		requestsTotal.WithLabelValues(method, "started").Inc()

		// ハンドラーを実行
		resp, err := handler(ctx, req)

		// 処理時間を記録
		duration := time.Since(startTime).Seconds()
		requestDuration.WithLabelValues(method).Observe(duration)

		// ステータスコードを取得
		statusCode := codes.OK
		statusString := "success"
		if err != nil {
			if grpcErr, ok := status.FromError(err); ok {
				statusCode = grpcErr.Code()
			} else {
				statusCode = codes.Internal
			}
			statusString = "error"
			errorRate.WithLabelValues(method, statusCode.String()).Inc()
		}

		// リクエスト完了数を記録
		requestsTotal.WithLabelValues(method, statusString).Inc()

		return resp, err
	}
}

// StreamMetricsInterceptor ストリーミングRPC用のメトリクス収集インターセプターを提供します
func StreamMetricsInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		startTime := time.Now()
		method := info.FullMethod

		// ストリーム開始
		streamCount.WithLabelValues(method).Inc()
		requestsTotal.WithLabelValues(method, "stream_started").Inc()

		// ハンドラーを実行
		err := handler(srv, ss)

		// ストリーム終了
		streamCount.WithLabelValues(method).Dec()

		// 処理時間を記録
		duration := time.Since(startTime).Seconds()
		requestDuration.WithLabelValues(method).Observe(duration)

		// エラー処理
		statusString := "success"
		if err != nil {
			statusString = "error"
			if grpcErr, ok := status.FromError(err); ok {
				errorRate.WithLabelValues(method, grpcErr.Code().String()).Inc()
			} else {
				errorRate.WithLabelValues(method, codes.Internal.String()).Inc()
			}
		}

		requestsTotal.WithLabelValues(method, statusString).Inc()

		return err
	}
}

// RecordConnectionOpen 接続開始を記録します
func RecordConnectionOpen() {
	activeConnections.Inc()
}

// RecordConnectionClose 接続終了を記録します
func RecordConnectionClose() {
	activeConnections.Dec()
}

// RecordRequestSize リクエストサイズを記録します
func RecordRequestSize(method string, size int) {
	requestSize.WithLabelValues(method).Observe(float64(size))
}

// RecordResponseSize レスポンスサイズを記録します
func RecordResponseSize(method string, size int) {
	responseSize.WithLabelValues(method).Observe(float64(size))
}

// StartMetricsServer Prometheusメトリクス用のHTTPサーバーを起動します
func StartMetricsServer(port int) error {
	http.Handle("/metrics", promhttp.Handler())
	
	// ヘルスチェックエンドポイント
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	address := ":" + strconv.Itoa(port)
	return http.ListenAndServe(address, nil)
}

// GetMetricsSummary 現在のメトリクス概要を取得します
func GetMetricsSummary() map[string]interface{} {
	// Prometheusから現在の値を取得（実際の実装では、各メトリクスの値を収集）
	summary := map[string]interface{}{
		"active_connections": getGaugeValue(activeConnections),
		"total_requests":     getCounterVecValue(requestsTotal),
		"error_rate":         getCounterVecValue(errorRate),
		"active_streams":     getGaugeVecValue(streamCount),
	}

	return summary
}

// 以下は内部関数（実際の実装では、Prometheusからメトリクス値を取得する必要があります）
func getGaugeValue(gauge prometheus.Gauge) float64 {
	// 実際の実装では、prometheus.GaugeからDto()を使用してメトリクス値を取得
	return 0.0 // プレースホルダー
}

func getCounterVecValue(counterVec *prometheus.CounterVec) map[string]float64 {
	// 実際の実装では、prometheus.CounterVecからメトリクス値を取得
	return map[string]float64{} // プレースホルダー
}

func getGaugeVecValue(gaugeVec *prometheus.GaugeVec) map[string]float64 {
	// 実際の実装では、prometheus.GaugeVecからメトリクス値を取得
	return map[string]float64{} // プレースホルダー
}

// CustomMetrics カスタムメトリクス構造体
type CustomMetrics struct {
	BusinessMetrics map[string]prometheus.Counter
	TimingMetrics   map[string]prometheus.Histogram
}

// NewCustomMetrics 新しいカスタムメトリクスを作成します
func NewCustomMetrics() *CustomMetrics {
	return &CustomMetrics{
		BusinessMetrics: make(map[string]prometheus.Counter),
		TimingMetrics:   make(map[string]prometheus.Histogram),
	}
}

// AddBusinessMetric ビジネスメトリクスを追加します
func (cm *CustomMetrics) AddBusinessMetric(name, help string) {
	counter := prometheus.NewCounter(prometheus.CounterOpts{
		Name: name,
		Help: help,
	})
	cm.BusinessMetrics[name] = counter
	prometheus.MustRegister(counter)
}

// AddTimingMetric タイミングメトリクスを追加します
func (cm *CustomMetrics) AddTimingMetric(name, help string) {
	histogram := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    name,
		Help:    help,
		Buckets: prometheus.DefBuckets,
	})
	cm.TimingMetrics[name] = histogram
	prometheus.MustRegister(histogram)
}

// IncrementBusinessMetric ビジネスメトリクスをインクリメントします
func (cm *CustomMetrics) IncrementBusinessMetric(name string) {
	if counter, ok := cm.BusinessMetrics[name]; ok {
		counter.Inc()
	}
}

// ObserveTimingMetric タイミングメトリクスを記録します
func (cm *CustomMetrics) ObserveTimingMetric(name string, duration time.Duration) {
	if histogram, ok := cm.TimingMetrics[name]; ok {
		histogram.Observe(duration.Seconds())
	}
}