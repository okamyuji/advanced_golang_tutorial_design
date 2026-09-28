package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"maps"
	"math"
	"net"
	"net/http"
	httppprof "net/http/pprof"
	"os"
	"runtime"
	"runtime/metrics"
	"runtime/pprof"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// net/http/pprof は import した時点で DefaultServeMux に登録する。この init は import 先の init より後に動くので、
// ここで空の mux に差し替えておけば、別のコードが nil ハンドラーで全インターフェースに待ち受けても pprof は出ない
func init() {
	http.DefaultServeMux = http.NewServeMux()
}

var (
	errAlreadyRunning = errors.New("プロファイリングは実行中です")
	errStopped        = errors.New("プロファイリングは停止済みです")
)

// ProfilingSystem 継続的性能監視システム
type ProfilingSystem struct {
	httpServer      *http.Server
	profileInterval time.Duration
	dataRetention   time.Duration
	profiles        map[string]*ProfileData
	mutex           sync.RWMutex
	running         atomic.Bool
	stopChan        chan struct{}

	// stateMu 開始と停止の多重呼び出しを排他する
	stateMu sync.Mutex
	stopped bool
	addr    net.Addr // 実際に待ち受けているアドレス（ポート0を指定した場合に確定したポートを知るため）
	wg      sync.WaitGroup
}

// ProfileData プロファイルデータ構造
type ProfileData struct {
	Timestamp     time.Time         `json:"timestamp"`
	CPUProfile    string            `json:"cpu_profile,omitempty"`
	MemProfile    string            `json:"mem_profile,omitempty"`
	GoroutineInfo *GoroutineInfo    `json:"goroutine_info"`
	MemStats      *runtime.MemStats `json:"mem_stats"`
	Metrics       map[string]any    `json:"metrics"`
}

// GoroutineInfo Goroutine情報。状態別の数は runtime/metrics の近似値で、合計は Count と一致するとは限らない
type GoroutineInfo struct {
	Count    int `json:"count"`
	Running  int `json:"running"`
	Runnable int `json:"runnable"`
	Waiting  int `json:"waiting"`
	Syscall  int `json:"syscall"`
}

// goroutineMetricNames collectGoroutineInfo が読むメトリクス（Go 1.26 以降）
var goroutineMetricNames = []string{
	"/sched/goroutines:goroutines",
	"/sched/goroutines/running:goroutines",
	"/sched/goroutines/runnable:goroutines",
	"/sched/goroutines/waiting:goroutines",
	"/sched/goroutines/not-in-go:goroutines",
}

// PerformanceAlert パフォーマンスアラート
type PerformanceAlert struct {
	Level     string         `json:"level"`
	Message   string         `json:"message"`
	Timestamp time.Time      `json:"timestamp"`
	Metrics   map[string]any `json:"metrics"`
}

// NewProfilingSystem 新しいプロファイリングシステムを作成
func NewProfilingSystem(port int, profileInterval, dataRetention time.Duration) *ProfilingSystem {
	mux := http.NewServeMux()

	ps := &ProfilingSystem{
		httpServer: &http.Server{
			// pprof はメモリの中身やスタックを返すので、外部から届かないループバックだけで待ち受ける
			Addr:              fmt.Sprintf("127.0.0.1:%d", port),
			Handler:           localRequestsOnly(limitSeconds(mux)),
			ReadHeaderTimeout: 5 * time.Second,
			// pprof は seconds の分だけ書き込み期限を自分で延ばすので、長さは limitSeconds で抑える
			WriteTimeout: 60 * time.Second,
			IdleTimeout:  60 * time.Second,
		},
		profileInterval: profileInterval,
		dataRetention:   dataRetention,
		profiles:        make(map[string]*ProfileData),
		stopChan:        make(chan struct{}),
	}

	// カスタムエンドポイントを追加
	// net/http/pprof を import するだけでは DefaultServeMux にしか登録されないので、この mux に明示的に登録する
	mux.HandleFunc("/debug/pprof/", httppprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", httppprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", httppprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", httppprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", httppprof.Trace)
	mux.HandleFunc("/health", ps.healthHandler)
	mux.HandleFunc("/metrics", ps.metricsHandler)
	mux.HandleFunc("/profiles", ps.profilesHandler)
	mux.HandleFunc("/alerts", ps.alertsHandler)

	return ps
}

// maxProfileSeconds seconds に指定できる秒数の上限。pprof 自身には上限がなく、長い要求は Stop でも止まらない
const maxProfileSeconds = 60

// limitSeconds seconds が数値でないか maxProfileSeconds を超える要求を400で断る。
// profile と trace だけでなく、Index 経由の差分プロファイル（heap?seconds=N など）も seconds の分だけ待つので、全経路に掛ける
func limitSeconds(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if v := r.FormValue("seconds"); v != "" {
			// NaN は比較がすべて偽になり、上限の判定をすり抜けるので明示的に弾く
			if sec, err := strconv.ParseFloat(v, 64); err != nil || math.IsNaN(sec) || sec > maxProfileSeconds {
				http.Error(w, fmt.Sprintf("seconds は %d 以下の数値にしてください", maxProfileSeconds), http.StatusBadRequest)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

// localRequestsOnly 同じマシンの人が直接送った要求だけを通す。
// ループバックで待ち受けても、DNS rebinding ではブラウザが攻撃者のドメイン名のまま接続してくる。
// Host がループバックでも、別サイトのページは <img> などでブラウザに要求を送らせられる
func localRequestsOnly(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Sec-Fetch-Site") == "cross-site" {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		host, _, err := net.SplitHostPort(r.Host)
		if err != nil {
			host = r.Host // ポートを含まない Host
		}
		if ip := net.ParseIP(host); host != "localhost" && (ip == nil || !ip.IsLoopback()) {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// StartProfiling プロファイリング開始。ポートを確保できなければエラーを返す
func (ps *ProfilingSystem) StartProfiling(ctx context.Context) error {
	ps.stateMu.Lock()
	defer ps.stateMu.Unlock()
	// http.Server と stopChan は停止後に再利用できないので、停止後の再開は受け付けない
	if ps.stopped {
		return errStopped
	}
	if ps.running.Load() {
		return errAlreadyRunning
	}

	listener, err := net.Listen("tcp", ps.httpServer.Addr)
	if err != nil {
		return fmt.Errorf("プロファイリングサーバー起動エラー: %w", err)
	}
	ps.addr = listener.Addr()
	ps.running.Store(true)

	// HTTP pprofエンドポイント起動
	fmt.Printf("プロファイリングサーバー開始: %s\n", listener.Addr())
	ps.wg.Go(func() {
		if err := ps.httpServer.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fmt.Printf("プロファイリングサーバーエラー: %v\n", err)
		}
	})

	// 定期プロファイル収集
	ps.wg.Go(func() { ps.profileCollectionLoop(ctx) })

	// データクリーンアップ
	ps.wg.Go(func() { ps.dataCleanupLoop(ctx) })

	return nil
}

// listenAddr 待ち受け中のアドレスを返す。開始前は空文字列
func (ps *ProfilingSystem) listenAddr() string {
	ps.stateMu.Lock()
	defer ps.stateMu.Unlock()
	if ps.addr == nil {
		return ""
	}
	return ps.addr.String()
}

// Stop プロファイリング停止。二度目以降の呼び出しや開始前の呼び出しは何もしない
func (ps *ProfilingSystem) Stop() error {
	ps.stateMu.Lock()
	defer ps.stateMu.Unlock()
	if !ps.running.Load() {
		return nil
	}
	ps.running.Store(false)
	ps.stopped = true
	close(ps.stopChan)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := ps.httpServer.Shutdown(ctx)
	ps.wg.Wait()
	return err
}

// profileCollectionLoop プロファイル収集ループ
func (ps *ProfilingSystem) profileCollectionLoop(ctx context.Context) {
	tick := time.Tick(ps.profileInterval)

	for {
		select {
		case <-tick:
			ps.collectProfiles()
		case <-ctx.Done():
			return
		case <-ps.stopChan:
			return
		}
	}
}

// collectProfiles プロファイル収集
func (ps *ProfilingSystem) collectProfiles() {
	timestamp := time.Now()
	profileKey := timestamp.Format("2006-01-02T15:04:05")

	// メモリ統計収集
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	// Goroutine情報収集
	goroutineInfo := ps.collectGoroutineInfo()

	// カスタムメトリクス収集
	metrics := ps.collectCustomMetrics()

	profileData := &ProfileData{
		Timestamp:     timestamp,
		GoroutineInfo: goroutineInfo,
		MemStats:      &memStats,
		Metrics:       metrics,
	}

	ps.mutex.Lock()
	ps.profiles[profileKey] = profileData
	ps.mutex.Unlock()

	// アラート判定
	ps.checkPerformanceAlerts(profileData)
}

// collectGoroutineInfo Goroutine情報収集
func (ps *ProfilingSystem) collectGoroutineInfo() *GoroutineInfo {
	samples := make([]metrics.Sample, len(goroutineMetricNames))
	for i, name := range goroutineMetricNames {
		samples[i].Name = name
	}
	metrics.Read(samples)

	values := make([]int, len(samples))
	for i, sample := range samples {
		// 古いランタイムなど、メトリクスが存在しない場合は KindBad になるので 0 のままにする
		if sample.Value.Kind() == metrics.KindUint64 {
			values[i] = int(sample.Value.Uint64())
		}
	}

	return &GoroutineInfo{
		Count:    values[0],
		Running:  values[1],
		Runnable: values[2],
		Waiting:  values[3],
		Syscall:  values[4],
	}
}

// collectCustomMetrics カスタムメトリクス収集
func (ps *ProfilingSystem) collectCustomMetrics() map[string]any {
	return map[string]any{
		"timestamp":       time.Now().Unix(),
		"cpu_count":       runtime.NumCPU(),
		"goroutine_count": runtime.NumGoroutine(),
		"cgo_calls":       runtime.NumCgoCall(),
	}
}

// checkPerformanceAlerts パフォーマンスアラート判定。発生したアラートを返す
func (ps *ProfilingSystem) checkPerformanceAlerts(data *ProfileData) []PerformanceAlert {
	alerts := []PerformanceAlert{}

	// メモリ使用量アラート
	if data.MemStats.Alloc > 100*1024*1024 { // 100MB超過
		alerts = append(alerts, PerformanceAlert{
			Level:     "WARNING",
			Message:   "メモリ使用量が閾値を超過しています",
			Timestamp: data.Timestamp,
			Metrics: map[string]any{
				"memory_alloc": data.MemStats.Alloc,
				"threshold":    100 * 1024 * 1024,
			},
		})
	}

	// Goroutine数アラート
	if data.GoroutineInfo.Count > 1000 {
		alerts = append(alerts, PerformanceAlert{
			Level:     "WARNING",
			Message:   "Goroutine数が閾値を超過しています",
			Timestamp: data.Timestamp,
			Metrics: map[string]any{
				"goroutine_count": data.GoroutineInfo.Count,
				"threshold":       1000,
			},
		})
	}

	// アラートがある場合はログ出力
	for _, alert := range alerts {
		fmt.Printf("ALERT [%s] %s: %s\n", alert.Level, alert.Timestamp.Format(time.RFC3339), alert.Message)
	}
	return alerts
}

// dataCleanupLoop データクリーンアップループ
func (ps *ProfilingSystem) dataCleanupLoop(ctx context.Context) {
	tick := time.Tick(time.Hour)

	for {
		select {
		case <-tick:
			ps.cleanupOldData()
		case <-ctx.Done():
			return
		case <-ps.stopChan:
			return
		}
	}
}

// cleanupOldData 古いデータのクリーンアップ
func (ps *ProfilingSystem) cleanupOldData() {
	cutoff := time.Now().Add(-ps.dataRetention)

	ps.mutex.Lock()
	defer ps.mutex.Unlock()

	for key, profile := range ps.profiles {
		if profile.Timestamp.Before(cutoff) {
			delete(ps.profiles, key)
		}
	}
}

// HTTP ハンドラー関数群

// healthHandler ヘルスチェックハンドラー
func (ps *ProfilingSystem) healthHandler(w http.ResponseWriter, r *http.Request) {
	status := map[string]any{
		"status":    "healthy",
		"timestamp": time.Now().Unix(),
		"running":   ps.running.Load(),
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(status); err != nil {
		log.Printf("Failed to encode status response: %v", err)
	}
}

// metricsHandler メトリクスハンドラー
func (ps *ProfilingSystem) metricsHandler(w http.ResponseWriter, r *http.Request) {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	metrics := map[string]any{
		"goroutine_count": runtime.NumGoroutine(),
		"memory_alloc":    memStats.Alloc,
		"memory_sys":      memStats.Sys,
		"gc_cycles":       memStats.NumGC,
		"timestamp":       time.Now().Unix(),
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(metrics); err != nil {
		log.Printf("Failed to encode metrics response: %v", err)
	}
}

// profilesHandler プロファイルデータハンドラー
func (ps *ProfilingSystem) profilesHandler(w http.ResponseWriter, r *http.Request) {
	// 最新の10件のプロファイルを新しい順に返す（map の走査順は不定なので並べ替える）
	ps.mutex.RLock()
	profiles := slices.SortedFunc(maps.Values(ps.profiles), func(a, b *ProfileData) int {
		return b.Timestamp.Compare(a.Timestamp)
	})
	ps.mutex.RUnlock()
	profiles = profiles[:min(len(profiles), 10)]

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(profiles); err != nil {
		log.Printf("Failed to encode profiles response: %v", err)
	}
}

// alertsHandler アラートハンドラー
func (ps *ProfilingSystem) alertsHandler(w http.ResponseWriter, r *http.Request) {
	// 簡略化のため、現在のメトリクスベースでアラート状態を判定
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	alerts := []PerformanceAlert{}

	if memStats.Alloc > 100*1024*1024 {
		alerts = append(alerts, PerformanceAlert{
			Level:     "WARNING",
			Message:   "メモリ使用量が閾値を超過",
			Timestamp: time.Now(),
			Metrics: map[string]any{
				"memory_alloc": memStats.Alloc,
			},
		})
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(alerts); err != nil {
		log.Printf("Failed to encode alerts response: %v", err)
	}
}

// SaveCPUProfile CPUプロファイル保存
func (ps *ProfilingSystem) SaveCPUProfile(filename string, duration time.Duration) error {
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer func() {
		if err := f.Close(); err != nil {
			log.Printf("Failed to close CPU profile file: %v", err)
		}
	}()

	if err := pprof.StartCPUProfile(f); err != nil {
		return err
	}

	time.Sleep(duration)
	pprof.StopCPUProfile()

	return nil
}

// SaveMemProfile メモリプロファイル保存
func (ps *ProfilingSystem) SaveMemProfile(filename string) error {
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer func() {
		if err := f.Close(); err != nil {
			log.Printf("Failed to close memory profile file: %v", err)
		}
	}()

	runtime.GC()
	return pprof.WriteHeapProfile(f)
}

func main() {
	ctx := context.Background()

	// プロファイリングシステムを作成
	profiler := NewProfilingSystem(8080, 30*time.Second, 24*time.Hour)

	// システム開始
	if err := profiler.StartProfiling(ctx); err != nil {
		panic(err)
	}
	defer func() {
		if err := profiler.Stop(); err != nil {
			log.Printf("Failed to stop profiler: %v", err)
		}
	}()

	fmt.Println("プロファイリングシステム開始")
	fmt.Println("エンドポイント:")
	fmt.Println("- http://localhost:8080/debug/pprof/ (標準プロファイル)")
	fmt.Println("- http://localhost:8080/health (ヘルスチェック)")
	fmt.Println("- http://localhost:8080/metrics (メトリクス)")
	fmt.Println("- http://localhost:8080/profiles (プロファイルデータ)")
	fmt.Println("- http://localhost:8080/alerts (アラート)")

	// 30秒間プロファイリング実行
	time.Sleep(30 * time.Second)

	// CPUプロファイル保存
	if err := profiler.SaveCPUProfile("cpu_profile.out", 5*time.Second); err != nil {
		fmt.Printf("CPUプロファイル保存エラー: %v\n", err)
	} else {
		fmt.Println("CPUプロファイルを保存しました: cpu_profile.out")
	}

	// メモリプロファイル保存
	if err := profiler.SaveMemProfile("mem_profile.out"); err != nil {
		fmt.Printf("メモリプロファイル保存エラー: %v\n", err)
	} else {
		fmt.Println("メモリプロファイルを保存しました: mem_profile.out")
	}
}
