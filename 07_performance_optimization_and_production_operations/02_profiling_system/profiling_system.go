package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
	"time"
)

// ProfilingSystem 継続的性能監視システム
type ProfilingSystem struct {
	httpServer      *http.Server
	profileInterval time.Duration
	dataRetention   time.Duration
	profiles        map[string]*ProfileData
	mutex           sync.RWMutex
	running         bool
	stopChan        chan struct{}
}

// ProfileData プロファイルデータ構造
type ProfileData struct {
	Timestamp     time.Time              `json:"timestamp"`
	CPUProfile    string                 `json:"cpu_profile,omitempty"`
	MemProfile    string                 `json:"mem_profile,omitempty"`
	GoroutineInfo *GoroutineInfo         `json:"goroutine_info"`
	MemStats      *runtime.MemStats      `json:"mem_stats"`
	Metrics       map[string]interface{} `json:"metrics"`
}

// GoroutineInfo Goroutine情報
type GoroutineInfo struct {
	Count    int `json:"count"`
	Running  int `json:"running"`
	Waiting  int `json:"waiting"`
	Sleeping int `json:"sleeping"`
	Blocked  int `json:"blocked"`
	Syscall  int `json:"syscall"`
}

// PerformanceAlert パフォーマンスアラート
type PerformanceAlert struct {
	Level     string                 `json:"level"`
	Message   string                 `json:"message"`
	Timestamp time.Time              `json:"timestamp"`
	Metrics   map[string]interface{} `json:"metrics"`
}

// NewProfilingSystem 新しいプロファイリングシステムを作成
func NewProfilingSystem(port int, profileInterval, dataRetention time.Duration) *ProfilingSystem {
	mux := http.NewServeMux()

	ps := &ProfilingSystem{
		httpServer: &http.Server{
			Addr:    fmt.Sprintf(":%d", port),
			Handler: mux,
		},
		profileInterval: profileInterval,
		dataRetention:   dataRetention,
		profiles:        make(map[string]*ProfileData),
		stopChan:        make(chan struct{}),
	}

	// カスタムエンドポイントを追加
	mux.HandleFunc("/health", ps.healthHandler)
	mux.HandleFunc("/metrics", ps.metricsHandler)
	mux.HandleFunc("/profiles", ps.profilesHandler)
	mux.HandleFunc("/alerts", ps.alertsHandler)

	return ps
}

// Start プロファイリング開始
func (ps *ProfilingSystem) StartProfiling(ctx context.Context) error {
	ps.running = true

	// HTTP pprofエンドポイント起動
	go func() {
		fmt.Printf("プロファイリングサーバー開始: %s\n", ps.httpServer.Addr)
		if err := ps.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("プロファイリングサーバーエラー: %v\n", err)
		}
	}()

	// 定期プロファイル収集
	go ps.profileCollectionLoop(ctx)

	// データクリーンアップ
	go ps.dataCleanupLoop(ctx)

	return nil
}

// Stop プロファイリング停止
func (ps *ProfilingSystem) Stop() error {
	ps.running = false
	close(ps.stopChan)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return ps.httpServer.Shutdown(ctx)
}

// profileCollectionLoop プロファイル収集ループ
func (ps *ProfilingSystem) profileCollectionLoop(ctx context.Context) {
	ticker := time.NewTicker(ps.profileInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
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
	count := runtime.NumGoroutine()

	// 詳細なGoroutine情報は実装簡略化のため基本情報のみ
	return &GoroutineInfo{
		Count:    count,
		Running:  count / 4, // 簡略化した推定値
		Waiting:  count / 4,
		Sleeping: count / 4,
		Blocked:  count / 8,
		Syscall:  count / 8,
	}
}

// collectCustomMetrics カスタムメトリクス収集
func (ps *ProfilingSystem) collectCustomMetrics() map[string]interface{} {
	return map[string]interface{}{
		"timestamp":       time.Now().Unix(),
		"cpu_count":       runtime.NumCPU(),
		"goroutine_count": runtime.NumGoroutine(),
		"cgo_calls":       runtime.NumCgoCall(),
	}
}

// checkPerformanceAlerts パフォーマンスアラート判定
func (ps *ProfilingSystem) checkPerformanceAlerts(data *ProfileData) {
	alerts := []PerformanceAlert{}

	// メモリ使用量アラート
	if data.MemStats.Alloc > 100*1024*1024 { // 100MB超過
		alerts = append(alerts, PerformanceAlert{
			Level:     "WARNING",
			Message:   "メモリ使用量が閾値を超過しています",
			Timestamp: data.Timestamp,
			Metrics: map[string]interface{}{
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
			Metrics: map[string]interface{}{
				"goroutine_count": data.GoroutineInfo.Count,
				"threshold":       1000,
			},
		})
	}

	// アラートがある場合はログ出力
	for _, alert := range alerts {
		fmt.Printf("ALERT [%s] %s: %s\n", alert.Level, alert.Timestamp.Format(time.RFC3339), alert.Message)
	}
}

// dataCleanupLoop データクリーンアップループ
func (ps *ProfilingSystem) dataCleanupLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
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
	status := map[string]interface{}{
		"status":    "healthy",
		"timestamp": time.Now().Unix(),
		"running":   ps.running,
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

	metrics := map[string]interface{}{
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
	ps.mutex.RLock()
	defer ps.mutex.RUnlock()

	// 最新の10件のプロファイルを返す
	profiles := make([]*ProfileData, 0, 10)
	count := 0

	for _, profile := range ps.profiles {
		if count >= 10 {
			break
		}
		profiles = append(profiles, profile)
		count++
	}

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
			Metrics: map[string]interface{}{
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
