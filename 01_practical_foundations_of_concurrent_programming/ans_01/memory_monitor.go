package main

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"sync"
	"time"
)

// MemoryMonitorはメモリとGoroutineの監視を行います
type MemoryMonitor struct {
	mu                      sync.RWMutex
	previousMemoryMB        float64
	previousGoroutines      int
	alertCallback           func(string)
	maxGoroutines           int
	memoryIncreaseThreshold float64
}

// AlertInfoはアラート情報を格納します
type AlertInfo struct {
	Timestamp     time.Time
	Type          string
	Message       string
	CurrentValue  float64
	PreviousValue float64
}

// NewMemoryMonitorは新しいメモリモニターを作成します
func NewMemoryMonitor(maxGoroutines int, memoryIncreaseThreshold float64) *MemoryMonitor {
	return &MemoryMonitor{
		maxGoroutines:           maxGoroutines,
		memoryIncreaseThreshold: memoryIncreaseThreshold,
		alertCallback: func(message string) {
			log.Printf("ALERT: %s", message)
		},
	}
}

// SetAlertCallbackはアラートコールバック関数を設定します
func (mm *MemoryMonitor) SetAlertCallback(callback func(string)) {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	mm.alertCallback = callback
}

// Startは監視を開始します
func (mm *MemoryMonitor) Start(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// 初期値を設定
	mm.updateMetrics()

	for {
		select {
		case <-ctx.Done():
			fmt.Println("Memory monitor stopping")
			return
		case <-ticker.C:
			mm.checkAndAlert()
		}
	}
}

// updateMetricsはメトリクスを更新します
func (mm *MemoryMonitor) updateMetrics() {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	mm.mu.Lock()
	defer mm.mu.Unlock()

	mm.previousMemoryMB = float64(memStats.Alloc) / 1024 / 1024
	mm.previousGoroutines = runtime.NumGoroutine()
}

// checkAndAlertはメトリクスをチェックしてアラートを発行します
func (mm *MemoryMonitor) checkAndAlert() {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	currentMemoryMB := float64(memStats.Alloc) / 1024 / 1024
	currentGoroutines := runtime.NumGoroutine()

	mm.mu.Lock()
	previousMemoryMB := mm.previousMemoryMB
	alertCallback := mm.alertCallback
	mm.mu.Unlock()

	// Goroutine数のチェック
	if currentGoroutines > mm.maxGoroutines {
		message := fmt.Sprintf("Goroutine count exceeded threshold: %d > %d",
			currentGoroutines, mm.maxGoroutines)
		alertCallback(message)
	}

	// メモリ使用量の増加率チェック
	if previousMemoryMB > 0 {
		increaseRate := (currentMemoryMB - previousMemoryMB) / previousMemoryMB
		if increaseRate > mm.memoryIncreaseThreshold {
			message := fmt.Sprintf("Memory usage increased by %.2f%%: %.2fMB -> %.2fMB",
				increaseRate*100, previousMemoryMB, currentMemoryMB)
			alertCallback(message)
		}
	}

	// 現在の値を出力
	fmt.Printf("Monitor: Goroutines=%d, Memory=%.2fMB\n", currentGoroutines, currentMemoryMB)

	// 前回の値を更新
	mm.mu.Lock()
	mm.previousMemoryMB = currentMemoryMB
	mm.previousGoroutines = currentGoroutines
	mm.mu.Unlock()
}

// GetCurrentMetricsは現在のメトリクスを取得します
func (mm *MemoryMonitor) GetCurrentMetrics() (int, float64) {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	return runtime.NumGoroutine(), float64(memStats.Alloc) / 1024 / 1024
}

// 使用例とテスト
func main() {
	// モニターを作成（最大100Goroutine、メモリ増加率50%でアラート）
	monitor := NewMemoryMonitor(100, 0.5)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// アラートコールバックを設定
	var alertMessages []string
	var alertMu sync.Mutex

	monitor.SetAlertCallback(func(message string) {
		alertMu.Lock()
		alertMessages = append(alertMessages, message)
		alertMu.Unlock()
		fmt.Printf("🚨 ALERT: %s\n", message)
	})

	// 監視を開始
	go monitor.Start(ctx, 1*time.Second)

	// 大量のGoroutineを作成してアラートをテスト
	go func() {
		time.Sleep(3 * time.Second)
		fmt.Println("Creating many goroutines...")

		for i := 0; i < 150; i++ {
			go func() {
				time.Sleep(10 * time.Second)
			}()
		}
	}()

	// メモリを大量に確保してアラートをテスト
	go func() {
		time.Sleep(6 * time.Second)
		fmt.Println("Allocating large memory...")

		// 大量のメモリを確保
		data := make([][]byte, 1000)
		for i := range data {
			data[i] = make([]byte, 1024*1024) // 1MB
		}

		// メモリを保持
		time.Sleep(5 * time.Second)
		_ = data
	}()

	// 完了を待機
	<-ctx.Done()

	// アラート数を確認
	alertMu.Lock()
	fmt.Printf("Total alerts generated: %d\n", len(alertMessages))
	for i, msg := range alertMessages {
		fmt.Printf("Alert %d: %s\n", i+1, msg)
	}
	alertMu.Unlock()
}
