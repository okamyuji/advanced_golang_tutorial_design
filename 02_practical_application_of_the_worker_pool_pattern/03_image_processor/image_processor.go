package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// ImageTaskは画像処理タスクを表現します
type ImageTask struct {
	ID         int
	InputPath  string
	OutputPath string
	Operation  ImageOperation
	Quality    int
	Width      int
	Height     int
	Priority   int
}

// ImageOperationは画像処理操作を定義します
type ImageOperation int

const (
	OperationResize ImageOperation = iota
	OperationCompress
	OperationConvert
	OperationThumbnail
)

// ImageResultは画像処理結果を表現します
type ImageResult struct {
	TaskID     int
	Success    bool
	Error      error
	InputSize  int64
	OutputSize int64
	Duration   time.Duration
	WorkerID   int
}

// ImageProcessorは画像処理専用のワーカープールです
type ImageProcessor struct {
	// ワーカー設定
	cpuWorkers int
	ioWorkers  int

	// キュー
	cpuQueue    chan ImageTask // CPU集約的処理用
	ioQueue     chan ImageTask // I/O集約的処理用
	resultQueue chan ImageResult

	// 制御
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// 統計
	stats *ProcessingStats

	// 設定
	tempDir     string
	maxFileSize int64
}

// ProcessingStatsは処理統計を管理します
type ProcessingStats struct {
	mu                 sync.RWMutex
	totalTasks         int64
	completedTasks     int64
	failedTasks        int64
	totalInputSize     int64
	totalOutputSize    int64
	averageProcessTime time.Duration
	operationCounts    map[ImageOperation]int64
}

// NewImageProcessorは新しい画像処理プールを作成します
func NewImageProcessor(tempDir string) *ImageProcessor {
	ctx, cancel := context.WithCancel(context.Background())

	// CPU集約的処理はCPUコア数、I/O処理は多めに設定
	cpuWorkers := runtime.NumCPU()
	ioWorkers := cpuWorkers * 2

	return &ImageProcessor{
		cpuWorkers:  cpuWorkers,
		ioWorkers:   ioWorkers,
		cpuQueue:    make(chan ImageTask, cpuWorkers*2),
		ioQueue:     make(chan ImageTask, ioWorkers*2),
		resultQueue: make(chan ImageResult, cpuWorkers+ioWorkers),
		ctx:         ctx,
		cancel:      cancel,
		stats: &ProcessingStats{
			operationCounts: make(map[ImageOperation]int64),
		},
		tempDir:     tempDir,
		maxFileSize: 100 * 1024 * 1024, // 100MB制限
	}
}

// Startは画像処理プールを開始します
func (ip *ImageProcessor) Start() error {
	// 一時ディレクトリを作成
	if err := os.MkdirAll(ip.tempDir, 0755); err != nil {
		return fmt.Errorf("failed to create temp directory: %v", err)
	}

	// CPUワーカーを開始
	for i := 0; i < ip.cpuWorkers; i++ {
		ip.wg.Add(1)
		go ip.cpuWorker(i)
	}

	// I/Oワーカーを開始
	for i := 0; i < ip.ioWorkers; i++ {
		ip.wg.Add(1)
		go ip.ioWorker(i)
	}

	// 結果処理を開始
	ip.wg.Add(1)
	go ip.resultHandler()

	// 統計レポートを開始
	ip.wg.Add(1)
	go ip.statsReporter()

	log.Printf("Image processor started: %d CPU workers, %d I/O workers",
		ip.cpuWorkers, ip.ioWorkers)
	return nil
}

// cpuWorkerはCPU集約的な画像処理を実行します
func (ip *ImageProcessor) cpuWorker(workerID int) {
	defer ip.wg.Done()

	for {
		select {
		case <-ip.ctx.Done():
			return
		case task, ok := <-ip.cpuQueue:
			if !ok {
				return
			}

			result := ip.processCPUTask(task, workerID)

			select {
			case ip.resultQueue <- result:
			case <-ip.ctx.Done():
				return
			}
		}
	}
}

// ioWorkerはI/O集約的な画像処理を実行します
func (ip *ImageProcessor) ioWorker(workerID int) {
	defer ip.wg.Done()

	for {
		select {
		case <-ip.ctx.Done():
			return
		case task, ok := <-ip.ioQueue:
			if !ok {
				return
			}

			result := ip.processIOTask(task, workerID+1000) // IDを区別

			select {
			case ip.resultQueue <- result:
			case <-ip.ctx.Done():
				return
			}
		}
	}
}

// processCPUTaskはCPU集約的タスクを処理します
func (ip *ImageProcessor) processCPUTask(task ImageTask, workerID int) ImageResult {
	start := time.Now()

	result := ImageResult{
		TaskID:   task.ID,
		WorkerID: workerID,
	}

	// ファイルサイズをチェック
	inputInfo, err := os.Stat(task.InputPath)
	if err != nil {
		result.Error = fmt.Errorf("failed to stat input file: %v", err)
		result.Duration = time.Since(start)
		return result
	}

	if inputInfo.Size() > ip.maxFileSize {
		result.Error = fmt.Errorf("file too large: %d bytes", inputInfo.Size())
		result.Duration = time.Since(start)
		return result
	}

	result.InputSize = inputInfo.Size()

	// 実際の画像処理をシミュレート（実際の実装では画像ライブラリを使用）
	processingTime := ip.simulateProcessing(task.Operation, inputInfo.Size())

	select {
	case <-time.After(processingTime):
		// 処理完了
	case <-ip.ctx.Done():
		result.Error = fmt.Errorf("processing cancelled")
		result.Duration = time.Since(start)
		return result
	}

	// 出力ファイルサイズをシミュレート
	result.OutputSize = int64(float64(result.InputSize) * ip.getCompressionRatio(task.Operation))
	result.Success = true
	result.Duration = time.Since(start)

	log.Printf("CPU Worker %d: Processed task %d (%s) in %v",
		workerID, task.ID, ip.getOperationName(task.Operation), result.Duration)

	return result
}

// processIOTaskはI/O集約的タスクを処理します
func (ip *ImageProcessor) processIOTask(task ImageTask, workerID int) ImageResult {
	start := time.Now()

	result := ImageResult{
		TaskID:   task.ID,
		WorkerID: workerID,
	}

	// ファイル読み込みをシミュレート
	inputInfo, err := os.Stat(task.InputPath)
	if err != nil {
		result.Error = fmt.Errorf("failed to read input file: %v", err)
		result.Duration = time.Since(start)
		return result
	}

	result.InputSize = inputInfo.Size()

	// I/O処理時間をシミュレート（ファイルサイズに比例）
	ioTime := time.Duration(float64(inputInfo.Size())/1024/1024) * 10 * time.Millisecond

	select {
	case <-time.After(ioTime):
		// I/O完了
	case <-ip.ctx.Done():
		result.Error = fmt.Errorf("I/O operation cancelled")
		result.Duration = time.Since(start)
		return result
	}

	// 出力をシミュレート
	result.OutputSize = result.InputSize // I/O操作ではサイズ変更なし
	result.Success = true
	result.Duration = time.Since(start)

	log.Printf("I/O Worker %d: Processed task %d in %v",
		workerID, task.ID, result.Duration)

	return result
}

// simulateProcessingは処理時間をシミュレートします
func (ip *ImageProcessor) simulateProcessing(operation ImageOperation, fileSize int64) time.Duration {
	baseTime := time.Duration(float64(fileSize)/1024/1024) * 50 * time.Millisecond

	switch operation {
	case OperationResize:
		return baseTime * 2 // リサイズは重い
	case OperationCompress:
		return baseTime * 3 // 圧縮はさらに重い
	case OperationConvert:
		return baseTime * 1 // 変換は標準
	case OperationThumbnail:
		return baseTime / 2 // サムネイルは軽い
	default:
		return baseTime
	}
}

// getCompressionRatioは圧縮比率を取得します
func (ip *ImageProcessor) getCompressionRatio(operation ImageOperation) float64 {
	switch operation {
	case OperationCompress:
		return 0.3 // 70%圧縮
	case OperationThumbnail:
		return 0.1 // 90%削減
	case OperationConvert:
		return 0.8 // 20%削減
	default:
		return 1.0 // サイズ変更なし
	}
}

// getOperationNameは操作名を取得します
func (ip *ImageProcessor) getOperationName(operation ImageOperation) string {
	switch operation {
	case OperationResize:
		return "resize"
	case OperationCompress:
		return "compress"
	case OperationConvert:
		return "convert"
	case OperationThumbnail:
		return "thumbnail"
	default:
		return "unknown"
	}
}

// resultHandlerは結果を処理します
func (ip *ImageProcessor) resultHandler() {
	defer ip.wg.Done()

	for {
		select {
		case <-ip.ctx.Done():
			return
		case result, ok := <-ip.resultQueue:
			if !ok {
				return
			}

			ip.updateStats(result)
		}
	}
}

// updateStatsは統計を更新します
func (ip *ImageProcessor) updateStats(result ImageResult) {
	ip.stats.mu.Lock()
	defer ip.stats.mu.Unlock()

	atomic.AddInt64(&ip.stats.totalTasks, 1)

	if result.Success {
		atomic.AddInt64(&ip.stats.completedTasks, 1)
		atomic.AddInt64(&ip.stats.totalInputSize, result.InputSize)
		atomic.AddInt64(&ip.stats.totalOutputSize, result.OutputSize)
	} else {
		atomic.AddInt64(&ip.stats.failedTasks, 1)
	}

	// 平均処理時間を更新
	total := atomic.LoadInt64(&ip.stats.totalTasks)
	ip.stats.averageProcessTime = time.Duration(
		(int64(ip.stats.averageProcessTime)*total + int64(result.Duration)) / (total + 1))
}

// statsReporterは統計を定期的に報告します
func (ip *ImageProcessor) statsReporter() {
	defer ip.wg.Done()

	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ip.ctx.Done():
			return
		case <-ticker.C:
			ip.printStats()
		}
	}
}

// printStatsは統計を出力します
func (ip *ImageProcessor) printStats() {
	ip.stats.mu.RLock()
	defer ip.stats.mu.RUnlock()

	total := atomic.LoadInt64(&ip.stats.totalTasks)
	completed := atomic.LoadInt64(&ip.stats.completedTasks)
	failed := atomic.LoadInt64(&ip.stats.failedTasks)
	inputSize := atomic.LoadInt64(&ip.stats.totalInputSize)
	outputSize := atomic.LoadInt64(&ip.stats.totalOutputSize)

	cpuQueueLen := len(ip.cpuQueue)
	ioQueueLen := len(ip.ioQueue)

	compressionRatio := float64(0)
	if inputSize > 0 {
		compressionRatio = float64(outputSize) / float64(inputSize) * 100
	}

	log.Printf("Processing Stats: Total=%d, Completed=%d, Failed=%d, "+
		"CPU Queue=%d, I/O Queue=%d, Compression=%.1f%%, AvgTime=%v",
		total, completed, failed, cpuQueueLen, ioQueueLen,
		compressionRatio, ip.stats.averageProcessTime)
}

// SubmitTaskはタスクを適切なキューに追加します
func (ip *ImageProcessor) SubmitTask(task ImageTask) error {
	var targetQueue chan ImageTask

	// 操作種別に応じてキューを選択
	switch task.Operation {
	case OperationResize, OperationCompress:
		targetQueue = ip.cpuQueue // CPU集約的
	case OperationConvert, OperationThumbnail:
		targetQueue = ip.ioQueue // I/O集約的
	default:
		targetQueue = ip.cpuQueue
	}

	select {
	case targetQueue <- task:
		ip.stats.mu.Lock()
		ip.stats.operationCounts[task.Operation]++
		ip.stats.mu.Unlock()
		return nil
	case <-ip.ctx.Done():
		return fmt.Errorf("processor is shutting down")
	default:
		return fmt.Errorf("queue is full")
	}
}

// Shutdownはプロセッサーを停止します
func (ip *ImageProcessor) Shutdown(timeout time.Duration) error {
	log.Println("Starting image processor shutdown...")

	// 新しいタスクの受付を停止
	close(ip.cpuQueue)
	close(ip.ioQueue)

	// ワーカーに停止シグナルを送信
	ip.cancel()

	// 完了を待機
	done := make(chan struct{})
	go func() {
		ip.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("Image processor shutdown completed")
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("shutdown timeout exceeded")
	}
}

// GetStatsは現在の統計を取得します
func (ip *ImageProcessor) GetStats() (int64, int64, int64, float64) {
	total := atomic.LoadInt64(&ip.stats.totalTasks)
	completed := atomic.LoadInt64(&ip.stats.completedTasks)
	failed := atomic.LoadInt64(&ip.stats.failedTasks)

	successRate := float64(0)
	if total > 0 {
		successRate = float64(completed) / float64(total) * 100
	}

	return total, completed, failed, successRate
}

func main() {
	// 一時ディレクトリを作成
	tempDir := "/tmp/image_processor"

	// 画像処理プールを作成
	processor := NewImageProcessor(tempDir)

	// プロセッサーを開始
	if err := processor.Start(); err != nil {
		log.Fatalf("Failed to start image processor: %v", err)
	}

	// テスト用の画像ファイル情報を作成
	testFiles := []struct {
		path      string
		operation ImageOperation
		size      int64
	}{
		{"/tmp/test1.jpg", OperationResize, 1024 * 1024},
		{"/tmp/test2.png", OperationCompress, 2048 * 1024},
		{"/tmp/test3.gif", OperationConvert, 512 * 1024},
		{"/tmp/test4.bmp", OperationThumbnail, 4096 * 1024},
	}

	// テストファイルを作成
	for _, file := range testFiles {
		if err := createTestFile(file.path, file.size); err != nil {
			log.Printf("Failed to create test file %s: %v", file.path, err)
		}
	}

	// タスクを送信
	go func() {
		for i := 1; i <= 20; i++ {
			file := testFiles[i%len(testFiles)]
			task := ImageTask{
				ID:         i,
				InputPath:  file.path,
				OutputPath: filepath.Join(tempDir, fmt.Sprintf("output_%d.jpg", i)),
				Operation:  file.operation,
				Quality:    80,
				Width:      1024,
				Height:     768,
				Priority:   i % 3,
			}

			if err := processor.SubmitTask(task); err != nil {
				log.Printf("Failed to submit task %d: %v", i, err)
			}

			time.Sleep(200 * time.Millisecond)
		}
	}()

	// 30秒間動作させる
	time.Sleep(30 * time.Second)

	// 最終統計を表示
	total, completed, failed, successRate := processor.GetStats()
	log.Printf("Final Results: Total=%d, Completed=%d, Failed=%d, Success Rate=%.1f%%",
		total, completed, failed, successRate)

	// シャットダウン
	if err := processor.Shutdown(10 * time.Second); err != nil {
		log.Printf("Shutdown error: %v", err)
	}

	// テストファイルをクリーンアップ
	for _, file := range testFiles {
		if err := os.Remove(file.path); err != nil {
			log.Printf("Failed to remove test file %s: %v", file.path, err)
		}
	}
	if err := os.RemoveAll(tempDir); err != nil {
		log.Printf("Failed to remove temp directory %s: %v", tempDir, err)
	}
}

// createTestFile はテスト用ファイルを作成します
func createTestFile(path string, size int64) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() {
		if err := file.Close(); err != nil {
			log.Printf("Failed to close file %s: %v", path, err)
		}
	}()

	// 指定サイズのダミーデータを書き込み
	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	written := int64(0)
	for written < size {
		n, err := file.Write(data)
		if err != nil {
			return err
		}
		written += int64(n)
	}

	return nil
}
