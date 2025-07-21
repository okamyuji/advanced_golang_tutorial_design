package main

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestNewTaskConfig(t *testing.T) {
	config := NewTaskConfig()

	if config.DefaultTimeout <= 0 {
		t.Errorf("Expected positive DefaultTimeout, got %v", config.DefaultTimeout)
	}
	if config.MaxTimeout <= 0 {
		t.Errorf("Expected positive MaxTimeout, got %v", config.MaxTimeout)
	}
	if config.WorkerCount <= 0 {
		t.Errorf("Expected positive WorkerCount, got %d", config.WorkerCount)
	}
	if config.QueueSize <= 0 {
		t.Errorf("Expected positive QueueSize, got %d", config.QueueSize)
	}
	if config.MaxRetries < 0 {
		t.Errorf("Expected non-negative MaxRetries, got %d", config.MaxRetries)
	}
}

func TestNewTimeoutManager(t *testing.T) {
	defaultTimeout := 5 * time.Second
	maxTimeout := 30 * time.Second

	tm := NewTimeoutManager(defaultTimeout, maxTimeout)

	if tm == nil {
		t.Fatal("Expected TimeoutManager to be created")
	}
	if tm.defaultTimeout != defaultTimeout {
		t.Errorf("Expected defaultTimeout %v, got %v", defaultTimeout, tm.defaultTimeout)
	}
	if tm.maxTimeout != maxTimeout {
		t.Errorf("Expected maxTimeout %v, got %v", maxTimeout, tm.maxTimeout)
	}
	if tm.activeTimeouts == nil {
		t.Error("Expected activeTimeouts map to be initialized")
	}
}

func TestNewContextControlController(t *testing.T) {
	config := &TaskConfig{
		DefaultTimeout:  5 * time.Second,
		MaxTimeout:      30 * time.Second,
		WorkerCount:     2,
		QueueSize:       10,
		EnableMetrics:   true,
		MetricsInterval: 10 * time.Second,
		RetryEnabled:    true,
		MaxRetries:      3,
	}

	controller := NewContextControlController(config)

	if controller == nil {
		t.Fatal("Expected controller to be created")
	}
	if len(controller.workers) != config.WorkerCount {
		t.Errorf("Expected %d workers, got %d", config.WorkerCount, len(controller.workers))
	}
	if controller.workerCount != config.WorkerCount {
		t.Errorf("Expected workerCount %d, got %d", config.WorkerCount, controller.workerCount)
	}
	if controller.defaultTimeout != config.DefaultTimeout {
		t.Errorf("Expected defaultTimeout %v, got %v", config.DefaultTimeout, controller.defaultTimeout)
	}
}

func TestControllerStartAndShutdown(t *testing.T) {
	config := &TaskConfig{
		DefaultTimeout:  2 * time.Second,
		MaxTimeout:      10 * time.Second,
		WorkerCount:     2,
		QueueSize:       10,
		EnableMetrics:   false, // テストを簡素化
		MetricsInterval: 10 * time.Second,
		RetryEnabled:    true,
		MaxRetries:      1,
	}

	controller := NewContextControlController(config)

	// Start test
	err := controller.Start()
	if err != nil {
		t.Fatalf("Failed to start controller: %v", err)
	}

	// 重複起動のテスト
	err = controller.Start()
	if err == nil {
		t.Error("Expected error when starting already running controller")
	}

	// 少し待ってワーカーが開始されることを確認
	time.Sleep(10 * time.Millisecond)

	// Shutdown test
	err = controller.Shutdown(2 * time.Second)
	if err != nil {
		t.Fatalf("Failed to shutdown controller: %v", err)
	}

	// 重複停止のテスト
	err = controller.Shutdown(1 * time.Second)
	if err == nil {
		t.Error("Expected error when shutting down already stopped controller")
	}
}

func TestTaskSubmissionAndProcessing(t *testing.T) {
	config := &TaskConfig{
		DefaultTimeout:  5 * time.Second,
		MaxTimeout:      10 * time.Second,
		WorkerCount:     2,
		QueueSize:       100,
		EnableMetrics:   false,
		MetricsInterval: 10 * time.Second,
		RetryEnabled:    false, // リトライ無効でシンプルに
		MaxRetries:      0,
	}

	controller := NewContextControlController(config)

	err := controller.Start()
	if err != nil {
		t.Fatalf("Failed to start controller: %v", err)
	}
	defer func() {
		if err := controller.Shutdown(2 * time.Second); err != nil {
			t.Logf("Failed to shutdown controller: %v", err)
		}
	}()

	// 処理監視用
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		timeout := time.After(5 * time.Second)
		for {
			select {
			case <-timeout:
				return
			default:
				time.Sleep(10 * time.Millisecond)
				// 少し待って結果を確認
				stats := controller.GetStats()
				completed := stats["total_completed"].(int64)
				if completed >= 5 { // 5つのタスク完了を待つ
					return
				}
			}
		}
	}()

	// テストタスクを送信
	taskCount := 5
	for i := 0; i < taskCount; i++ {
		task := Task{
			ID:        int64(i),
			Name:      fmt.Sprintf("test_task_%d", i),
			Data:      []byte(fmt.Sprintf("test_data_%d", i)),
			Priority:  1,
			Timeout:   1 * time.Second,
			CreatedAt: time.Now(),
			Metadata:  map[string]interface{}{"test": true},
		}

		err := controller.SubmitTask(task)
		if err != nil {
			t.Errorf("Failed to submit task %d: %v", i, err)
		}
	}

	// 処理完了を待つ
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// 正常完了
	case <-time.After(10 * time.Second):
		t.Fatal("Timeout waiting for task processing")
	}

	// 統計を検証
	stats := controller.GetStats()
	submitted := stats["total_submitted"].(int64)
	completed := stats["total_completed"].(int64)

	if submitted != int64(taskCount) {
		t.Errorf("Expected %d submitted tasks, got %d", taskCount, submitted)
	}
	if completed < 1 {
		t.Errorf("Expected at least 1 completed task, got %d", completed)
	}
}

func TestTaskTimeout(t *testing.T) {
	config := &TaskConfig{
		DefaultTimeout:  100 * time.Millisecond, // 短いタイムアウト
		MaxTimeout:      1 * time.Second,
		WorkerCount:     1,
		QueueSize:       10,
		EnableMetrics:   false,
		MetricsInterval: 10 * time.Second,
		RetryEnabled:    false,
		MaxRetries:      0,
	}

	controller := NewContextControlController(config)

	err := controller.Start()
	if err != nil {
		t.Fatalf("Failed to start controller: %v", err)
	}
	defer func() {
		if err := controller.Shutdown(2 * time.Second); err != nil {
			t.Logf("Failed to shutdown controller: %v", err)
		}
	}()

	// 長時間かかるタスクを送信（タイムアウトするはず）
	task := Task{
		ID:      1,
		Name:    "timeout_test_task",
		Data:    []byte("timeout_test"),
		Timeout: 50 * time.Millisecond, // 非常に短いタイムアウト
	}

	err = controller.SubmitTask(task)
	if err != nil {
		t.Fatalf("Failed to submit task: %v", err)
	}

	// 少し待ってタイムアウトが発生することを確認
	time.Sleep(500 * time.Millisecond)

	stats := controller.GetStats()
	timeouts := stats["total_timeouts"].(int64)

	if timeouts < 1 {
		t.Errorf("Expected at least 1 timeout, got %d", timeouts)
	}
}

func TestTaskCancellation(t *testing.T) {
	config := &TaskConfig{
		DefaultTimeout:  5 * time.Second,
		MaxTimeout:      10 * time.Second,
		WorkerCount:     1,
		QueueSize:       10,
		EnableMetrics:   false,
		MetricsInterval: 10 * time.Second,
		RetryEnabled:    false,
		MaxRetries:      0,
	}

	controller := NewContextControlController(config)

	err := controller.Start()
	if err != nil {
		t.Fatalf("Failed to start controller: %v", err)
	}
	defer func() {
		if err := controller.Shutdown(2 * time.Second); err != nil {
			t.Logf("Failed to shutdown controller: %v", err)
		}
	}()

	// タスクを送信
	task := Task{
		ID:      100,
		Name:    "cancellation_test_task",
		Data:    []byte("cancellation_test"),
		Timeout: 10 * time.Second, // 長いタイムアウト
	}

	err = controller.SubmitTask(task)
	if err != nil {
		t.Fatalf("Failed to submit task: %v", err)
	}

	// 少し待ってからタスクをキャンセル
	time.Sleep(50 * time.Millisecond)

	cancelled := controller.CancelTask(task.ID)
	if !cancelled {
		t.Error("Expected task to be cancelled")
	}

	// 再度キャンセルを試行（すでにキャンセル済みなのでfalseを期待）
	cancelled = controller.CancelTask(task.ID)
	if cancelled {
		t.Error("Expected second cancellation to return false")
	}
}

func TestSubmitTaskWhenNotRunning(t *testing.T) {
	config := &TaskConfig{
		WorkerCount: 1,
		QueueSize:   10,
	}

	controller := NewContextControlController(config)

	task := Task{
		ID:   1,
		Name: "test",
	}

	err := controller.SubmitTask(task)
	if err == nil {
		t.Error("Expected error when submitting task to non-running controller")
	}
}

func TestSubmitTaskWithTimeout(t *testing.T) {
	config := &TaskConfig{
		DefaultTimeout:  5 * time.Second,
		MaxTimeout:      10 * time.Second,
		WorkerCount:     1,
		QueueSize:       1, // 非常に小さなキューサイズ
		EnableMetrics:   false,
		MetricsInterval: 10 * time.Second,
		RetryEnabled:    false,
		MaxRetries:      0,
	}

	controller := NewContextControlController(config)

	err := controller.Start()
	if err != nil {
		t.Fatalf("Failed to start controller: %v", err)
	}
	defer func() {
		if err := controller.Shutdown(2 * time.Second); err != nil {
			t.Logf("Failed to shutdown controller: %v", err)
		}
	}()

	// 長時間のタスクでワーカーをブロック
	longTask := Task{
		ID:      1,
		Name:    "long_running_task",
		Timeout: 3 * time.Second, // 長いタスク
	}
	if err := controller.SubmitTask(longTask); err != nil {
		t.Logf("Failed to submit long task: %v", err)
	} // ワーカーをブロック

	// 少し待ってワーカーがブロックされることを確認
	time.Sleep(50 * time.Millisecond)

	// キューを満杯にする
	fillTask := Task{
		ID:   2,
		Name: "queue_fill_task",
	}
	if err := controller.SubmitTask(fillTask); err != nil {
		t.Logf("Failed to submit fill task: %v", err)
	} // キューを満杯にする

	// タイムアウト付きで送信を試行
	task := Task{
		ID:   999,
		Name: "timeout_submit_test",
	}

	start := time.Now()
	err = controller.SubmitTaskWithTimeout(task, 100*time.Millisecond)
	elapsed := time.Since(start)

	// タイムアウトが発生することを期待
	if err == nil {
		t.Error("Expected timeout error when submitting to full queue")
	}

	// タイムアウト時間が適切であることを確認
	if elapsed > 200*time.Millisecond {
		t.Errorf("Timeout took too long: %v", elapsed)
	}
}

func TestGetCurrentTasks(t *testing.T) {
	config := &TaskConfig{
		DefaultTimeout:  2 * time.Second,
		MaxTimeout:      10 * time.Second,
		WorkerCount:     2,
		QueueSize:       10,
		EnableMetrics:   false,
		MetricsInterval: 10 * time.Second,
		RetryEnabled:    false,
		MaxRetries:      0,
	}

	controller := NewContextControlController(config)

	err := controller.Start()
	if err != nil {
		t.Fatalf("Failed to start controller: %v", err)
	}
	defer func() {
		if err := controller.Shutdown(2 * time.Second); err != nil {
			t.Logf("Failed to shutdown controller: %v", err)
		}
	}()

	// 最初は現在実行中のタスクはないはず
	currentTasks := controller.GetCurrentTasks()
	if len(currentTasks) != 0 {
		t.Errorf("Expected 0 current tasks, got %d", len(currentTasks))
	}

	// タスクを送信
	task := Task{
		ID:      1,
		Name:    "current_task_test",
		Timeout: 500 * time.Millisecond,
	}

	err = controller.SubmitTask(task)
	if err != nil {
		t.Fatalf("Failed to submit task: %v", err)
	}

	// 少し待ってタスクが実行されることを確認
	time.Sleep(50 * time.Millisecond)

	currentTasks = controller.GetCurrentTasks()
	// この時点では実行中のタスクがある可能性がある（タイミング依存）
	if len(currentTasks) > 2 {
		t.Errorf("Expected at most 2 current tasks, got %d", len(currentTasks))
	}
}

func TestControllerGetStats(t *testing.T) {
	config := &TaskConfig{
		DefaultTimeout:  5 * time.Second,
		MaxTimeout:      10 * time.Second,
		WorkerCount:     2,
		QueueSize:       10,
		EnableMetrics:   true,
		MetricsInterval: 10 * time.Second,
		RetryEnabled:    false,
		MaxRetries:      0,
	}

	controller := NewContextControlController(config)

	err := controller.Start()
	if err != nil {
		t.Fatalf("Failed to start controller: %v", err)
	}
	defer func() {
		if err := controller.Shutdown(2 * time.Second); err != nil {
			t.Logf("Failed to shutdown controller: %v", err)
		}
	}()

	// 統計を取得
	stats := controller.GetStats()

	// 統計の基本構造をテスト
	if stats == nil {
		t.Fatal("Expected stats to be returned")
	}

	expectedFields := []string{
		"total_submitted", "total_completed", "total_success", "total_errors",
		"total_timeouts", "total_cancellations", "average_process_time",
		"throughput_tasks_sec", "uptime", "worker_count", "worker_stats",
	}

	for _, field := range expectedFields {
		if _, exists := stats[field]; !exists {
			t.Errorf("Expected stats field '%s' to exist", field)
		}
	}

	workerCount := stats["worker_count"].(int)
	if workerCount != config.WorkerCount {
		t.Errorf("Expected worker_count %d, got %d", config.WorkerCount, workerCount)
	}
}

func TestTimeoutManagerOperations(t *testing.T) {
	tm := NewTimeoutManager(5*time.Second, 30*time.Second)

	// キャンセル関数をモック
	cancelled := false
	mockCancel := func() {
		cancelled = true
	}

	// タスクを登録
	taskID := int64(123)
	tm.registerTask(taskID, mockCancel)

	// タスクをキャンセル
	result := tm.cancelTask(taskID)
	if !result {
		t.Error("Expected cancelTask to return true")
	}
	if !cancelled {
		t.Error("Expected cancel function to be called")
	}

	// 既にキャンセル済みのタスクを再度キャンセル
	result = tm.cancelTask(taskID)
	if result {
		t.Error("Expected cancelTask to return false for already cancelled task")
	}

	// 存在しないタスクをキャンセル
	result = tm.cancelTask(999)
	if result {
		t.Error("Expected cancelTask to return false for non-existent task")
	}
}

func TestWorkerProcessTaskWithContext(t *testing.T) {
	config := &TaskConfig{
		DefaultTimeout:  5 * time.Second,
		MaxTimeout:      10 * time.Second,
		WorkerCount:     1,
		QueueSize:       10,
		EnableMetrics:   false,
		MetricsInterval: 10 * time.Second,
		RetryEnabled:    false,
		MaxRetries:      0,
	}

	controller := NewContextControlController(config)
	worker := controller.workers[0]

	task := Task{
		ID:        123,
		Name:      "test_task",
		Data:      []byte("test_data"),
		Timeout:   1 * time.Second,
		CreatedAt: time.Now(),
	}

	result := worker.processTaskWithContext(task)

	// 結果を検証
	if result.TaskID != task.ID {
		t.Errorf("Expected TaskID %d, got %d", task.ID, result.TaskID)
	}
	if result.WorkerID != worker.ID {
		t.Errorf("Expected WorkerID %d, got %d", worker.ID, result.WorkerID)
	}
	if result.Duration <= 0 {
		t.Errorf("Expected positive duration, got %v", result.Duration)
	}
}

func BenchmarkControllerTaskProcessing(b *testing.B) {
	config := &TaskConfig{
		DefaultTimeout:  30 * time.Second,
		MaxTimeout:      60 * time.Second,
		WorkerCount:     2,
		QueueSize:       1000,
		EnableMetrics:   false,
		MetricsInterval: 60 * time.Second,
		RetryEnabled:    false,
		MaxRetries:      0,
	}

	controller := NewContextControlController(config)

	err := controller.Start()
	if err != nil {
		b.Fatalf("Failed to start controller: %v", err)
	}
	defer func() {
		if err := controller.Shutdown(5 * time.Second); err != nil {
			b.Logf("Failed to shutdown controller: %v", err)
		}
	}()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		task := Task{
			ID:      int64(i),
			Name:    fmt.Sprintf("bench_task_%d", i),
			Data:    []byte(fmt.Sprintf("bench_data_%d", i)),
			Timeout: 10 * time.Second,
		}

		err := controller.SubmitTask(task)
		if err != nil {
			b.Errorf("Failed to submit task %d: %v", i, err)
		}
	}
}
