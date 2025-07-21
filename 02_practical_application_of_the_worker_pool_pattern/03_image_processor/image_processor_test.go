package main

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestImageProcessor_Start(t *testing.T) {
	tempDir := "/tmp/test_image_processor"
	processor := NewImageProcessor(tempDir)
	defer func() {
		if err := os.RemoveAll(tempDir); err != nil {
			t.Logf("Failed to remove temp directory: %v", err)
		}
	}()

	err := processor.Start()
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	defer func() {
		if err := processor.Shutdown(5 * time.Second); err != nil {
			t.Logf("Failed to shutdown processor: %v", err)
		}
	}()

	// 一時ディレクトリが作成されていることを確認
	if _, err := os.Stat(tempDir); os.IsNotExist(err) {
		t.Error("Temp directory was not created")
	}
}

func TestImageProcessor_SubmitTask_CPUIntensive(t *testing.T) {
	tempDir := "/tmp/test_cpu_processor"
	processor := NewImageProcessor(tempDir)
	defer func() {
		if err := os.RemoveAll(tempDir); err != nil {
			t.Logf("Failed to remove temp directory: %v", err)
		}
	}()

	if err := processor.Start(); err != nil {
		t.Fatalf("Failed to start processor: %v", err)
	}
	defer func() {
		if err := processor.Shutdown(5 * time.Second); err != nil {
			t.Logf("Failed to shutdown processor: %v", err)
		}
	}()

	// テストファイルを作成
	testFile := filepath.Join(tempDir, "test_input.jpg")
	err := createTestFile(testFile, 1024*1024) // 1MB
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	task := ImageTask{
		ID:         1,
		InputPath:  testFile,
		OutputPath: filepath.Join(tempDir, "output.jpg"),
		Operation:  OperationCompress, // CPU集約的操作
		Quality:    80,
	}

	err = processor.SubmitTask(task)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// 処理完了を待つ
	time.Sleep(3 * time.Second)

	total, completed, failed, successRate := processor.GetStats()
	if total != 1 {
		t.Errorf("Expected 1 total task, got %d", total)
	}
	if completed != 1 {
		t.Errorf("Expected 1 completed task, got %d", completed)
	}
	if failed != 0 {
		t.Errorf("Expected 0 failed tasks, got %d", failed)
	}
	if successRate != 100.0 {
		t.Errorf("Expected 100%% success rate, got %.1f%%", successRate)
	}
}

func TestImageProcessor_SubmitTask_IOIntensive(t *testing.T) {
	tempDir := "/tmp/test_io_processor"
	processor := NewImageProcessor(tempDir)
	defer func() {
		if err := os.RemoveAll(tempDir); err != nil {
			t.Logf("Failed to remove temp directory: %v", err)
		}
	}()

	if err := processor.Start(); err != nil {
		t.Fatalf("Failed to start processor: %v", err)
	}
	defer func() {
		if err := processor.Shutdown(5 * time.Second); err != nil {
			t.Logf("Failed to shutdown processor: %v", err)
		}
	}()

	// テストファイルを作成
	testFile := filepath.Join(tempDir, "test_input.png")
	err := createTestFile(testFile, 2*1024*1024) // 2MB
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	task := ImageTask{
		ID:         1,
		InputPath:  testFile,
		OutputPath: filepath.Join(tempDir, "output.png"),
		Operation:  OperationConvert, // I/O集約的操作
	}

	err = processor.SubmitTask(task)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// 処理完了を待つ
	time.Sleep(2 * time.Second)

	total, completed, failed, _ := processor.GetStats()
	if total != 1 {
		t.Errorf("Expected 1 total task, got %d", total)
	}
	if completed != 1 {
		t.Errorf("Expected 1 completed task, got %d", completed)
	}
	if failed != 0 {
		t.Errorf("Expected 0 failed tasks, got %d", failed)
	}
}

func TestImageProcessor_FileNotFound(t *testing.T) {
	tempDir := "/tmp/test_error_processor"
	processor := NewImageProcessor(tempDir)
	defer func() {
		if err := os.RemoveAll(tempDir); err != nil {
			t.Logf("Failed to remove temp directory: %v", err)
		}
	}()

	if err := processor.Start(); err != nil {
		t.Fatalf("Failed to start processor: %v", err)
	}
	defer func() {
		if err := processor.Shutdown(5 * time.Second); err != nil {
			t.Logf("Failed to shutdown processor: %v", err)
		}
	}()

	task := ImageTask{
		ID:         1,
		InputPath:  "/nonexistent/file.jpg",
		OutputPath: filepath.Join(tempDir, "output.jpg"),
		Operation:  OperationResize,
	}

	err := processor.SubmitTask(task)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// エラー処理完了を待つ
	time.Sleep(1 * time.Second)

	total, completed, failed, _ := processor.GetStats()
	if total != 1 {
		t.Errorf("Expected 1 total task, got %d", total)
	}
	if completed != 0 {
		t.Errorf("Expected 0 completed tasks, got %d", completed)
	}
	if failed != 1 {
		t.Errorf("Expected 1 failed task, got %d", failed)
	}
}

func TestImageProcessor_FileSizeLimit(t *testing.T) {
	tempDir := "/tmp/test_size_processor"
	processor := NewImageProcessor(tempDir)
	processor.maxFileSize = 1024 * 1024 // 1MB制限
	defer func() {
		if err := os.RemoveAll(tempDir); err != nil {
			t.Logf("Failed to remove temp directory: %v", err)
		}
	}()

	if err := processor.Start(); err != nil {
		t.Fatalf("Failed to start processor: %v", err)
	}
	defer func() {
		if err := processor.Shutdown(5 * time.Second); err != nil {
			t.Logf("Failed to shutdown processor: %v", err)
		}
	}()

	// 制限を超えるサイズのファイルを作成
	testFile := filepath.Join(tempDir, "large_file.jpg")
	err := createTestFile(testFile, 2*1024*1024) // 2MB
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	task := ImageTask{
		ID:         1,
		InputPath:  testFile,
		OutputPath: filepath.Join(tempDir, "output.jpg"),
		Operation:  OperationResize,
	}

	err = processor.SubmitTask(task)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// エラー処理完了を待つ
	time.Sleep(1 * time.Second)

	total, completed, failed, _ := processor.GetStats()
	if total != 1 {
		t.Errorf("Expected 1 total task, got %d", total)
	}
	if completed != 0 {
		t.Errorf("Expected 0 completed tasks, got %d", completed)
	}
	if failed != 1 {
		t.Errorf("Expected 1 failed task, got %d", failed)
	}
}

func TestImageProcessor_MixedOperations(t *testing.T) {
	tempDir := "/tmp/test_mixed_processor"
	processor := NewImageProcessor(tempDir)
	defer func() {
		if err := os.RemoveAll(tempDir); err != nil {
			t.Logf("Failed to remove temp directory: %v", err)
		}
	}()

	if err := processor.Start(); err != nil {
		t.Fatalf("Failed to start processor: %v", err)
	}
	defer func() {
		if err := processor.Shutdown(10 * time.Second); err != nil {
			t.Logf("Failed to shutdown processor: %v", err)
		}
	}()

	// 複数のテストファイルを作成
	testFiles := []string{"test1.jpg", "test2.png", "test3.gif"}
	for _, filename := range testFiles {
		testFile := filepath.Join(tempDir, filename)
		err := createTestFile(testFile, 512*1024) // 512KB
		if err != nil {
			t.Fatalf("Failed to create test file %s: %v", filename, err)
		}
	}

	// 異なる操作のタスクを送信
	operations := []ImageOperation{
		OperationResize,    // CPU集約的
		OperationConvert,   // I/O集約的
		OperationCompress,  // CPU集約的
		OperationThumbnail, // I/O集約的
	}

	for i, op := range operations {
		task := ImageTask{
			ID:         i + 1,
			InputPath:  filepath.Join(tempDir, testFiles[i%len(testFiles)]),
			OutputPath: filepath.Join(tempDir, fmt.Sprintf("output_%d.jpg", i)),
			Operation:  op,
		}

		err := processor.SubmitTask(task)
		if err != nil {
			t.Fatalf("Expected no error for task %d, got %v", i+1, err)
		}
	}

	// 全タスクの処理完了を待つ
	time.Sleep(8 * time.Second)

	total, completed, failed, successRate := processor.GetStats()
	expectedTasks := int64(len(operations))

	if total != expectedTasks {
		t.Errorf("Expected %d total tasks, got %d", expectedTasks, total)
	}
	if completed != expectedTasks {
		t.Errorf("Expected %d completed tasks, got %d", expectedTasks, completed)
	}
	if failed != 0 {
		t.Errorf("Expected 0 failed tasks, got %d", failed)
	}
	if successRate != 100.0 {
		t.Errorf("Expected 100%% success rate, got %.1f%%", successRate)
	}
}
