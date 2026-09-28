package main

import (
	"testing"
	"time"
)

func TestExecuteLoadTest_リクエストが0件でもpanicせずゼロの結果を返す(t *testing.T) {
	// grpc.NewClient 最初のRPCまで接続しないので、リクエスト0件ならサーバーは不要
	result := executeLoadTest(LoadTestConfig{
		ServerAddress:     "127.0.0.1:1",
		ConcurrentClients: 1,
		RequestsPerClient: 0,
		TestDuration:      time.Second,
	})

	if result.TotalRequests != 0 || result.AverageLatency != 0 {
		t.Fatalf("結果 = total:%d avg:%v, want 0/0", result.TotalRequests, result.AverageLatency)
	}
}

func TestExecuteLoadTest_クライアント数0でもpanicしない(t *testing.T) {
	result := executeLoadTest(LoadTestConfig{ServerAddress: "127.0.0.1:1", RampUpDuration: time.Second})

	if result.TotalRequests != 0 {
		t.Fatalf("TotalRequests = %d, want 0", result.TotalRequests)
	}
}
