package main

import (
	"bytes"
	"io"
	"net"
	"net/http"
	"testing"
	"time"
)

// TestServesUnencryptedHTTP2 平文のHTTP/2（h2c）で応答し、1MBのボディも欠けずに届くことを確かめます
func TestServesUnencryptedHTTP2(t *testing.T) {
	config := NewServerConfig()
	config.Port = 0
	server := NewConcurrentHTTPServer(config)
	if err := server.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := server.Shutdown(5 * time.Second); err != nil {
			t.Logf("shutdown: %v", err)
		}
	}()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = server.server.Serve(ln) }()

	// HTTP/1を無効にしたクライアントは、http://のURLにh2cで接続する
	var protocols http.Protocols
	protocols.SetUnencryptedHTTP2(true)
	client := &http.Client{Transport: &http.Transport{Protocols: &protocols}, Timeout: 5 * time.Second}

	body := bytes.Repeat([]byte("x"), 1<<20)
	resp, err := client.Post("http://"+ln.Addr().String()+"/api/echo", "text/plain", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.ProtoMajor != 2 {
		t.Fatalf("Proto = %s, want HTTP/2", resp.Proto)
	}
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, body = %.200s", resp.StatusCode, got)
	}
	if n := bytes.Count(got, []byte("x")); n < len(body) {
		t.Fatalf("echo returned %d of %d body bytes", n, len(body))
	}
}
