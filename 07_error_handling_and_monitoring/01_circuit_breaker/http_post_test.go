package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestHTTPClientWrapper_PostSendsBody Postに渡したbodyがそのままサーバーに届くことを確かめます
func TestHTTPClientWrapper_PostSendsBody(t *testing.T) {
	var got string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		got = string(b)
	}))
	defer server.Close()

	w := NewHTTPClientWrapper(server.Client(), NewCircuitBreaker(Settings{Name: "post"}))
	resp, err := w.Post(server.URL, "application/json", strings.NewReader(`{"id":1}`))
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()

	if got != `{"id":1}` {
		t.Fatalf("server received %q, want %q", got, `{"id":1}`)
	}
}
