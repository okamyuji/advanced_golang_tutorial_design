package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"multi-db/internal/manager"
)

func TestPlaceholder_UsesDriverSpecificSyntax(t *testing.T) {
	tests := []struct {
		dbType string
		want   string
	}{
		{"PostgreSQL", "$1"},
		{"MySQL", "?"},
		{"MSSQL", "?"},
		{"Oracle", ":1"},
	}
	for _, tt := range tests {
		if got := placeholder(tt.dbType, 1); got != tt.want {
			t.Errorf("placeholder(%q, 1) = %q, 期待値 %q", tt.dbType, got, tt.want)
		}
	}
}

func TestSetupRoutes_MethodPatterns(t *testing.T) {
	dbManager = manager.NewConcurrentDBManager(manager.WorkerPoolConfig{MaxWorkers: 2})
	t.Cleanup(func() {
		if err := dbManager.Close(); err != nil {
			t.Errorf("Close が失敗した: %v", err)
		}
	})
	router := setupRoutes()

	tests := []struct {
		name   string
		method string
		path   string
		want   int
	}{
		{"プリフライトは CORS ヘッダー付きで 200", http.MethodOptions, "/api/v1/users/1", http.StatusOK},
		{"メソッド違いは 405", http.MethodPut, "/api/v1/health", http.StatusMethodNotAllowed},
		{"数値でない ID は 400", http.MethodGet, "/api/v1/users/abc", http.StatusBadRequest},
		{"DB 未登録なら 500", http.MethodGet, "/api/v1/users/1", http.StatusInternalServerError},
		{"任意 SQL エンドポイントは 410", http.MethodPost, "/api/v1/databases/query", http.StatusGone},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, httptest.NewRequest(tt.method, tt.path, nil))

			if rec.Code != tt.want {
				t.Errorf("%s %s = %d, 期待値 %d", tt.method, tt.path, rec.Code, tt.want)
			}
			if tt.method == http.MethodOptions && rec.Header().Get("Access-Control-Allow-Origin") != "*" {
				t.Error("プリフライト応答に CORS ヘッダーがない")
			}
		})
	}
}
