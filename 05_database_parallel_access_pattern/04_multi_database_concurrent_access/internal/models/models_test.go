package models

import "testing"

func TestUserSearchRequest_SetDefaults(t *testing.T) {
	tests := []struct {
		name string
		in   UserSearchRequest
		want UserSearchRequest
	}{
		{"未指定は既定値", UserSearchRequest{},
			UserSearchRequest{Limit: 50, OrderBy: "created_at", OrderDir: "DESC"}},
		{"負の値は補正", UserSearchRequest{Limit: -1, Offset: -5},
			UserSearchRequest{Limit: 50, Offset: 0, OrderBy: "created_at", OrderDir: "DESC"}},
		{"上限を超える Limit は 10000", UserSearchRequest{Limit: 20000},
			UserSearchRequest{Limit: 10000, OrderBy: "created_at", OrderDir: "DESC"}},
		{"指定値はそのまま", UserSearchRequest{Limit: 10, Offset: 3, OrderBy: "age", OrderDir: "asc"},
			UserSearchRequest{Limit: 10, Offset: 3, OrderBy: "age", OrderDir: "asc"}},
	}
	for _, tt := range tests {
		got := tt.in
		got.SetDefaults()
		if got != tt.want {
			t.Errorf("%s: SetDefaults() = %+v, 期待値 %+v", tt.name, got, tt.want)
		}
	}
}
