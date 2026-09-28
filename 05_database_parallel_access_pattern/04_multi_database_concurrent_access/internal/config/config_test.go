package config

import "testing"

func TestExpandEnvVars_UsesEnvThenDefault(t *testing.T) {
	t.Setenv("MULTIDB_TEST_HOST", "db.example")
	t.Setenv("MULTIDB_TEST_EMPTY", "")

	tests := []struct {
		in, want string
	}{
		{"host: ${MULTIDB_TEST_HOST:localhost}", "host: db.example"},
		{"host: ${MULTIDB_TEST_EMPTY:localhost}", "host: localhost"},
		{"host: ${MULTIDB_TEST_UNSET:localhost}", "host: localhost"},
		{"host: ${MULTIDB_TEST_UNSET}", "host: "},
		{"url: ${MULTIDB_TEST_UNSET:a:b}", "url: a:b"},
	}
	for _, tt := range tests {
		if got := expandEnvVars(tt.in); got != tt.want {
			t.Errorf("expandEnvVars(%q) = %q, 期待値 %q", tt.in, got, tt.want)
		}
	}
}
