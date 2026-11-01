package config

import (
	"testing"

	"github.com/sijms/go-ora/v3/configurations"
)

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

// go-ora v3.0.1 の FastLogin は、2 本目以降の接続で Oracle 23c に EOF で切られる（sijms/go-ora#732）
func TestGetDSN_OracleDisablesFastLogin(t *testing.T) {
	d := &DatabaseConfig{Host: "localhost", Port: 1521, Database: "FREEPDB1", Username: "u", Password: "p"}

	cfg, err := configurations.ParseConfig(d.GetDSN("oracle"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.FastLogin {
		t.Fatal("FastLogin must be disabled for go-ora v3.0.1")
	}
}
