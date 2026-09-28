package adapters

import "testing"

func TestOracleInt64_ParsesNumberReturnedAsString(t *testing.T) {
	tests := []struct {
		name string
		in   any
		want int64
	}{
		{"文字列の数値", "10000", 10000},
		{"int64 はそのまま", int64(21), 21},
		{"数値でない文字列は0", "abc", 0},
		{"nil は0", nil, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := oracleInt64(tt.in); got != tt.want {
				t.Errorf("oracleInt64(%v) = %d, 期待値 %d", tt.in, got, tt.want)
			}
		})
	}
}
