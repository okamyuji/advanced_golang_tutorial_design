package main

import "testing"

// TestZScoreExceeds 標準偏差0の指標を異常と判定せず、3σを超えたときだけ真になることを確かめます
func TestZScoreExceeds(t *testing.T) {
	tests := []struct {
		name                string
		value, mean, stdDev float64
		want                bool
	}{
		{"学習中に変化がなかった指標", 51, 50, 0, false},
		{"3σちょうど", 80, 50, 10, false},
		{"3σを超える", 81, 50, 10, true},
		{"負の方向に3σを超える", 19, 50, 10, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := zScoreExceeds(tt.value, tt.mean, tt.stdDev); got != tt.want {
				t.Errorf("zScoreExceeds(%v, %v, %v) = %v, want %v", tt.value, tt.mean, tt.stdDev, got, tt.want)
			}
		})
	}
}
