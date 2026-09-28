package main

import (
	"strings"
	"testing"
)

func TestMemoryMonitor_CheckAndAlert(t *testing.T) {
	tests := []struct {
		name          string
		maxGoroutines int
		previousMB    float64
		wantSubstr    string
	}{
		{"goroutine threshold", 0, 0, "Goroutine count exceeded"},
		{"memory increase", 1 << 20, 1e-9, "Memory usage increased"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mm := NewMemoryMonitor(tt.maxGoroutines, 0.5)
			mm.previousMemoryMB = tt.previousMB
			var alerts []string
			mm.SetAlertCallback(func(msg string) { alerts = append(alerts, msg) })

			mm.checkAndAlert()

			if len(alerts) != 1 || !strings.Contains(alerts[0], tt.wantSubstr) {
				t.Errorf("expected one alert containing %q, got %q", tt.wantSubstr, alerts)
			}
		})
	}
}
