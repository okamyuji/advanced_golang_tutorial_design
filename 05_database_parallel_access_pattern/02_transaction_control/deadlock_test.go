package main

import (
	"errors"
	"fmt"
	"testing"

	"github.com/lib/pq"
)

// TestIsDeadlockError ラップされたpq.ErrorをSQLSTATEで判定し、文字列が似ているだけのエラーは除くことを確かめます
func TestIsDeadlockError(t *testing.T) {
	tm := &TransactionManager{}
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"wrapped deadlock", fmt.Errorf("update failed: %w", &pq.Error{Code: "40P01", Message: "deadlock detected"}), true},
		{"serialization failure", &pq.Error{Code: "40001"}, false},
		{"plain text mentioning 40P01", errors.New("pq: deadlock detected (40P01)"), false},
		{"nil", nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tm.isDeadlockError(tt.err); got != tt.want {
				t.Errorf("isDeadlockError(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}
