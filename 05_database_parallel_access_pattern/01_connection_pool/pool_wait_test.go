package main

import (
	"database/sql"
	"testing"
	"time"
)

// TestConnWaitSince 累計値の差分だけを返し、待ちが増えていなければ0になることを確かめます
func TestConnWaitSince(t *testing.T) {
	first := sql.DBStats{WaitCount: 5, WaitDuration: 300 * time.Millisecond}
	second := sql.DBStats{WaitCount: 5, WaitDuration: 300 * time.Millisecond}
	third := sql.DBStats{WaitCount: 8, WaitDuration: 450 * time.Millisecond}

	if n, d := connWaitSince(first, second); n != 0 || d != 0 {
		t.Errorf("no new waits: got (%d, %v), want (0, 0)", n, d)
	}
	if n, d := connWaitSince(second, third); n != 3 || d != 150*time.Millisecond {
		t.Errorf("new waits: got (%d, %v), want (3, 150ms)", n, d)
	}
	if n, d := connWaitSince(sql.DBStats{}, first); n != 5 || d != 300*time.Millisecond {
		t.Errorf("first check: got (%d, %v), want (5, 300ms)", n, d)
	}
}
