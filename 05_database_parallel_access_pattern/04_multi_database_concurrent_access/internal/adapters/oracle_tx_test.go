package adapters

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"
)

// defaultIsolationOnlyDriver は go-ora v3 と同じく、既定以外の分離レベルを拒む（go-ora v3 connection.go の BeginTx）。
type defaultIsolationOnlyDriver struct{}

type defaultIsolationOnlyConn struct{ driver.Conn }

type noopTx struct{}

func (noopTx) Commit() error   { return nil }
func (noopTx) Rollback() error { return nil }

func (defaultIsolationOnlyDriver) Open(string) (driver.Conn, error) {
	return defaultIsolationOnlyConn{}, nil
}

func (defaultIsolationOnlyConn) Close() error { return nil }

func (defaultIsolationOnlyConn) BeginTx(_ context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if opts.Isolation != driver.IsolationLevel(sql.LevelDefault) {
		return nil, errors.New("only support default value for isolation")
	}
	return noopTx{}, nil
}

func init() {
	sql.Register("default-isolation-only", defaultIsolationOnlyDriver{})
}

func TestOracleTransaction_UsesDefaultIsolation(t *testing.T) {
	db, err := sql.Open("default-isolation-only", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	o := &OracleAdapter{db: db}

	called := false
	err = o.Transaction(context.Background(), func(*sql.Tx) error {
		called = true
		return nil
	})
	if err != nil || !called {
		t.Fatalf("Transaction() = %v, fn called = %v; Oracle の既定の分離レベル（READ COMMITTED）で開始できること", err, called)
	}
}
