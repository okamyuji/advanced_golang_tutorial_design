package manager

import (
	"context"
	"database/sql"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"multi-db/internal/adapters"
	"multi-db/internal/config"
	"multi-db/internal/models"
)

// fakeAdapter ネットワークを使わない DBAdapter の代役
type fakeAdapter struct {
	dbType string
	// failFirst 最初の failFirst 回の Query を再試行可能なエラーで失敗させる
	failFirst int64
	calls     atomic.Int64
	closed    atomic.Bool
	// tx Transactionがfnへ渡すトランザクション（中身は使わず同一性だけを確かめる）
	tx *sql.Tx
}

func (f *fakeAdapter) Connect(context.Context, config.DatabaseConfig) error { return nil }

func (f *fakeAdapter) Query(ctx context.Context, _ string, _ ...any) (*adapters.QueryResult, error) {
	if f.closed.Load() {
		return nil, errors.New("closed")
	}
	if n := f.calls.Add(1); n <= f.failFirst {
		return &adapters.QueryResult{Error: errors.New("connection reset")}, nil
	}
	return &adapters.QueryResult{Data: []map[string]any{{"ok": true}}, DBType: f.dbType}, nil
}

func (f *fakeAdapter) Execute(context.Context, string, ...any) (sql.Result, error) { return nil, nil }
func (f *fakeAdapter) Transaction(_ context.Context, fn func(*sql.Tx) error) error { return fn(f.tx) }
func (f *fakeAdapter) Close() error                                                { f.closed.Store(true); return nil }
func (f *fakeAdapter) Ping(context.Context) error                                  { return nil }
func (f *fakeAdapter) GetStats() sql.DBStats                                       { return sql.DBStats{} }
func (f *fakeAdapter) GetType() string                                             { return f.dbType }

func (f *fakeAdapter) CreateUser(_ context.Context, u *models.User) (*models.User, error) {
	u.ID = 1
	return u, nil
}

// searchCreatedAt fakeAdapter.GetUsersByAgeRange が返す作成日時
var searchCreatedAt = time.Date(2026, 9, 28, 22, 42, 54, 0, time.UTC)

func (f *fakeAdapter) GetUsersByAgeRange(context.Context, int, int) ([]*models.User, error) {
	return []*models.User{{ID: 7, Name: "n", Email: "e@example.com", Age: 30, CreatedAt: searchCreatedAt}}, nil
}

func (f *fakeAdapter) BulkInsertUsers(context.Context, []*models.User) error { return nil }

// newTestManager 指定した種別の fakeAdapter を登録したマネージャーを返す。テスト終了時に Close する
func newTestManager(t *testing.T, dbTypes ...string) (*ConcurrentDBManager, map[string]*fakeAdapter) {
	t.Helper()
	cm := NewConcurrentDBManager(WorkerPoolConfig{MaxWorkers: 4})
	t.Cleanup(func() {
		if err := cm.Close(); err != nil {
			t.Errorf("Close が失敗した: %v", err)
		}
	})
	fakes := make(map[string]*fakeAdapter)
	for _, dbType := range dbTypes {
		f := &fakeAdapter{dbType: dbType}
		fakes[dbType] = f
		cm.adapters[dbType] = f
	}
	return cm, fakes
}

func TestHealthCheck_PingsAllAdaptersConcurrently(t *testing.T) {
	cm, _ := newTestManager(t, "A", "B", "C", "D")

	results := cm.HealthCheck(t.Context())

	if len(results) != 4 {
		t.Fatalf("結果の件数 = %d, 期待値 4", len(results))
	}
	for dbType, err := range results {
		if err != nil {
			t.Errorf("%s: 想定外のエラー %v", dbType, err)
		}
	}
}

func TestCreateUser_CountsMetricsOnce(t *testing.T) {
	cm, _ := newTestManager(t, "A")

	result := <-cm.CreateUser(t.Context(), "A", &models.User{Name: "n", Email: "e@example.com"})
	if result.Error != nil {
		t.Fatalf("CreateUser が失敗した: %v", result.Error)
	}

	m := cm.GetMetrics()
	if m.TotalTasks != 1 || m.SuccessTasks != 1 || m.FailedTasks != 0 {
		t.Errorf("メトリクス = %+v, 期待値 Total=1 Success=1 Failed=0", m)
	}
}

func TestGetRegisteredDatabases_ReturnsSortedNames(t *testing.T) {
	cm, _ := newTestManager(t, "Oracle", "MySQL", "PostgreSQL", "MSSQL")

	for range 20 {
		got := cm.GetRegisteredDatabases()
		want := []string{"MSSQL", "MySQL", "Oracle", "PostgreSQL"}
		if !slices.Equal(got, want) {
			t.Fatalf("GetRegisteredDatabases() = %v, 期待値 %v", got, want)
		}
	}
}

func TestExecuteQuery_RetriesWithBackoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cm, fakes := newTestManager(t, "A")
		fakes["A"].failFirst = 2

		start := time.Now()
		result := <-cm.ExecuteQuery(t.Context(), "A", "SELECT 1")

		if result.Error != nil {
			t.Fatalf("再試行後も失敗した: %v", result.Error)
		}
		if got := fakes["A"].calls.Load(); got != 3 {
			t.Errorf("Query の呼び出し回数 = %d, 期待値 3", got)
		}
		// 待ち時間は 100ms + 200ms（仮想時間なので実時間はかからない）
		if got := time.Since(start); got != 300*time.Millisecond {
			t.Errorf("待ち時間 = %v, 期待値 300ms", got)
		}
	})
}

func TestExecuteQuery_UnknownDatabaseReturnsError(t *testing.T) {
	cm, _ := newTestManager(t)

	result := <-cm.ExecuteQuery(t.Context(), "missing", "SELECT 1")
	if result.Error == nil {
		t.Fatal("未登録の DB でエラーにならなかった")
	}
}

func TestClose_ConcurrentWithSubmitDoesNotPanicAndRejectsLaterTasks(t *testing.T) {
	for range 50 {
		cm, _ := newTestManager(t, "A")

		var wg sync.WaitGroup
		for range 8 {
			wg.Go(func() {
				// Close と競合するので成功・失敗のどちらも正しい。panic しないことだけを見る
				<-cm.ExecuteQuery(t.Context(), "A", "SELECT 1")
				<-cm.CreateUser(t.Context(), "A", &models.User{Name: "n", Email: "e"})
			})
		}
		wg.Go(func() {
			if err := cm.Close(); err != nil {
				t.Errorf("Close が失敗した: %v", err)
			}
		})
		wg.Wait()

		result := <-cm.ExecuteQuery(t.Context(), "A", "SELECT 1")
		if !errors.Is(result.Error, ErrManagerClosed) {
			t.Fatalf("Close 後の投入のエラー = %v, 期待値 ErrManagerClosed", result.Error)
		}
		if err := cm.RegisterDatabase("A", config.DatabaseConfig{}); !errors.Is(err, ErrManagerClosed) {
			t.Fatalf("Close 後の登録のエラー = %v, 期待値 ErrManagerClosed", err)
		}
	}
}

// TestExecuteTransaction_PassesAdapterTx fnがアダプターの開始したトランザクションを受け取り、fnのエラーが結果に届くことを確かめます
func TestExecuteTransaction_PassesAdapterTx(t *testing.T) {
	cm, fakes := newTestManager(t, "A")
	fakes["A"].tx = new(sql.Tx)
	wantErr := errors.New("rollback me")

	var got *sql.Tx
	result := <-cm.ExecuteTransaction(t.Context(), "A", func(tx *sql.Tx) error {
		got = tx
		return wantErr
	})

	if got != fakes["A"].tx {
		t.Fatalf("fn received tx %p, want the adapter's tx %p", got, fakes["A"].tx)
	}
	if !errors.Is(result.Error, wantErr) {
		t.Fatalf("result.Error = %v, want %v", result.Error, wantErr)
	}
}

func TestGetUsersByAgeRange_KeepsAdapterUserFields(t *testing.T) {
	cm, _ := newTestManager(t, "A")

	result := <-cm.GetUsersByAgeRange(t.Context(), "A", 20, 40)

	if result.Error != nil {
		t.Fatalf("GetUsersByAgeRange が失敗した: %v", result.Error)
	}
	if len(result.Users) != 1 {
		t.Fatalf("件数 = %d, 期待値 1", len(result.Users))
	}
	if u := result.Users[0]; u.ID != 7 || u.Age != 30 || !u.CreatedAt.Equal(searchCreatedAt) {
		t.Errorf("ユーザー = %+v, 期待値 ID=7 Age=30 CreatedAt=%v", u, searchCreatedAt)
	}
}
