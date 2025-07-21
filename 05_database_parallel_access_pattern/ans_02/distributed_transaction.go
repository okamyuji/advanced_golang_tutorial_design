package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	_ "github.com/lib/pq"
)

// 分散トランザクション処理システム
// 複数テーブル（注文、在庫、決済）にまたがる処理を2フェーズコミットで実装

// TransactionCoordinator 分散トランザクションコーディネーター
type TransactionCoordinator struct {
	db              *sql.DB
	timeout         time.Duration
	maxRetries      int
	participantMgrs map[string]ParticipantManager
	metrics         *DistributedTxMetrics
}

// ParticipantManager 分散トランザクション参加者
type ParticipantManager interface {
	Prepare(ctx context.Context, tx *sql.Tx, data interface{}) error
	Commit(ctx context.Context, tx *sql.Tx, data interface{}) error
	Rollback(ctx context.Context, tx *sql.Tx, data interface{}) error
	GetName() string
}

// OrderData 注文データ
type OrderData struct {
	OrderID     string    `json:"order_id"`
	CustomerID  int64     `json:"customer_id"`
	ItemID      int64     `json:"item_id"`
	Quantity    int32     `json:"quantity"`
	TotalAmount int64     `json:"total_amount"`
	CreatedAt   time.Time `json:"created_at"`
}

// DistributedTransaction 分散トランザクション
type DistributedTransaction struct {
	ID           string
	Participants []string
	Data         *OrderData
	Status       TransactionStatus
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// TransactionStatus トランザクション状態
type TransactionStatus string

const (
	StatusPending   TransactionStatus = "PENDING"
	StatusPrepared  TransactionStatus = "PREPARED"
	StatusCommitted TransactionStatus = "COMMITTED"
	StatusAborted   TransactionStatus = "ABORTED"
)

// DistributedTxMetrics 分散トランザクションメトリクス
type DistributedTxMetrics struct {
	mu                sync.RWMutex
	totalTransactions int64
	committedTx       int64
	abortedTx         int64
	prepareFailures   int64
	commitFailures    int64
	timeouts          int64
}

// DistributedTxMetricsSnapshot メトリクスのスナップショット（mutex無し）
type DistributedTxMetricsSnapshot struct {
	TotalTransactions int64 `json:"total_transactions"`
	CommittedTx       int64 `json:"committed_tx"`
	AbortedTx         int64 `json:"aborted_tx"`
	PrepareFailures   int64 `json:"prepare_failures"`
	CommitFailures    int64 `json:"commit_failures"`
	Timeouts          int64 `json:"timeouts"`
}

// カスタムエラー定義
var (
	ErrPreparePhaseFailure = errors.New("prepare phase failure")
	ErrCommitPhaseFailure  = errors.New("commit phase failure")
	ErrTransactionTimeout  = errors.New("transaction timeout")
	ErrParticipantNotFound = errors.New("participant not found")
)

// NewTransactionCoordinator 新しい分散トランザクションコーディネーターを作成
func NewTransactionCoordinator(db *sql.DB) *TransactionCoordinator {
	coordinator := &TransactionCoordinator{
		db:              db,
		timeout:         30 * time.Second,
		maxRetries:      3,
		participantMgrs: make(map[string]ParticipantManager),
		metrics:         &DistributedTxMetrics{},
	}

	// 参加者マネージャーを登録
	coordinator.RegisterParticipant(&OrderManager{})
	coordinator.RegisterParticipant(&InventoryManager{})
	coordinator.RegisterParticipant(&PaymentManager{})

	return coordinator
}

// RegisterParticipant 参加者を登録
func (tc *TransactionCoordinator) RegisterParticipant(pm ParticipantManager) {
	tc.participantMgrs[pm.GetName()] = pm
}

// ExecuteDistributedTransaction 分散トランザクションを実行
func (tc *TransactionCoordinator) ExecuteDistributedTransaction(ctx context.Context, data *OrderData) error {
	tc.metrics.incrementTotalTransactions()

	// タイムアウト設定
	ctx, cancel := context.WithTimeout(ctx, tc.timeout)
	defer cancel()

	// 分散トランザクションID生成
	txID := fmt.Sprintf("DTX-%d-%s", time.Now().Unix(), data.OrderID)

	log.Printf("Starting distributed transaction: %s", txID)

	// 分散トランザクション記録
	if err := tc.recordDistributedTransaction(ctx, txID, data); err != nil {
		return fmt.Errorf("failed to record distributed transaction: %w", err)
	}

	// 2フェーズコミットプロトコル実行
	err := tc.executeTwoPhaseCommit(ctx, txID, data)

	// 結果に応じてメトリクス更新
	if err != nil {
		tc.metrics.incrementAbortedTx()
		if err := tc.updateTransactionStatus(ctx, txID, StatusAborted); err != nil {
			log.Printf("Failed to update transaction status to aborted: %v", err)
		}
		return err
	}

	tc.metrics.incrementCommittedTx()
	if err := tc.updateTransactionStatus(ctx, txID, StatusCommitted); err != nil {
		log.Printf("Failed to update transaction status to committed: %v", err)
	}

	log.Printf("Distributed transaction completed successfully: %s", txID)
	return nil
}

// executeTwoPhaseCommit 2フェーズコミットを実行
func (tc *TransactionCoordinator) executeTwoPhaseCommit(ctx context.Context, txID string, data *OrderData) error {
	// Phase 1: Prepare Phase
	log.Printf("Phase 1: Prepare phase for transaction %s", txID)

	preparedTxs := make(map[string]*sql.Tx)
	participantNames := []string{"order", "inventory", "payment"}

	// すべての参加者でPrepareを実行
	for _, name := range participantNames {
		participant, exists := tc.participantMgrs[name]
		if !exists {
			tc.rollbackAll(preparedTxs, data)
			return fmt.Errorf("%w: %s", ErrParticipantNotFound, name)
		}

		tx, err := tc.db.BeginTx(ctx, &sql.TxOptions{
			Isolation: sql.LevelSerializable,
		})
		if err != nil {
			tc.rollbackAll(preparedTxs, data)
			tc.metrics.incrementPrepareFailures()
			return fmt.Errorf("failed to begin transaction for %s: %w", name, err)
		}

		err = participant.Prepare(ctx, tx, data)
		if err != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				log.Printf("Failed to rollback transaction: %v", rollbackErr)
			}
			tc.rollbackAll(preparedTxs, data)
			tc.metrics.incrementPrepareFailures()
			return fmt.Errorf("%w: participant %s failed: %w", ErrPreparePhaseFailure, name, err)
		}

		preparedTxs[name] = tx
		log.Printf("Participant %s prepared successfully", name)
	}

	// Prepareフェーズ成功 - ステータス更新
	if err := tc.updateTransactionStatus(ctx, txID, StatusPrepared); err != nil {
		log.Printf("Failed to update transaction status to prepared: %v", err)
	}

	// Phase 2: Commit Phase
	log.Printf("Phase 2: Commit phase for transaction %s", txID)

	// すべての参加者でCommitを実行
	for name, tx := range preparedTxs {
		participant := tc.participantMgrs[name]

		err := participant.Commit(ctx, tx, data)
		if err != nil {
			// コミットフェーズでのエラーは致命的
			// 他の参加者も強制的にロールバック
			tc.forceRollbackAll(preparedTxs, data, name)
			tc.metrics.incrementCommitFailures()
			return fmt.Errorf("%w: participant %s failed: %w", ErrCommitPhaseFailure, name, err)
		}

		err = tx.Commit()
		if err != nil {
			tc.forceRollbackAll(preparedTxs, data, name)
			tc.metrics.incrementCommitFailures()
			return fmt.Errorf("failed to commit transaction for %s: %w", name, err)
		}

		log.Printf("Participant %s committed successfully", name)
	}

	return nil
}

// rollbackAll すべてのトランザクションをロールバック
func (tc *TransactionCoordinator) rollbackAll(txs map[string]*sql.Tx, data *OrderData) {
	for name, tx := range txs {
		participant := tc.participantMgrs[name]

		if err := participant.Rollback(context.Background(), tx, data); err != nil {
			log.Printf("Failed to rollback participant %s: %v", name, err)
		}

		if err := tx.Rollback(); err != nil {
			log.Printf("Failed to rollback transaction for %s: %v", name, err)
		}

		log.Printf("Participant %s rolled back", name)
	}
}

// forceRollbackAll 強制的にすべてをロールバック
func (tc *TransactionCoordinator) forceRollbackAll(txs map[string]*sql.Tx, data *OrderData, failedParticipant string) {
	log.Printf("Force rollback initiated due to failure in %s", failedParticipant)
	tc.rollbackAll(txs, data)
}

// OrderManager 注文管理参加者
type OrderManager struct{}

func (om *OrderManager) GetName() string { return "order" }

func (om *OrderManager) Prepare(ctx context.Context, tx *sql.Tx, data interface{}) error {
	orderData := data.(*OrderData)

	// 注文データの検証
	if orderData.Quantity <= 0 {
		return errors.New("invalid quantity")
	}
	if orderData.TotalAmount <= 0 {
		return errors.New("invalid total amount")
	}

	// 顧客の存在確認
	var exists bool
	err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM customers WHERE id = $1)",
		orderData.CustomerID).Scan(&exists)
	if err != nil {
		return fmt.Errorf("failed to check customer existence: %w", err)
	}
	if !exists {
		return errors.New("customer not found")
	}

	// 注文の準備（実際の挿入はCommitフェーズで実行）
	log.Printf("Order manager prepared for order: %s", orderData.OrderID)
	return nil
}

func (om *OrderManager) Commit(ctx context.Context, tx *sql.Tx, data interface{}) error {
	orderData := data.(*OrderData)

	query := `
		INSERT INTO orders (order_id, customer_id, item_id, quantity, total_amount, status, created_at)
		VALUES ($1, $2, $3, $4, $5, 'CONFIRMED', $6)`

	_, err := tx.ExecContext(ctx, query, orderData.OrderID, orderData.CustomerID,
		orderData.ItemID, orderData.Quantity, orderData.TotalAmount, orderData.CreatedAt)

	if err != nil {
		return fmt.Errorf("failed to create order: %w", err)
	}

	log.Printf("Order created: %s", orderData.OrderID)
	return nil
}

func (om *OrderManager) Rollback(ctx context.Context, tx *sql.Tx, data interface{}) error {
	// Rollback処理（必要に応じて補償処理を実装）
	log.Printf("Order manager rollback executed")
	return nil
}

// InventoryManager 在庫管理参加者
type InventoryManager struct{}

func (im *InventoryManager) GetName() string { return "inventory" }

func (im *InventoryManager) Prepare(ctx context.Context, tx *sql.Tx, data interface{}) error {
	orderData := data.(*OrderData)

	// 在庫の確認と予約
	var currentQuantity int32
	query := "SELECT quantity FROM inventory WHERE id = $1 FOR UPDATE"
	err := tx.QueryRowContext(ctx, query, orderData.ItemID).Scan(&currentQuantity)
	if err == sql.ErrNoRows {
		return errors.New("item not found")
	}
	if err != nil {
		return fmt.Errorf("failed to get inventory: %w", err)
	}

	if currentQuantity < orderData.Quantity {
		return errors.New("insufficient stock")
	}

	// 在庫予約マークを設定
	reserveQuery := `
		INSERT INTO inventory_reservations (item_id, order_id, quantity, created_at)
		VALUES ($1, $2, $3, CURRENT_TIMESTAMP)`

	_, err = tx.ExecContext(ctx, reserveQuery, orderData.ItemID, orderData.OrderID, orderData.Quantity)
	if err != nil {
		return fmt.Errorf("failed to reserve inventory: %w", err)
	}

	log.Printf("Inventory reserved for order: %s, quantity: %d", orderData.OrderID, orderData.Quantity)
	return nil
}

func (im *InventoryManager) Commit(ctx context.Context, tx *sql.Tx, data interface{}) error {
	orderData := data.(*OrderData)

	// 在庫を実際に減算
	updateQuery := `
		UPDATE inventory 
		SET quantity = quantity - $1, updated_at = CURRENT_TIMESTAMP 
		WHERE id = $2`

	_, err := tx.ExecContext(ctx, updateQuery, orderData.Quantity, orderData.ItemID)
	if err != nil {
		return fmt.Errorf("failed to update inventory: %w", err)
	}

	// 予約レコードを削除
	deleteQuery := "DELETE FROM inventory_reservations WHERE order_id = $1"
	_, err = tx.ExecContext(ctx, deleteQuery, orderData.OrderID)
	if err != nil {
		return fmt.Errorf("failed to remove reservation: %w", err)
	}

	log.Printf("Inventory updated for order: %s", orderData.OrderID)
	return nil
}

func (im *InventoryManager) Rollback(ctx context.Context, tx *sql.Tx, data interface{}) error {
	orderData := data.(*OrderData)

	// 予約を解除
	deleteQuery := "DELETE FROM inventory_reservations WHERE order_id = $1"
	_, err := tx.ExecContext(ctx, deleteQuery, orderData.OrderID)
	if err != nil {
		log.Printf("Failed to remove reservation during rollback: %v", err)
	}

	log.Printf("Inventory reservation rolled back for order: %s", orderData.OrderID)
	return nil
}

// PaymentManager 決済管理参加者
type PaymentManager struct{}

func (pm *PaymentManager) GetName() string { return "payment" }

func (pm *PaymentManager) Prepare(ctx context.Context, tx *sql.Tx, data interface{}) error {
	orderData := data.(*OrderData)

	// 顧客の支払い能力確認（残高チェック等）
	var balance int64
	query := "SELECT balance FROM customer_accounts WHERE customer_id = $1 FOR UPDATE"
	err := tx.QueryRowContext(ctx, query, orderData.CustomerID).Scan(&balance)
	if err == sql.ErrNoRows {
		return errors.New("customer account not found")
	}
	if err != nil {
		return fmt.Errorf("failed to get customer balance: %w", err)
	}

	if balance < orderData.TotalAmount {
		return errors.New("insufficient funds")
	}

	// 決済予約の作成
	reserveQuery := `
		INSERT INTO payment_reservations (customer_id, order_id, amount, created_at)
		VALUES ($1, $2, $3, CURRENT_TIMESTAMP)`

	_, err = tx.ExecContext(ctx, reserveQuery, orderData.CustomerID, orderData.OrderID, orderData.TotalAmount)
	if err != nil {
		return fmt.Errorf("failed to reserve payment: %w", err)
	}

	log.Printf("Payment reserved for order: %s, amount: %d", orderData.OrderID, orderData.TotalAmount)
	return nil
}

func (pm *PaymentManager) Commit(ctx context.Context, tx *sql.Tx, data interface{}) error {
	orderData := data.(*OrderData)

	// 残高から実際に減算
	updateQuery := `
		UPDATE customer_accounts 
		SET balance = balance - $1, updated_at = CURRENT_TIMESTAMP 
		WHERE customer_id = $2`

	_, err := tx.ExecContext(ctx, updateQuery, orderData.TotalAmount, orderData.CustomerID)
	if err != nil {
		return fmt.Errorf("failed to deduct payment: %w", err)
	}

	// 決済履歴の記録
	paymentQuery := `
		INSERT INTO payments (order_id, customer_id, amount, status, created_at)
		VALUES ($1, $2, $3, 'COMPLETED', CURRENT_TIMESTAMP)`

	_, err = tx.ExecContext(ctx, paymentQuery, orderData.OrderID, orderData.CustomerID, orderData.TotalAmount)
	if err != nil {
		return fmt.Errorf("failed to record payment: %w", err)
	}

	// 予約レコードを削除
	deleteQuery := "DELETE FROM payment_reservations WHERE order_id = $1"
	_, err = tx.ExecContext(ctx, deleteQuery, orderData.OrderID)
	if err != nil {
		return fmt.Errorf("failed to remove payment reservation: %w", err)
	}

	log.Printf("Payment processed for order: %s", orderData.OrderID)
	return nil
}

func (pm *PaymentManager) Rollback(ctx context.Context, tx *sql.Tx, data interface{}) error {
	orderData := data.(*OrderData)

	// 決済予約を解除
	deleteQuery := "DELETE FROM payment_reservations WHERE order_id = $1"
	_, err := tx.ExecContext(ctx, deleteQuery, orderData.OrderID)
	if err != nil {
		log.Printf("Failed to remove payment reservation during rollback: %v", err)
	}

	log.Printf("Payment reservation rolled back for order: %s", orderData.OrderID)
	return nil
}

// ヘルパーメソッド群
func (tc *TransactionCoordinator) recordDistributedTransaction(ctx context.Context, txID string, data *OrderData) error {
	query := `
		INSERT INTO distributed_transactions (tx_id, order_id, status, created_at)
		VALUES ($1, $2, $3, CURRENT_TIMESTAMP)`

	_, err := tc.db.ExecContext(ctx, query, txID, data.OrderID, StatusPending)
	return err
}

func (tc *TransactionCoordinator) updateTransactionStatus(ctx context.Context, txID string, status TransactionStatus) error {
	query := `
		UPDATE distributed_transactions 
		SET status = $1, updated_at = CURRENT_TIMESTAMP 
		WHERE tx_id = $2`

	_, err := tc.db.ExecContext(ctx, query, status, txID)
	return err
}

// メトリクス更新メソッド群
func (m *DistributedTxMetrics) incrementTotalTransactions() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.totalTransactions++
}

func (m *DistributedTxMetrics) incrementCommittedTx() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.committedTx++
}

func (m *DistributedTxMetrics) incrementAbortedTx() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.abortedTx++
}

func (m *DistributedTxMetrics) incrementPrepareFailures() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.prepareFailures++
}

func (m *DistributedTxMetrics) incrementCommitFailures() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.commitFailures++
}

// GetMetrics メトリクスを取得
func (tc *TransactionCoordinator) GetMetrics() DistributedTxMetricsSnapshot {
	tc.metrics.mu.RLock()
	defer tc.metrics.mu.RUnlock()

	return DistributedTxMetricsSnapshot{
		TotalTransactions: tc.metrics.totalTransactions,
		CommittedTx:       tc.metrics.committedTx,
		AbortedTx:         tc.metrics.abortedTx,
		PrepareFailures:   tc.metrics.prepareFailures,
		CommitFailures:    tc.metrics.commitFailures,
		Timeouts:          tc.metrics.timeouts,
	}
}

// 使用例とデモンストレーション
func main() {
	// データベース接続
	db, err := sql.Open("postgres",
		"host=localhost port=5432 user=tutorial_user password=tutorial_pass dbname=tutorial_db sslmode=disable")
	if err != nil {
		log.Fatal("Failed to connect to database:", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Printf("Failed to close database connection: %v", err)
		}
	}()

	// 分散トランザクションコーディネーター初期化
	coordinator := NewTransactionCoordinator(db)

	// サンプル注文データ
	orderData := &OrderData{
		OrderID:     "ORDER-2024-001",
		CustomerID:  1,
		ItemID:      1,
		Quantity:    2,
		TotalAmount: 2000,
		CreatedAt:   time.Now(),
	}

	// 分散トランザクション実行
	ctx := context.Background()
	err = coordinator.ExecuteDistributedTransaction(ctx, orderData)
	if err != nil {
		log.Printf("Distributed transaction failed: %v", err)
	} else {
		log.Printf("Distributed transaction completed successfully")
	}

	// メトリクス表示
	metrics := coordinator.GetMetrics()
	log.Printf("Transaction metrics: %+v", metrics)
}
