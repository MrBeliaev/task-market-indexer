package db

import (
	"context"
	"fmt"
	"strings"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
)

// IndexerState tracks the last processed block per chain.
type IndexerState struct {
	ChainID         int64     `gorm:"primaryKey;column:chain_id"`
	LastBlockNumber uint64    `gorm:"column:last_block_number"`
	UpdatedAt       time.Time `gorm:"column:updated_at"`
}

func (IndexerState) TableName() string { return "indexer_state" }

// ChainConfig is per-chain indexer configuration. Rows are managed externally
// (e.g. via Prisma/admin tooling); the indexer only reads enabled chains.
type ChainConfig struct {
	ChainID         int64  `gorm:"primaryKey;column:chain_id"`
	RPCURL          string `gorm:"column:rpc_url"`
	ContractAddress string `gorm:"column:contract_address"`
	StartBlock      uint64 `gorm:"column:start_block"`
	Enabled         bool   `gorm:"column:enabled"`
}

func (ChainConfig) TableName() string { return "chain_config" }

// LoadEnabledChains returns all chains with enabled = true, ordered by chain_id.
func LoadEnabledChains(ctx context.Context, db *gorm.DB) ([]ChainConfig, error) {
	var chains []ChainConfig
	if err := db.WithContext(ctx).Where("enabled = ?", true).Order("chain_id").Find(&chains).Error; err != nil {
		return nil, fmt.Errorf("loading chain_config: %w", err)
	}
	return chains, nil
}

// Task is a partial model for the columns the indexer writes.
type Task struct {
	ID                    int64     `gorm:"primaryKey"`
	ChainID               int64     `gorm:"column:chain_id"`
	OnChainID             int64     `gorm:"column:on_chain_id"`
	Client                string    `gorm:"column:client"`
	Executor              *string   `gorm:"column:executor"`
	Reward                string    `gorm:"column:reward"`
	Deadline              time.Time `gorm:"column:deadline"`
	MetadataHash          string    `gorm:"column:metadata_hash"`
	Status                string    `gorm:"column:status"`
	Title                 string    `gorm:"column:title"`
	Description           string    `gorm:"column:description"`
	ContactInfo           string    `gorm:"column:contact_info"`
	ClientConfirmed       bool      `gorm:"column:client_confirmed"`
	ExecutorConfirmed     bool      `gorm:"column:executor_confirmed"`
	PayoutWei             *string   `gorm:"column:payout_wei"`
	FeeWei                *string   `gorm:"column:fee_wei"`
	DisputedBy            *string   `gorm:"column:disputed_by"`
	DisputeClientRefund   *string   `gorm:"column:dispute_client_refund"`
	DisputeExecutorPayout *string   `gorm:"column:dispute_executor_payout"`
	CreatedAt             time.Time `gorm:"column:created_at"`
	UpdatedAt             time.Time `gorm:"column:updated_at"`
}

// Withdrawal records each pull-payment withdrawal event.
type Withdrawal struct {
	ID          int64     `gorm:"primaryKey"`
	ChainID     int64     `gorm:"column:chain_id"`
	Recipient   string    `gorm:"column:recipient"`
	Amount      string    `gorm:"column:amount"`
	BlockNumber uint64    `gorm:"column:block_number"`
	TxHash      string    `gorm:"column:tx_hash"`
	CreatedAt   time.Time `gorm:"column:created_at"`
}

// Connect opens a GORM connection pool and pings the server.
func Connect(databaseURL string) (*gorm.DB, error) {
	db, err := gorm.Open(postgres.Open(databaseURL), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		return nil, fmt.Errorf("gorm.Open: %w", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}
	if err := sqlDB.Ping(); err != nil {
		return nil, fmt.Errorf("db ping: %w", err)
	}
	return db, nil
}

// Close closes the underlying sql.DB connection pool.
func Close(db *gorm.DB) {
	if sqlDB, err := db.DB(); err == nil {
		sqlDB.Close()
	}
}

// ErrNotFound is returned when an UPDATE affects 0 rows (task not yet synced to DB).
var ErrNotFound = fmt.Errorf("task not found in db")

// GetOrInitLastBlock returns the last processed block for a chain.
// Inserts a record with startBlock if none exists yet.
func GetOrInitLastBlock(ctx context.Context, db *gorm.DB, chainID int64, startBlock uint64) (uint64, error) {
	var last uint64
	err := db.WithContext(ctx).Raw(`
		INSERT INTO indexer_state (chain_id, last_block_number, updated_at)
		VALUES (?, ?, NOW())
		ON CONFLICT (chain_id) DO UPDATE SET updated_at = NOW()
		RETURNING last_block_number
	`, chainID, startBlock).Scan(&last).Error
	if err != nil {
		return 0, fmt.Errorf("GetOrInitLastBlock chain=%d: %w", chainID, err)
	}
	return last, nil
}

// SetLastBlock persists the last processed block number for a chain.
func SetLastBlock(ctx context.Context, db *gorm.DB, chainID int64, block uint64) error {
	result := db.WithContext(ctx).
		Table("indexer_state").
		Where("chain_id = ?", chainID).
		Updates(map[string]interface{}{
			"last_block_number": block,
			"updated_at":        gorm.Expr("NOW()"),
		})
	if result.Error != nil {
		return fmt.Errorf("SetLastBlock chain=%d: %w", chainID, result.Error)
	}
	return nil
}

// UpsertTaskCreated inserts a placeholder task row, skipping silently if already exists.
func UpsertTaskCreated(
	ctx context.Context,
	db *gorm.DB,
	chainID int64,
	onChainID int64,
	client string,
	rewardWei string,
	deadline time.Time,
	metadataHash string,
) error {
	task := Task{
		ChainID:      chainID,
		OnChainID:    onChainID,
		Client:       strings.ToLower(client),
		Reward:       rewardWei,
		Deadline:     deadline,
		MetadataHash: metadataHash,
		Status:       "OPEN",
		Title:        fmt.Sprintf("Task #%d", onChainID),
		Description:  "Pending metadata sync",
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
	}
	result := db.WithContext(ctx).
		Clauses(clause.OnConflict{DoNothing: true}).
		Create(&task)
	if result.Error != nil {
		return fmt.Errorf("UpsertTaskCreated chain=%d id=%d: %w", chainID, onChainID, result.Error)
	}
	return nil
}

// UpdateTaskAssigned sets the executor and transitions status to ASSIGNED.
func UpdateTaskAssigned(ctx context.Context, db *gorm.DB, chainID, onChainID int64, executor string) error {
	result := db.WithContext(ctx).
		Table("tasks").
		Where("chain_id = ? AND on_chain_id = ?", chainID, onChainID).
		Updates(map[string]interface{}{
			"executor":   strings.ToLower(executor),
			"status":     "ASSIGNED",
			"updated_at": gorm.Expr("NOW()"),
		})
	if result.Error != nil {
		return fmt.Errorf("UpdateTaskAssigned chain=%d id=%d: %w", chainID, onChainID, result.Error)
	}
	if result.RowsAffected == 0 {
		return ErrNotFound
	}
	return nil
}

// UpdateTaskStatus syncs a status transition from a TaskStatusChanged event.
func UpdateTaskStatus(ctx context.Context, db *gorm.DB, chainID, onChainID int64, status string) error {
	result := db.WithContext(ctx).
		Table("tasks").
		Where("chain_id = ? AND on_chain_id = ?", chainID, onChainID).
		Updates(map[string]interface{}{
			"status":     status,
			"updated_at": gorm.Expr("NOW()"),
		})
	if result.Error != nil {
		return fmt.Errorf("UpdateTaskStatus chain=%d id=%d: %w", chainID, onChainID, result.Error)
	}
	if result.RowsAffected == 0 {
		return ErrNotFound
	}
	return nil
}

// UpdateConfirmations syncs multi-sig state from a CompletionConfirmed event.
func UpdateConfirmations(
	ctx context.Context,
	db *gorm.DB,
	chainID, onChainID int64,
	clientConfirmed, executorConfirmed bool,
) error {
	result := db.WithContext(ctx).
		Table("tasks").
		Where("chain_id = ? AND on_chain_id = ?", chainID, onChainID).
		Updates(map[string]interface{}{
			"client_confirmed":   clientConfirmed,
			"executor_confirmed": executorConfirmed,
			"updated_at":         gorm.Expr("NOW()"),
		})
	if result.Error != nil {
		return fmt.Errorf("UpdateConfirmations chain=%d id=%d: %w", chainID, onChainID, result.Error)
	}
	if result.RowsAffected == 0 {
		return ErrNotFound
	}
	return nil
}

// UpdateTaskCompleted stores payout and fee amounts from a TaskCompleted event.
func UpdateTaskCompleted(ctx context.Context, db *gorm.DB, chainID, onChainID int64, payoutWei, feeWei string) error {
	result := db.WithContext(ctx).
		Table("tasks").
		Where("chain_id = ? AND on_chain_id = ?", chainID, onChainID).
		Updates(map[string]interface{}{
			"payout_wei": payoutWei,
			"fee_wei":    feeWei,
			"updated_at": gorm.Expr("NOW()"),
		})
	if result.Error != nil {
		return fmt.Errorf("UpdateTaskCompleted chain=%d id=%d: %w", chainID, onChainID, result.Error)
	}
	if result.RowsAffected == 0 {
		return ErrNotFound
	}
	return nil
}

// UpdateTaskDisputed stores the address that raised the dispute.
func UpdateTaskDisputed(ctx context.Context, db *gorm.DB, chainID, onChainID int64, disputedBy string) error {
	result := db.WithContext(ctx).
		Table("tasks").
		Where("chain_id = ? AND on_chain_id = ?", chainID, onChainID).
		Updates(map[string]interface{}{
			"disputed_by": strings.ToLower(disputedBy),
			"updated_at":  gorm.Expr("NOW()"),
		})
	if result.Error != nil {
		return fmt.Errorf("UpdateTaskDisputed chain=%d id=%d: %w", chainID, onChainID, result.Error)
	}
	if result.RowsAffected == 0 {
		return ErrNotFound
	}
	return nil
}

// UpdateDisputeResolved stores the client/executor split from a DisputeResolved event.
func UpdateDisputeResolved(
	ctx context.Context,
	db *gorm.DB,
	chainID, onChainID int64,
	clientRefund, executorPayout string,
) error {
	result := db.WithContext(ctx).
		Table("tasks").
		Where("chain_id = ? AND on_chain_id = ?", chainID, onChainID).
		Updates(map[string]interface{}{
			"dispute_client_refund":   clientRefund,
			"dispute_executor_payout": executorPayout,
			"updated_at":              gorm.Expr("NOW()"),
		})
	if result.Error != nil {
		return fmt.Errorf("UpdateDisputeResolved chain=%d id=%d: %w", chainID, onChainID, result.Error)
	}
	if result.RowsAffected == 0 {
		return ErrNotFound
	}
	return nil
}

// UpdateTaskDeadline updates the deadline field from a DeadlineExtended event.
func UpdateTaskDeadline(ctx context.Context, db *gorm.DB, chainID, onChainID int64, newDeadline time.Time) error {
	result := db.WithContext(ctx).
		Table("tasks").
		Where("chain_id = ? AND on_chain_id = ?", chainID, onChainID).
		Updates(map[string]interface{}{
			"deadline":   newDeadline,
			"updated_at": gorm.Expr("NOW()"),
		})
	if result.Error != nil {
		return fmt.Errorf("UpdateTaskDeadline chain=%d id=%d: %w", chainID, onChainID, result.Error)
	}
	if result.RowsAffected == 0 {
		return ErrNotFound
	}
	return nil
}

// RecordWithdrawal inserts a row for each Withdrawn event.
func RecordWithdrawal(
	ctx context.Context,
	db *gorm.DB,
	chainID int64,
	recipient, amount string,
	blockNumber uint64,
	txHash string,
) error {
	w := Withdrawal{
		ChainID:     chainID,
		Recipient:   strings.ToLower(recipient),
		Amount:      amount,
		BlockNumber: blockNumber,
		TxHash:      txHash,
		CreatedAt:   time.Now(),
	}
	result := db.WithContext(ctx).
		Clauses(clause.OnConflict{DoNothing: true}).
		Create(&w)
	if result.Error != nil {
		return fmt.Errorf("RecordWithdrawal chain=%d recipient=%s: %w", chainID, recipient, result.Error)
	}
	return nil
}
