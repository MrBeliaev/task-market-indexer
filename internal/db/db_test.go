package db

import (
	"context"
	"fmt"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pgdriver "gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func newMockDB(t *testing.T) (*gorm.DB, sqlmock.Sqlmock) {
	t.Helper()

	sqlDB, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = sqlDB.Close() })

	gdb, err := gorm.Open(pgdriver.New(pgdriver.Config{Conn: sqlDB}), &gorm.Config{
		Logger:                 logger.Default.LogMode(logger.Silent),
		SkipDefaultTransaction: true,
	})
	require.NoError(t, err)

	return gdb, mock
}

func TestGetOrInitLastBlock_Insert(t *testing.T) {
	gdb, mock := newMockDB(t)

	mock.ExpectQuery("INSERT INTO indexer_state").
		WillReturnRows(sqlmock.NewRows([]string{"last_block_number"}).AddRow(uint64(0)))

	last, err := GetOrInitLastBlock(context.Background(), gdb, 1, 0)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), last)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestGetOrInitLastBlock_ExistingBlock(t *testing.T) {
	gdb, mock := newMockDB(t)

	mock.ExpectQuery("INSERT INTO indexer_state").
		WillReturnRows(sqlmock.NewRows([]string{"last_block_number"}).AddRow(uint64(999)))

	last, err := GetOrInitLastBlock(context.Background(), gdb, 1, 0)
	require.NoError(t, err)
	assert.Equal(t, uint64(999), last)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestGetOrInitLastBlock_DBError(t *testing.T) {
	gdb, mock := newMockDB(t)

	mock.ExpectQuery("INSERT INTO indexer_state").
		WillReturnError(errDBConn)

	_, err := GetOrInitLastBlock(context.Background(), gdb, 1, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "GetOrInitLastBlock")
}

func TestSetLastBlock(t *testing.T) {
	gdb, mock := newMockDB(t)

	mock.ExpectExec(`UPDATE "indexer_state"`).
		WillReturnResult(sqlmock.NewResult(0, 1))

	err := SetLastBlock(context.Background(), gdb, 1, 500)
	require.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestSetLastBlock_DBError(t *testing.T) {
	gdb, mock := newMockDB(t)

	mock.ExpectExec(`UPDATE "indexer_state"`).
		WillReturnError(errDBConn)

	err := SetLastBlock(context.Background(), gdb, 1, 500)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "SetLastBlock")
}

func TestUpsertTaskCreated(t *testing.T) {
	gdb, mock := newMockDB(t)

	mock.ExpectQuery(`INSERT INTO "tasks"`).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(1)))

	err := UpsertTaskCreated(
		context.Background(), gdb, 1, 42,
		"0xClient", "1000000000000000000",
		time.Now().Add(24*time.Hour), "0xhash",
	)
	require.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestUpsertTaskCreated_DBError(t *testing.T) {
	gdb, mock := newMockDB(t)

	mock.ExpectQuery(`INSERT INTO "tasks"`).
		WillReturnError(errDBConn)

	err := UpsertTaskCreated(
		context.Background(), gdb, 1, 42,
		"0xClient", "1000000000000000000",
		time.Now().Add(24*time.Hour), "0xhash",
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "UpsertTaskCreated")
}

func TestUpdateTaskAssigned_OK(t *testing.T) {
	gdb, mock := newMockDB(t)

	mock.ExpectExec(`UPDATE "tasks"`).
		WillReturnResult(sqlmock.NewResult(0, 1))

	err := UpdateTaskAssigned(context.Background(), gdb, 1, 42, "0xExecutor")
	require.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestUpdateTaskAssigned_NotFound(t *testing.T) {
	gdb, mock := newMockDB(t)

	mock.ExpectExec(`UPDATE "tasks"`).
		WillReturnResult(sqlmock.NewResult(0, 0))

	err := UpdateTaskAssigned(context.Background(), gdb, 1, 42, "0xExecutor")
	assert.ErrorIs(t, err, ErrNotFound)
}

func TestUpdateTaskAssigned_DBError(t *testing.T) {
	gdb, mock := newMockDB(t)

	mock.ExpectExec(`UPDATE "tasks"`).
		WillReturnError(errDBConn)

	err := UpdateTaskAssigned(context.Background(), gdb, 1, 42, "0xExecutor")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "UpdateTaskAssigned")
}

func TestUpdateTaskStatus_OK(t *testing.T) {
	gdb, mock := newMockDB(t)

	mock.ExpectExec(`UPDATE "tasks"`).
		WillReturnResult(sqlmock.NewResult(0, 1))

	err := UpdateTaskStatus(context.Background(), gdb, 1, 42, "IN_PROGRESS")
	require.NoError(t, err)
}

func TestUpdateTaskStatus_NotFound(t *testing.T) {
	gdb, mock := newMockDB(t)

	mock.ExpectExec(`UPDATE "tasks"`).
		WillReturnResult(sqlmock.NewResult(0, 0))

	err := UpdateTaskStatus(context.Background(), gdb, 1, 42, "IN_PROGRESS")
	assert.ErrorIs(t, err, ErrNotFound)
}

func TestUpdateConfirmations_OK(t *testing.T) {
	gdb, mock := newMockDB(t)

	mock.ExpectExec(`UPDATE "tasks"`).
		WillReturnResult(sqlmock.NewResult(0, 1))

	err := UpdateConfirmations(context.Background(), gdb, 1, 42, true, false)
	require.NoError(t, err)
}

func TestUpdateConfirmations_NotFound(t *testing.T) {
	gdb, mock := newMockDB(t)

	mock.ExpectExec(`UPDATE "tasks"`).
		WillReturnResult(sqlmock.NewResult(0, 0))

	err := UpdateConfirmations(context.Background(), gdb, 1, 42, true, true)
	assert.ErrorIs(t, err, ErrNotFound)
}

func TestUpdateTaskCompleted_OK(t *testing.T) {
	gdb, mock := newMockDB(t)

	mock.ExpectExec(`UPDATE "tasks"`).
		WillReturnResult(sqlmock.NewResult(0, 1))

	err := UpdateTaskCompleted(context.Background(), gdb, 1, 42, "950000000000000000", "50000000000000000")
	require.NoError(t, err)
}

func TestUpdateTaskCompleted_NotFound(t *testing.T) {
	gdb, mock := newMockDB(t)

	mock.ExpectExec(`UPDATE "tasks"`).
		WillReturnResult(sqlmock.NewResult(0, 0))

	err := UpdateTaskCompleted(context.Background(), gdb, 1, 42, "950000000000000000", "50000000000000000")
	assert.ErrorIs(t, err, ErrNotFound)
}

func TestUpdateTaskDisputed_OK(t *testing.T) {
	gdb, mock := newMockDB(t)

	mock.ExpectExec(`UPDATE "tasks"`).
		WillReturnResult(sqlmock.NewResult(0, 1))

	err := UpdateTaskDisputed(context.Background(), gdb, 1, 42, "0xDisputant")
	require.NoError(t, err)
}

func TestUpdateTaskDisputed_NotFound(t *testing.T) {
	gdb, mock := newMockDB(t)

	mock.ExpectExec(`UPDATE "tasks"`).
		WillReturnResult(sqlmock.NewResult(0, 0))

	err := UpdateTaskDisputed(context.Background(), gdb, 1, 42, "0xDisputant")
	assert.ErrorIs(t, err, ErrNotFound)
}

func TestUpdateDisputeResolved_OK(t *testing.T) {
	gdb, mock := newMockDB(t)

	mock.ExpectExec(`UPDATE "tasks"`).
		WillReturnResult(sqlmock.NewResult(0, 1))

	err := UpdateDisputeResolved(context.Background(), gdb, 1, 42, "500000000000000000", "500000000000000000")
	require.NoError(t, err)
}

func TestUpdateDisputeResolved_NotFound(t *testing.T) {
	gdb, mock := newMockDB(t)

	mock.ExpectExec(`UPDATE "tasks"`).
		WillReturnResult(sqlmock.NewResult(0, 0))

	err := UpdateDisputeResolved(context.Background(), gdb, 1, 42, "500000000000000000", "500000000000000000")
	assert.ErrorIs(t, err, ErrNotFound)
}

func TestRecordWithdrawal_OK(t *testing.T) {
	gdb, mock := newMockDB(t)

	mock.ExpectQuery(`INSERT INTO "withdrawals"`).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(1)))

	err := RecordWithdrawal(context.Background(), gdb, 1, "0xRecipient", "1000000000000000000", 100, "0xtxhash")
	require.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestRecordWithdrawal_DBError(t *testing.T) {
	gdb, mock := newMockDB(t)

	mock.ExpectQuery(`INSERT INTO "withdrawals"`).
		WillReturnError(errDBConn)

	err := RecordWithdrawal(context.Background(), gdb, 1, "0xRecipient", "1000", 100, "0xtxhash")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "RecordWithdrawal")
}

// sentinel error used only in tests to simulate DB failures.
var errDBConn = fmt.Errorf("connection refused")
