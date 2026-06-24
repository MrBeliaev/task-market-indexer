package indexer

import (
	"context"
	"errors"
	"math/big"
	"strings"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pgdriver "gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/taskmarket/indexer/internal/db"
)

// mockFilterer implements ethereum.LogFilterer for tests.
type mockFilterer struct {
	logs []ethtypes.Log
	err  error
}

func (m *mockFilterer) FilterLogs(_ context.Context, _ ethereum.FilterQuery) ([]ethtypes.Log, error) {
	return m.logs, m.err
}

func (m *mockFilterer) SubscribeFilterLogs(_ context.Context, _ ethereum.FilterQuery, _ chan<- ethtypes.Log) (ethereum.Subscription, error) {
	return nil, errors.New("not supported")
}

// newTestMockDB creates a GORM DB backed by sqlmock for event handler tests.
func newTestMockDB(t *testing.T) (*gorm.DB, sqlmock.Sqlmock) {
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

// newTestHandler builds an eventHandler with a mock GORM DB.
func newTestHandler(t *testing.T) (*eventHandler, sqlmock.Sqlmock) {
	t.Helper()

	gdb, mock := newTestMockDB(t)
	contractABI, err := abi.JSON(strings.NewReader(string(abiJSON)))
	require.NoError(t, err)

	return &eventHandler{
		contractABI:  contractABI,
		contractAddr: common.HexToAddress("0xfaD6d58168C0e3387Ac9B4A70818a84dBA6c2b78"),
		gdb:          gdb,
	}, mock
}

// packEventData encodes the non-indexed fields of the named event.
func packEventData(t *testing.T, contractABI abi.ABI, eventName string, args ...interface{}) []byte {
	t.Helper()
	event, ok := contractABI.Events[eventName]
	require.True(t, ok, "event %s not found in ABI", eventName)
	data, err := event.Inputs.NonIndexed().Pack(args...)
	require.NoError(t, err)
	return data
}

// makeLog creates a types.Log with proper topic encoding.
func makeLog(contractABI abi.ABI, eventName string, extraTopics []common.Hash, data []byte) ethtypes.Log {
	event := contractABI.Events[eventName]
	topics := make([]common.Hash, 1, 1+len(extraTopics))
	topics[0] = event.ID
	topics = append(topics, extraTopics...)

	return ethtypes.Log{
		Topics:      topics,
		Data:        data,
		BlockNumber: 100,
		TxHash:      common.HexToHash("0xabc123"),
	}
}

// ── ShortenAddr ──────────────────────────────────────────────────────────────

func TestShortenAddr_Long(t *testing.T) {
	result := shortenAddr("0x1234567890abcdef1234")
	assert.Contains(t, result, "…")
	assert.True(t, strings.HasPrefix(result, "0x123456"))
	assert.True(t, strings.HasSuffix(result, "1234"))
}

func TestShortenAddr_Short(t *testing.T) {
	result := shortenAddr("0xshort")
	assert.Equal(t, "0xshort", result)
}

func TestShortenAddr_Uppercase(t *testing.T) {
	result := shortenAddr("0xABCDEF")
	assert.Equal(t, "0xabcdef", result)
}

// ── FilterLogs ────────────────────────────────────────────────────────────────

func TestFilterLogs_UnknownEvent(t *testing.T) {
	h, _ := newTestHandler(t)

	_, err := h.filterLogs(context.Background(), &mockFilterer{}, "NonExistentEvent", 0, 100)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "NonExistentEvent")
}

func TestFilterLogs_FilterError(t *testing.T) {
	h, _ := newTestHandler(t)
	errRPC := errors.New("rpc error")

	_, err := h.filterLogs(context.Background(), &mockFilterer{err: errRPC}, "TaskCreated", 0, 100)
	require.Error(t, err)
	assert.ErrorIs(t, err, errRPC)
}

// ── TaskCreated ───────────────────────────────────────────────────────────────

func TestHandleTaskCreated_Empty(t *testing.T) {
	h, mock := newTestHandler(t)

	n, err := h.handleTaskCreated(context.Background(), &mockFilterer{}, 1, 0, 100)
	require.NoError(t, err)
	assert.Equal(t, 0, n)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestHandleTaskCreated_OK(t *testing.T) {
	h, mock := newTestHandler(t)

	reward := big.NewInt(1_000_000_000_000_000_000)
	deadline := big.NewInt(9_999_999_999)
	var metaHash [32]byte
	copy(metaHash[:], "testhash")

	data := packEventData(t, h.contractABI, "TaskCreated", reward, deadline, metaHash)
	log := makeLog(h.contractABI, "TaskCreated", []common.Hash{
		common.BigToHash(big.NewInt(1)),
		common.BytesToHash(common.HexToAddress("0xabc").Bytes()),
	}, data)

	mock.ExpectQuery(`INSERT INTO "tasks"`).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(1)))

	n, err := h.handleTaskCreated(context.Background(), &mockFilterer{logs: []ethtypes.Log{log}}, 1, 0, 100)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestHandleTaskCreated_DBError(t *testing.T) {
	h, mock := newTestHandler(t)

	reward := big.NewInt(1_000_000_000_000_000_000)
	deadline := big.NewInt(9_999_999_999)
	var metaHash [32]byte
	data := packEventData(t, h.contractABI, "TaskCreated", reward, deadline, metaHash)
	log := makeLog(h.contractABI, "TaskCreated", []common.Hash{
		common.BigToHash(big.NewInt(1)),
		common.BytesToHash(common.HexToAddress("0xabc").Bytes()),
	}, data)

	mock.ExpectQuery(`INSERT INTO "tasks"`).
		WillReturnError(errors.New("db error"))

	_, err := h.handleTaskCreated(context.Background(), &mockFilterer{logs: []ethtypes.Log{log}}, 1, 0, 100)
	require.Error(t, err)
}

func TestHandleTaskCreated_TooFewTopics(t *testing.T) {
	h, mock := newTestHandler(t)

	event := h.contractABI.Events["TaskCreated"]
	log := ethtypes.Log{Topics: []common.Hash{event.ID}, BlockNumber: 100}

	n, err := h.handleTaskCreated(context.Background(), &mockFilterer{logs: []ethtypes.Log{log}}, 1, 0, 100)
	require.NoError(t, err)
	assert.Equal(t, 0, n)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// ── TaskAssigned ──────────────────────────────────────────────────────────────

func TestHandleTaskAssigned_OK(t *testing.T) {
	h, mock := newTestHandler(t)

	log := makeLog(h.contractABI, "TaskAssigned", []common.Hash{
		common.BigToHash(big.NewInt(1)),
		common.BytesToHash(common.HexToAddress("0xexec").Bytes()),
	}, nil)

	mock.ExpectExec(`UPDATE "tasks"`).
		WillReturnResult(sqlmock.NewResult(0, 1))

	n, err := h.handleTaskAssigned(context.Background(), &mockFilterer{logs: []ethtypes.Log{log}}, 1, 0, 100)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
}

func TestHandleTaskAssigned_NotFound(t *testing.T) {
	h, mock := newTestHandler(t)

	log := makeLog(h.contractABI, "TaskAssigned", []common.Hash{
		common.BigToHash(big.NewInt(99)),
		common.BytesToHash(common.HexToAddress("0xexec").Bytes()),
	}, nil)

	mock.ExpectExec(`UPDATE "tasks"`).
		WillReturnResult(sqlmock.NewResult(0, 0))

	n, err := h.handleTaskAssigned(context.Background(), &mockFilterer{logs: []ethtypes.Log{log}}, 1, 0, 100)
	require.NoError(t, err)
	assert.Equal(t, 0, n)
}

func TestHandleTaskAssigned_DBError(t *testing.T) {
	h, mock := newTestHandler(t)

	log := makeLog(h.contractABI, "TaskAssigned", []common.Hash{
		common.BigToHash(big.NewInt(1)),
		common.BytesToHash(common.HexToAddress("0xexec").Bytes()),
	}, nil)

	mock.ExpectExec(`UPDATE "tasks"`).
		WillReturnError(errors.New("db error"))

	_, err := h.handleTaskAssigned(context.Background(), &mockFilterer{logs: []ethtypes.Log{log}}, 1, 0, 100)
	require.Error(t, err)
}

// ── TaskStatusChanged ─────────────────────────────────────────────────────────

func TestHandleTaskStatusChanged_OK(t *testing.T) {
	h, mock := newTestHandler(t)

	data := packEventData(t, h.contractABI, "TaskStatusChanged", uint8(0), uint8(1))
	log := makeLog(h.contractABI, "TaskStatusChanged", []common.Hash{
		common.BigToHash(big.NewInt(1)),
	}, data)

	mock.ExpectExec(`UPDATE "tasks"`).
		WillReturnResult(sqlmock.NewResult(0, 1))

	n, err := h.handleTaskStatusChanged(context.Background(), &mockFilterer{logs: []ethtypes.Log{log}}, 1, 0, 100)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
}

func TestHandleTaskStatusChanged_UnknownStatus(t *testing.T) {
	h, mock := newTestHandler(t)

	data := packEventData(t, h.contractABI, "TaskStatusChanged", uint8(0), uint8(99))
	log := makeLog(h.contractABI, "TaskStatusChanged", []common.Hash{
		common.BigToHash(big.NewInt(1)),
	}, data)

	n, err := h.handleTaskStatusChanged(context.Background(), &mockFilterer{logs: []ethtypes.Log{log}}, 1, 0, 100)
	require.NoError(t, err)
	assert.Equal(t, 0, n)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestHandleTaskStatusChanged_NotFound(t *testing.T) {
	h, mock := newTestHandler(t)

	data := packEventData(t, h.contractABI, "TaskStatusChanged", uint8(0), uint8(2))
	log := makeLog(h.contractABI, "TaskStatusChanged", []common.Hash{
		common.BigToHash(big.NewInt(1)),
	}, data)

	mock.ExpectExec(`UPDATE "tasks"`).
		WillReturnResult(sqlmock.NewResult(0, 0))

	n, err := h.handleTaskStatusChanged(context.Background(), &mockFilterer{logs: []ethtypes.Log{log}}, 1, 0, 100)
	require.NoError(t, err)
	assert.Equal(t, 0, n)
}

// ── CompletionConfirmed ───────────────────────────────────────────────────────

func TestHandleCompletionConfirmed_OK(t *testing.T) {
	h, mock := newTestHandler(t)

	data := packEventData(t, h.contractABI, "CompletionConfirmed", true, false)
	log := makeLog(h.contractABI, "CompletionConfirmed", []common.Hash{
		common.BigToHash(big.NewInt(1)),
		common.BytesToHash(common.HexToAddress("0xclient").Bytes()),
	}, data)

	mock.ExpectExec(`UPDATE "tasks"`).
		WillReturnResult(sqlmock.NewResult(0, 1))

	n, err := h.handleCompletionConfirmed(context.Background(), &mockFilterer{logs: []ethtypes.Log{log}}, 1, 0, 100)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
}

func TestHandleCompletionConfirmed_NotFound(t *testing.T) {
	h, mock := newTestHandler(t)

	data := packEventData(t, h.contractABI, "CompletionConfirmed", true, true)
	log := makeLog(h.contractABI, "CompletionConfirmed", []common.Hash{
		common.BigToHash(big.NewInt(1)),
		common.BytesToHash(common.HexToAddress("0xclient").Bytes()),
	}, data)

	mock.ExpectExec(`UPDATE "tasks"`).
		WillReturnResult(sqlmock.NewResult(0, 0))

	n, err := h.handleCompletionConfirmed(context.Background(), &mockFilterer{logs: []ethtypes.Log{log}}, 1, 0, 100)
	require.NoError(t, err)
	assert.Equal(t, 0, n)
}

// ── TaskCompleted ─────────────────────────────────────────────────────────────

func TestHandleTaskCompleted_OK(t *testing.T) {
	h, mock := newTestHandler(t)

	payout := big.NewInt(950_000_000_000_000_000)
	fee := big.NewInt(50_000_000_000_000_000)
	data := packEventData(t, h.contractABI, "TaskCompleted", payout, fee)
	log := makeLog(h.contractABI, "TaskCompleted", []common.Hash{
		common.BigToHash(big.NewInt(1)),
		common.BytesToHash(common.HexToAddress("0xexec").Bytes()),
	}, data)

	mock.ExpectExec(`UPDATE "tasks"`).
		WillReturnResult(sqlmock.NewResult(0, 1))

	n, err := h.handleTaskCompleted(context.Background(), &mockFilterer{logs: []ethtypes.Log{log}}, 1, 0, 100)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
}

func TestHandleTaskCompleted_NotFound(t *testing.T) {
	h, mock := newTestHandler(t)

	payout := big.NewInt(1_000_000_000_000_000_000)
	fee := big.NewInt(0)
	data := packEventData(t, h.contractABI, "TaskCompleted", payout, fee)
	log := makeLog(h.contractABI, "TaskCompleted", []common.Hash{
		common.BigToHash(big.NewInt(99)),
		common.BytesToHash(common.HexToAddress("0xexec").Bytes()),
	}, data)

	mock.ExpectExec(`UPDATE "tasks"`).
		WillReturnResult(sqlmock.NewResult(0, 0))

	n, err := h.handleTaskCompleted(context.Background(), &mockFilterer{logs: []ethtypes.Log{log}}, 1, 0, 100)
	require.NoError(t, err)
	assert.Equal(t, 0, n)
}

// ── TaskDisputed ──────────────────────────────────────────────────────────────

func TestHandleTaskDisputed_OK(t *testing.T) {
	h, mock := newTestHandler(t)

	log := makeLog(h.contractABI, "TaskDisputed", []common.Hash{
		common.BigToHash(big.NewInt(1)),
		common.BytesToHash(common.HexToAddress("0xclient").Bytes()),
	}, nil)

	mock.ExpectExec(`UPDATE "tasks"`).
		WillReturnResult(sqlmock.NewResult(0, 1))

	n, err := h.handleTaskDisputed(context.Background(), &mockFilterer{logs: []ethtypes.Log{log}}, 1, 0, 100)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
}

func TestHandleTaskDisputed_NotFound(t *testing.T) {
	h, mock := newTestHandler(t)

	log := makeLog(h.contractABI, "TaskDisputed", []common.Hash{
		common.BigToHash(big.NewInt(99)),
		common.BytesToHash(common.HexToAddress("0xclient").Bytes()),
	}, nil)

	mock.ExpectExec(`UPDATE "tasks"`).
		WillReturnResult(sqlmock.NewResult(0, 0))

	n, err := h.handleTaskDisputed(context.Background(), &mockFilterer{logs: []ethtypes.Log{log}}, 1, 0, 100)
	require.NoError(t, err)
	assert.Equal(t, 0, n)
}

// ── DisputeResolved ───────────────────────────────────────────────────────────

func TestHandleDisputeResolved_OK(t *testing.T) {
	h, mock := newTestHandler(t)

	clientRefund := big.NewInt(500_000_000_000_000_000)
	executorPayout := big.NewInt(500_000_000_000_000_000)
	fee := big.NewInt(25_000_000_000_000_000)
	data := packEventData(t, h.contractABI, "DisputeResolved", clientRefund, executorPayout, fee)
	log := makeLog(h.contractABI, "DisputeResolved", []common.Hash{
		common.BigToHash(big.NewInt(1)),
		common.BytesToHash(common.HexToAddress("0xadmin").Bytes()),
	}, data)

	mock.ExpectExec(`UPDATE "tasks"`).
		WillReturnResult(sqlmock.NewResult(0, 1))

	n, err := h.handleDisputeResolved(context.Background(), &mockFilterer{logs: []ethtypes.Log{log}}, 1, 0, 100)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
}

func TestHandleDisputeResolved_NotFound(t *testing.T) {
	h, mock := newTestHandler(t)

	clientRefund := big.NewInt(1_000_000_000_000_000_000)
	executorPayout := big.NewInt(0)
	fee := big.NewInt(25_000_000_000_000_000)
	data := packEventData(t, h.contractABI, "DisputeResolved", clientRefund, executorPayout, fee)
	log := makeLog(h.contractABI, "DisputeResolved", []common.Hash{
		common.BigToHash(big.NewInt(99)),
		common.BytesToHash(common.HexToAddress("0xadmin").Bytes()),
	}, data)

	mock.ExpectExec(`UPDATE "tasks"`).
		WillReturnResult(sqlmock.NewResult(0, 0))

	n, err := h.handleDisputeResolved(context.Background(), &mockFilterer{logs: []ethtypes.Log{log}}, 1, 0, 100)
	require.NoError(t, err)
	assert.Equal(t, 0, n)
}

// ── Withdrawn ─────────────────────────────────────────────────────────────────

func TestHandleWithdrawn_OK(t *testing.T) {
	h, mock := newTestHandler(t)

	amount := big.NewInt(1_000_000_000_000_000_000)
	data := packEventData(t, h.contractABI, "Withdrawn", amount)
	log := makeLog(h.contractABI, "Withdrawn", []common.Hash{
		common.BytesToHash(common.HexToAddress("0xrecipient").Bytes()),
	}, data)

	mock.ExpectQuery(`INSERT INTO "withdrawals"`).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(1)))

	n, err := h.handleWithdrawn(context.Background(), &mockFilterer{logs: []ethtypes.Log{log}}, 1, 0, 100)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestHandleWithdrawn_DBError(t *testing.T) {
	h, mock := newTestHandler(t)

	amount := big.NewInt(1_000_000_000_000_000_000)
	data := packEventData(t, h.contractABI, "Withdrawn", amount)
	log := makeLog(h.contractABI, "Withdrawn", []common.Hash{
		common.BytesToHash(common.HexToAddress("0xrecipient").Bytes()),
	}, data)

	mock.ExpectQuery(`INSERT INTO "withdrawals"`).
		WillReturnError(errors.New("db error"))

	_, err := h.handleWithdrawn(context.Background(), &mockFilterer{logs: []ethtypes.Log{log}}, 1, 0, 100)
	require.Error(t, err)
}

// ── FeeBpsUpdated ─────────────────────────────────────────────────────────────

func TestHandleFeeBpsUpdated_OK(t *testing.T) {
	h, _ := newTestHandler(t)

	data := packEventData(t, h.contractABI, "FeeBpsUpdated", big.NewInt(200), big.NewInt(300))
	log := makeLog(h.contractABI, "FeeBpsUpdated", nil, data)

	n, err := h.handleFeeBpsUpdated(context.Background(), &mockFilterer{logs: []ethtypes.Log{log}}, 1, 0, 100)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
}

func TestHandleFeeBpsUpdated_Empty(t *testing.T) {
	h, _ := newTestHandler(t)

	n, err := h.handleFeeBpsUpdated(context.Background(), &mockFilterer{}, 1, 0, 100)
	require.NoError(t, err)
	assert.Equal(t, 0, n)
}

// ── FeeRecipientUpdated ───────────────────────────────────────────────────────

func TestHandleFeeRecipientUpdated_OK(t *testing.T) {
	h, _ := newTestHandler(t)

	log := makeLog(h.contractABI, "FeeRecipientUpdated", []common.Hash{
		common.BytesToHash(common.HexToAddress("0xold").Bytes()),
		common.BytesToHash(common.HexToAddress("0xnew").Bytes()),
	}, nil)

	n, err := h.handleFeeRecipientUpdated(context.Background(), &mockFilterer{logs: []ethtypes.Log{log}}, 1, 0, 100)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
}

func TestHandleFeeRecipientUpdated_TooFewTopics(t *testing.T) {
	h, _ := newTestHandler(t)

	event := h.contractABI.Events["FeeRecipientUpdated"]
	log := ethtypes.Log{Topics: []common.Hash{event.ID}, BlockNumber: 100}

	n, err := h.handleFeeRecipientUpdated(context.Background(), &mockFilterer{logs: []ethtypes.Log{log}}, 1, 0, 100)
	require.NoError(t, err)
	assert.Equal(t, 0, n)
}

// ── ErrNotFound sentinel ──────────────────────────────────────────────────────

func TestErrNotFound_IsExpected(t *testing.T) {
	assert.Equal(t, "task not found in db", db.ErrNotFound.Error())
}
