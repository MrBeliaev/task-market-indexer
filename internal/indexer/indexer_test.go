package indexer

import (
	"context"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taskmarket/indexer/internal/config"
)

func TestNew_ValidConfig(t *testing.T) {
	gdb, _ := newTestMockDB(t)

	cfg := &config.Config{PollIntervalMs: 1000, ChainReloadIntervalMs: 30000}
	idx, err := New(cfg, gdb)
	require.NoError(t, err)
	require.NotNil(t, idx)
	assert.Empty(t, idx.active)
}

func TestNew_InvalidABI(t *testing.T) {
	gdb, _ := newTestMockDB(t)

	original := abiJSON
	abiJSON = []byte("not-valid-json")
	t.Cleanup(func() { abiJSON = original })

	cfg := &config.Config{PollIntervalMs: 1000, ChainReloadIntervalMs: 30000}
	_, err := New(cfg, gdb)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse ABI")
}

func TestReconcile_StartsNewChain(t *testing.T) {
	gdb, mock := newTestMockDB(t)

	rows := sqlmock.NewRows([]string{"chain_id", "rpc_url", "contract_address", "start_block", "enabled"}).
		AddRow(1, "http://localhost:9999", "0x1111111111111111111111111111111111111111", 0, true)
	mock.ExpectQuery(`SELECT \* FROM "chain_config" WHERE enabled`).
		WithArgs(true).
		WillReturnRows(rows)

	cfg := &config.Config{PollIntervalMs: 1000, ChainReloadIntervalMs: 30000}
	idx, err := New(cfg, gdb)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	idx.reconcile(ctx)

	idx.mu.Lock()
	_, active := idx.active[1]
	idx.mu.Unlock()
	assert.True(t, active, "chain 1 should be active after reconcile")

	// Clean up goroutine.
	cancel()
	idx.stopAll()
}

func TestReconcile_StopsDisabledChain(t *testing.T) {
	gdb, mock := newTestMockDB(t)

	// First reconcile: chain 1 enabled.
	rows1 := sqlmock.NewRows([]string{"chain_id", "rpc_url", "contract_address", "start_block", "enabled"}).
		AddRow(1, "http://localhost:9999", "0x1111111111111111111111111111111111111111", 0, true)
	mock.ExpectQuery(`SELECT \* FROM "chain_config" WHERE enabled`).
		WithArgs(true).
		WillReturnRows(rows1)

	// Second reconcile: no chains (disabled).
	rows2 := sqlmock.NewRows([]string{"chain_id", "rpc_url", "contract_address", "start_block", "enabled"})
	mock.ExpectQuery(`SELECT \* FROM "chain_config" WHERE enabled`).
		WithArgs(true).
		WillReturnRows(rows2)

	cfg := &config.Config{PollIntervalMs: 1000, ChainReloadIntervalMs: 30000}
	idx, err := New(cfg, gdb)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	idx.reconcile(ctx)

	idx.mu.Lock()
	_, active := idx.active[1]
	idx.mu.Unlock()
	require.True(t, active, "chain 1 should be active after first reconcile")

	// Second reconcile removes chain 1.
	idx.reconcile(ctx)

	// Wait a moment for goroutine cleanup (reconcile waits on <-done).
	assert.Eventually(t, func() bool {
		idx.mu.Lock()
		_, stillActive := idx.active[1]
		idx.mu.Unlock()
		return !stillActive
	}, time.Second, 10*time.Millisecond, "chain 1 should be stopped after second reconcile")
}

func TestReconcile_RestartsOnConfigChange(t *testing.T) {
	gdb, mock := newTestMockDB(t)

	rows1 := sqlmock.NewRows([]string{"chain_id", "rpc_url", "contract_address", "start_block", "enabled"}).
		AddRow(1, "http://old-rpc:9999", "0x1111111111111111111111111111111111111111", 0, true)
	mock.ExpectQuery(`SELECT \* FROM "chain_config" WHERE enabled`).
		WithArgs(true).
		WillReturnRows(rows1)

	rows2 := sqlmock.NewRows([]string{"chain_id", "rpc_url", "contract_address", "start_block", "enabled"}).
		AddRow(1, "http://new-rpc:9999", "0x1111111111111111111111111111111111111111", 0, true)
	mock.ExpectQuery(`SELECT \* FROM "chain_config" WHERE enabled`).
		WithArgs(true).
		WillReturnRows(rows2)

	cfg := &config.Config{PollIntervalMs: 1000, ChainReloadIntervalMs: 30000}
	idx, err := New(cfg, gdb)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	idx.reconcile(ctx)

	idx.mu.Lock()
	firstDone := idx.active[1].done
	idx.mu.Unlock()

	// Second reconcile: RPC changed → old goroutine stopped, new one started.
	idx.reconcile(ctx)

	idx.mu.Lock()
	rc, active := idx.active[1]
	var newDone <-chan struct{}
	if active {
		newDone = rc.done
	}
	idx.mu.Unlock()

	require.True(t, active, "chain 1 should still be active with new config")
	assert.NotEqual(t, firstDone, newDone, "done channel should be a new goroutine")

	cancel()
	idx.stopAll()
}

func TestReconcile_DBError(t *testing.T) {
	gdb, mock := newTestMockDB(t)

	mock.ExpectQuery(`SELECT \* FROM "chain_config" WHERE enabled`).
		WithArgs(true).
		WillReturnError(assert.AnError)

	cfg := &config.Config{PollIntervalMs: 1000, ChainReloadIntervalMs: 30000}
	idx, err := New(cfg, gdb)
	require.NoError(t, err)

	ctx := context.Background()
	// Should not panic; active remains empty.
	idx.reconcile(ctx)

	idx.mu.Lock()
	count := len(idx.active)
	idx.mu.Unlock()
	assert.Equal(t, 0, count)
}

func TestStatusMap_AllValues(t *testing.T) {
	expected := map[uint8]string{
		0: "OPEN", 1: "ASSIGNED", 2: "IN_PROGRESS",
		3: "UNDER_REVIEW", 4: "COMPLETED", 5: "DISPUTED", 6: "CANCELLED",
	}
	assert.Equal(t, expected, statusMap)
}

func TestABIJSON_ContainsRequiredEvents(t *testing.T) {
	abiStr := string(abiJSON)
	for _, event := range []string{
		"TaskCreated", "TaskAssigned", "TaskStatusChanged",
		"CompletionConfirmed", "TaskCompleted", "TaskDisputed",
		"DisputeResolved", "Withdrawn", "FeeBpsUpdated", "FeeRecipientUpdated",
	} {
		assert.Contains(t, abiStr, event, "ABI missing event: %s", event)
	}
}
