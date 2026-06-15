package indexer

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taskmarket/indexer/internal/config"
	"github.com/taskmarket/indexer/internal/db"
)

func TestNew_Success(t *testing.T) {
	gdb, _ := newTestMockDB(t)

	cfg := &config.Config{PollIntervalMs: 1000}
	chains := []db.ChainConfig{
		{
			ChainID:         1,
			RPCURL:          "http://localhost:8545",
			ContractAddress: "0xeb605C381323597825999ed48595A4CFCccBbaA0",
			StartBlock:      0,
		},
	}

	idx, err := New(cfg, chains, gdb)
	require.NoError(t, err)
	require.NotNil(t, idx)
	assert.Len(t, idx.chains, 1)
	assert.NotNil(t, idx.chains[0].handler)
	assert.Equal(t, 1000, idx.chains[0].pollMs)
}

func TestNew_MultiChain(t *testing.T) {
	gdb, _ := newTestMockDB(t)

	cfg := &config.Config{PollIntervalMs: 2000}
	chains := []db.ChainConfig{
		{ChainID: 1, RPCURL: "http://rpc1", ContractAddress: "0x1111111111111111111111111111111111111111"},
		{ChainID: 11155111, RPCURL: "http://rpc2", ContractAddress: "0x2222222222222222222222222222222222222222"},
	}

	idx, err := New(cfg, chains, gdb)
	require.NoError(t, err)
	assert.Len(t, idx.chains, 2)
	assert.Equal(t, int64(1), idx.chains[0].chain.ChainID)
	assert.Equal(t, int64(11155111), idx.chains[1].chain.ChainID)
}

func TestNew_InvalidABI(t *testing.T) {
	gdb, _ := newTestMockDB(t)

	// Temporarily replace the embedded ABI with invalid JSON
	original := abiJSON
	abiJSON = []byte("not-valid-json")
	t.Cleanup(func() { abiJSON = original })

	cfg := &config.Config{}
	chains := []db.ChainConfig{
		{ChainID: 1, RPCURL: "http://localhost:8545", ContractAddress: "0x1111111111111111111111111111111111111111"},
	}

	_, err := New(cfg, chains, gdb)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse ABI")
}

func TestNew_EmptyChains(t *testing.T) {
	gdb, _ := newTestMockDB(t)

	cfg := &config.Config{PollIntervalMs: 1000}

	idx, err := New(cfg, []db.ChainConfig{}, gdb)
	require.NoError(t, err)
	assert.Empty(t, idx.chains)
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
