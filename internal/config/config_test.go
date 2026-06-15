package config

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pgdriver "gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestLoad_MissingDatabaseURL(t *testing.T) {
	t.Setenv("DATABASE_URL", "")

	_, err := Load()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "DATABASE_URL")
}

func TestLoad_PollIntervalDefault(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://localhost/test")
	t.Setenv("POLLING_INTERVAL_MS", "")

	cfg, err := Load()
	require.NoError(t, err)
	assert.Equal(t, 5000, cfg.PollIntervalMs)
}

func TestLoad_InvalidPollInterval(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://localhost/test")
	t.Setenv("POLLING_INTERVAL_MS", "not-a-number")

	cfg, err := Load()
	require.NoError(t, err)
	assert.Equal(t, 5000, cfg.PollIntervalMs)
}

func TestLoad_NonPositivePollInterval(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://localhost/test")
	t.Setenv("POLLING_INTERVAL_MS", "0")

	_, err := Load()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "POLLING_INTERVAL_MS")
}

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

func TestLoadChains_Success(t *testing.T) {
	gdb, mock := newTestMockDB(t)

	rows := sqlmock.NewRows([]string{"chain_id", "rpc_url", "contract_address", "start_block", "enabled"}).
		AddRow(11155111, "http://rpc1", "0x1111111111111111111111111111111111111111", 0, true)
	mock.ExpectQuery(`SELECT \* FROM "chain_config" WHERE enabled = \$1 ORDER BY chain_id`).
		WithArgs(true).
		WillReturnRows(rows)

	chains, err := LoadChains(context.Background(), gdb)
	require.NoError(t, err)
	require.Len(t, chains, 1)
	assert.Equal(t, int64(11155111), chains[0].ChainID)
}

func TestLoadChains_QueryError(t *testing.T) {
	gdb, mock := newTestMockDB(t)

	mock.ExpectQuery(`SELECT \* FROM "chain_config"`).
		WillReturnError(errors.New("connection refused"))

	_, err := LoadChains(context.Background(), gdb)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "chain_config")
}

func TestLoadChains_Empty(t *testing.T) {
	gdb, mock := newTestMockDB(t)

	rows := sqlmock.NewRows([]string{"chain_id", "rpc_url", "contract_address", "start_block", "enabled"})
	mock.ExpectQuery(`SELECT \* FROM "chain_config"`).
		WillReturnRows(rows)

	_, err := LoadChains(context.Background(), gdb)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no enabled chains")
}

func TestLoadChains_NonPositiveChainID(t *testing.T) {
	gdb, mock := newTestMockDB(t)

	rows := sqlmock.NewRows([]string{"chain_id", "rpc_url", "contract_address", "start_block", "enabled"}).
		AddRow(0, "http://rpc1", "0x1111111111111111111111111111111111111111", 0, true)
	mock.ExpectQuery(`SELECT \* FROM "chain_config"`).
		WillReturnRows(rows)

	_, err := LoadChains(context.Background(), gdb)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "chain_id")
}

func TestLoadChains_MissingRPCURL(t *testing.T) {
	gdb, mock := newTestMockDB(t)

	rows := sqlmock.NewRows([]string{"chain_id", "rpc_url", "contract_address", "start_block", "enabled"}).
		AddRow(1, "", "0x1111111111111111111111111111111111111111", 0, true)
	mock.ExpectQuery(`SELECT \* FROM "chain_config"`).
		WillReturnRows(rows)

	_, err := LoadChains(context.Background(), gdb)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "rpc_url")
}

func TestLoadChains_InvalidContractAddress(t *testing.T) {
	gdb, mock := newTestMockDB(t)

	rows := sqlmock.NewRows([]string{"chain_id", "rpc_url", "contract_address", "start_block", "enabled"}).
		AddRow(1, "http://rpc1", "not-an-address", 0, true)
	mock.ExpectQuery(`SELECT \* FROM "chain_config"`).
		WillReturnRows(rows)

	_, err := LoadChains(context.Background(), gdb)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "contract_address")
}
