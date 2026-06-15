package config

import (
	"context"
	"fmt"
	"os"
	"strconv"

	"github.com/ethereum/go-ethereum/common"
	"gorm.io/gorm"

	"github.com/taskmarket/indexer/internal/db"
)

// Config holds global indexer settings shared by all chains.
type Config struct {
	DatabaseURL    string
	PollIntervalMs int
}

// Load reads global config from environment variables.
func Load() (*Config, error) {
	cfg := &Config{
		DatabaseURL:    os.Getenv("DATABASE_URL"),
		PollIntervalMs: 5000,
	}
	if v := os.Getenv("POLLING_INTERVAL_MS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.PollIntervalMs = n
		}
	}

	if cfg.DatabaseURL == "" {
		return nil, fmt.Errorf("DATABASE_URL is required")
	}
	if cfg.PollIntervalMs <= 0 {
		return nil, fmt.Errorf("POLLING_INTERVAL_MS must be positive, got %d", cfg.PollIntervalMs)
	}

	return cfg, nil
}

// LoadChains loads enabled chains from the chain_config table and validates them.
func LoadChains(ctx context.Context, gdb *gorm.DB) ([]db.ChainConfig, error) {
	chains, err := db.LoadEnabledChains(ctx, gdb)
	if err != nil {
		return nil, err
	}
	if len(chains) == 0 {
		return nil, fmt.Errorf("chain_config: no enabled chains")
	}
	for i, c := range chains {
		if err := validateChain(c, i); err != nil {
			return nil, err
		}
	}
	return chains, nil
}

func validateChain(c db.ChainConfig, idx int) error {
	if c.ChainID <= 0 {
		return fmt.Errorf("chain_config[%d]: chain_id must be positive", idx)
	}
	if c.RPCURL == "" {
		return fmt.Errorf("chain_config[%d]: rpc_url is required", idx)
	}
	if !common.IsHexAddress(c.ContractAddress) {
		return fmt.Errorf("chain_config[%d]: contract_address %q is not a valid address", idx, c.ContractAddress)
	}
	return nil
}
