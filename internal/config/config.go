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
	DatabaseURL           string
	PollIntervalMs        int
	ChainReloadIntervalMs int
}

// Load reads global config from environment variables.
func Load() (*Config, error) {
	cfg := &Config{
		DatabaseURL:           os.Getenv("DATABASE_URL"),
		PollIntervalMs:        5000,
		ChainReloadIntervalMs: 30000,
	}
	if v := os.Getenv("POLLING_INTERVAL_MS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.PollIntervalMs = n
		}
	}
	if v := os.Getenv("CHAIN_RELOAD_INTERVAL_MS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.ChainReloadIntervalMs = n
		}
	}

	if cfg.DatabaseURL == "" {
		return nil, fmt.Errorf("DATABASE_URL is required")
	}
	if cfg.PollIntervalMs <= 0 {
		return nil, fmt.Errorf("POLLING_INTERVAL_MS must be positive, got %d", cfg.PollIntervalMs)
	}
	if cfg.ChainReloadIntervalMs <= 0 {
		return nil, fmt.Errorf("CHAIN_RELOAD_INTERVAL_MS must be positive, got %d", cfg.ChainReloadIntervalMs)
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

// ValidateChain checks that a chain config entry has all required fields.
func ValidateChain(c db.ChainConfig) error {
	if c.ChainID <= 0 {
		return fmt.Errorf("chain_id must be positive")
	}
	if c.RPCURL == "" {
		return fmt.Errorf("rpc_url is required")
	}
	if !common.IsHexAddress(c.ContractAddress) {
		return fmt.Errorf("contract_address %q is not a valid address", c.ContractAddress)
	}
	return nil
}

func validateChain(c db.ChainConfig, idx int) error {
	if err := ValidateChain(c); err != nil {
		return fmt.Errorf("chain_config[%d]: %w", idx, err)
	}
	return nil
}
