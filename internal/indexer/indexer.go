package indexer

import (
	"context"
	_ "embed"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"golang.org/x/sync/errgroup"
	"gorm.io/gorm"

	"github.com/taskmarket/indexer/internal/config"
	"github.com/taskmarket/indexer/internal/db"
)

//go:embed abi/taskmarket.json
var abiJSON []byte

const maxBlockRange uint64 = 1000

// MultiIndexer runs one ChainIndexer per configured chain concurrently.
type MultiIndexer struct {
	chains []*ChainIndexer
}

// ChainIndexer handles polling for a single EVM chain.
type ChainIndexer struct {
	chain   db.ChainConfig
	gdb     *gorm.DB
	handler *eventHandler
	logger  *slog.Logger
	pollMs  int
}

// New parses the shared ABI once and constructs a ChainIndexer for each chain.
func New(cfg *config.Config, dbChains []db.ChainConfig, gdb *gorm.DB) (*MultiIndexer, error) {
	contractABI, err := abi.JSON(strings.NewReader(string(abiJSON)))
	if err != nil {
		return nil, fmt.Errorf("parse ABI: %w", err)
	}

	chains := make([]*ChainIndexer, 0, len(dbChains))
	for _, chain := range dbChains {
		chains = append(chains, &ChainIndexer{
			chain: chain,
			gdb:   gdb,
			handler: &eventHandler{
				contractABI:  contractABI,
				contractAddr: common.HexToAddress(chain.ContractAddress),
				gdb:          gdb,
			},
			logger: slog.With(
				"chain_id", chain.ChainID,
				"contract", chain.ContractAddress,
			),
			pollMs: cfg.PollIntervalMs,
		})
	}
	return &MultiIndexer{chains: chains}, nil
}

// Run starts all chain indexers concurrently.
// If any chain returns a non-context error, the whole group is cancelled.
func (m *MultiIndexer) Run(ctx context.Context) error {
	g, gCtx := errgroup.WithContext(ctx)
	for _, ci := range m.chains {
		g.Go(func() error {
			ci.logger.Info("chain indexer starting",
				"rpc", ci.chain.RPCURL,
				"start_block", ci.chain.StartBlock,
			)
			return ci.run(gCtx)
		})
	}
	return g.Wait()
}

// run connects to the chain RPC and polls until ctx is cancelled,
// reconnecting automatically on connection failure.
func (ci *ChainIndexer) run(ctx context.Context) error {
	pollInterval := time.Duration(ci.pollMs) * time.Millisecond

	for ctx.Err() == nil {
		client, err := ci.dialWithRetry(ctx)
		if err != nil {
			return err
		}
		ci.logger.Info("connected to RPC", "url", ci.chain.RPCURL)

		sessionErr := ci.runSession(ctx, client)
		client.Close()

		if ctx.Err() != nil {
			return ctx.Err()
		}
		ci.logger.Warn("session ended, reconnecting",
			"error", sessionErr,
			"delay", pollInterval.String(),
		)
		select {
		case <-ctx.Done():
		case <-time.After(pollInterval):
		}
	}
	return ctx.Err()
}

// dialWithRetry connects to the chain RPC with exponential backoff.
func (ci *ChainIndexer) dialWithRetry(ctx context.Context) (*ethclient.Client, error) {
	const maxAttempts = 10
	delay := 2 * time.Second

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		client, err := ethclient.DialContext(ctx, ci.chain.RPCURL)
		if err == nil {
			return client, nil
		}
		ci.logger.Warn("RPC dial failed",
			"attempt", attempt,
			"max", maxAttempts,
			"delay", delay.String(),
			"error", err,
		)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}
		if delay < 60*time.Second {
			delay *= 2
		}
	}
	return nil, fmt.Errorf("chain %d: RPC unreachable after %d attempts", ci.chain.ChainID, maxAttempts)
}

// runSession polls until an unrecoverable error or context cancellation.
func (ci *ChainIndexer) runSession(ctx context.Context, client *ethclient.Client) error {
	ticker := time.NewTicker(time.Duration(ci.pollMs) * time.Millisecond)
	defer ticker.Stop()

	if err := ci.poll(ctx, client); err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := ci.poll(ctx, client); err != nil {
				return err
			}
		}
	}
}

type pollResult struct {
	from, to      uint64
	lag           uint64
	eventsCreated int
	eventsUpdated int
}

// poll fetches the next block batch, processes all event types in parallel,
// and advances the indexed cursor only after all handlers succeed.
func (ci *ChainIndexer) poll(ctx context.Context, client *ethclient.Client) error {
	lastBlock, err := db.GetOrInitLastBlock(ctx, ci.gdb, ci.chain.ChainID, ci.chain.StartBlock)
	if err != nil {
		return fmt.Errorf("get last block: %w", err)
	}

	currentBlock, err := client.BlockNumber(ctx)
	if err != nil {
		return fmt.Errorf("get block number: %w", err)
	}

	fromBlock := lastBlock + 1
	if fromBlock > currentBlock {
		return nil
	}

	toBlock := fromBlock + maxBlockRange - 1
	if toBlock > currentBlock {
		toBlock = currentBlock
	}

	res := &pollResult{
		from: fromBlock,
		to:   toBlock,
		lag:  currentBlock - toBlock,
	}

	g, gCtx := errgroup.WithContext(ctx)

	g.Go(func() error {
		n, err := ci.handler.handleTaskCreated(gCtx, client, ci.chain.ChainID, fromBlock, toBlock)
		res.eventsCreated += n
		return err
	})
	g.Go(func() error {
		n, err := ci.handler.handleTaskAssigned(gCtx, client, ci.chain.ChainID, fromBlock, toBlock)
		res.eventsUpdated += n
		return err
	})
	g.Go(func() error {
		n, err := ci.handler.handleTaskStatusChanged(gCtx, client, ci.chain.ChainID, fromBlock, toBlock)
		res.eventsUpdated += n
		return err
	})
	g.Go(func() error {
		n, err := ci.handler.handleCompletionConfirmed(gCtx, client, ci.chain.ChainID, fromBlock, toBlock)
		res.eventsUpdated += n
		return err
	})
	g.Go(func() error {
		n, err := ci.handler.handleTaskCompleted(gCtx, client, ci.chain.ChainID, fromBlock, toBlock)
		res.eventsUpdated += n
		return err
	})
	g.Go(func() error {
		n, err := ci.handler.handleTaskDisputed(gCtx, client, ci.chain.ChainID, fromBlock, toBlock)
		res.eventsUpdated += n
		return err
	})
	g.Go(func() error {
		n, err := ci.handler.handleDisputeResolved(gCtx, client, ci.chain.ChainID, fromBlock, toBlock)
		res.eventsUpdated += n
		return err
	})
	g.Go(func() error {
		n, err := ci.handler.handleWithdrawn(gCtx, client, ci.chain.ChainID, fromBlock, toBlock)
		res.eventsUpdated += n
		return err
	})
	g.Go(func() error {
		_, err := ci.handler.handleFeeBpsUpdated(gCtx, client, ci.chain.ChainID, fromBlock, toBlock)
		return err
	})
	g.Go(func() error {
		_, err := ci.handler.handleFeeRecipientUpdated(gCtx, client, ci.chain.ChainID, fromBlock, toBlock)
		return err
	})

	if err := g.Wait(); err != nil {
		return fmt.Errorf("event processing [%d-%d]: %w", fromBlock, toBlock, err)
	}

	if err := db.SetLastBlock(ctx, ci.gdb, ci.chain.ChainID, toBlock); err != nil {
		return err
	}

	ci.logger.Info("poll complete",
		"from", res.from,
		"to", res.to,
		"lag_blocks", res.lag,
		"new_tasks", res.eventsCreated,
		"state_updates", res.eventsUpdated,
	)
	return nil
}
