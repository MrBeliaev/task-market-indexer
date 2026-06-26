package indexer

import (
	"context"
	_ "embed"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
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

// runningChain tracks an active ChainIndexer goroutine.
type runningChain struct {
	cfg    db.ChainConfig
	cancel context.CancelFunc
	done   <-chan struct{}
}

// MultiIndexer manages ChainIndexers dynamically, reconciling with the DB on a timer.
type MultiIndexer struct {
	cfg         *config.Config
	gdb         *gorm.DB
	contractABI abi.ABI
	mu          sync.Mutex
	active      map[int64]*runningChain
}

// ChainIndexer handles polling for a single EVM chain.
type ChainIndexer struct {
	chain   db.ChainConfig
	gdb     *gorm.DB
	handler *eventHandler
	logger  *slog.Logger
	pollMs  int
}

// New parses the shared ABI and initialises the MultiIndexer.
// Chains are loaded from the database on the first Run call.
func New(cfg *config.Config, gdb *gorm.DB) (*MultiIndexer, error) {
	contractABI, err := abi.JSON(strings.NewReader(string(abiJSON)))
	if err != nil {
		return nil, fmt.Errorf("parse ABI: %w", err)
	}
	return &MultiIndexer{
		cfg:         cfg,
		gdb:         gdb,
		contractABI: contractABI,
		active:      make(map[int64]*runningChain),
	}, nil
}

// Run performs an initial reconcile then re-reconciles on every ChainReloadIntervalMs.
// Blocks until ctx is cancelled; cleans up all goroutines before returning.
func (m *MultiIndexer) Run(ctx context.Context) error {
	m.reconcile(ctx)

	ticker := time.NewTicker(time.Duration(m.cfg.ChainReloadIntervalMs) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			m.stopAll()
			return ctx.Err()
		case <-ticker.C:
			m.reconcile(ctx)
		}
	}
}

// reconcile loads enabled chains from DB and starts/stops indexers to match.
func (m *MultiIndexer) reconcile(ctx context.Context) {
	chains, err := db.LoadEnabledChains(ctx, m.gdb)
	if err != nil {
		slog.Error("reconcile: failed to load chains", "error", err)
		return
	}

	// Build validated enabled set.
	enabled := make(map[int64]db.ChainConfig, len(chains))
	for _, c := range chains {
		if err := config.ValidateChain(c); err != nil {
			slog.Warn("reconcile: skipping invalid chain config", "chain_id", c.ChainID, "error", err)
			continue
		}
		enabled[c.ChainID] = c
	}

	if len(enabled) == 0 {
		slog.Warn("reconcile: no enabled chains in DB")
	}

	var toCancel []context.CancelFunc
	var toWait  []<-chan struct{}
	var toStart []db.ChainConfig

	m.mu.Lock()

	// Stop chains that were disabled or removed.
	for id, rc := range m.active {
		if _, ok := enabled[id]; !ok {
			slog.Info("reconcile: stopping chain indexer", "chain_id", id)
			toCancel = append(toCancel, rc.cancel)
			toWait = append(toWait, rc.done)
			// Pre-remove so the goroutine's deferred cleanup is a no-op.
			delete(m.active, id)
		}
	}

	// Start new chains; restart chains whose config changed.
	for id, chain := range enabled {
		existing, running := m.active[id]
		if running {
			if existing.cfg.RPCURL == chain.RPCURL &&
				existing.cfg.ContractAddress == chain.ContractAddress &&
				existing.cfg.StartBlock == chain.StartBlock {
				continue // nothing changed
			}
			slog.Info("reconcile: chain config changed, restarting", "chain_id", id)
			toCancel = append(toCancel, existing.cancel)
			toWait = append(toWait, existing.done)
			delete(m.active, id)
		}
		toStart = append(toStart, chain)
	}

	m.mu.Unlock()

	// Cancel and wait outside the lock to avoid deadlock with goroutine cleanups.
	for _, cancel := range toCancel {
		cancel()
	}
	for _, done := range toWait {
		<-done
	}

	if len(toStart) > 0 {
		m.mu.Lock()
		for _, chain := range toStart {
			m.startChainLocked(ctx, chain)
		}
		m.mu.Unlock()
	}
}

// startChainLocked starts a ChainIndexer goroutine. Caller must hold m.mu.
func (m *MultiIndexer) startChainLocked(ctx context.Context, chain db.ChainConfig) {
	ciCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})

	ci := &ChainIndexer{
		chain: chain,
		gdb:   m.gdb,
		handler: &eventHandler{
			contractABI:  m.contractABI,
			contractAddr: common.HexToAddress(chain.ContractAddress),
			gdb:          m.gdb,
		},
		logger: slog.With("chain_id", chain.ChainID, "contract", chain.ContractAddress),
		pollMs: m.cfg.PollIntervalMs,
	}

	m.active[chain.ChainID] = &runningChain{cfg: chain, cancel: cancel, done: done}

	go func() {
		defer close(done)
		defer func() {
			// Remove from active only if this is still our entry (not already replaced).
			m.mu.Lock()
			if entry, ok := m.active[chain.ChainID]; ok && entry.done == done {
				delete(m.active, chain.ChainID)
			}
			m.mu.Unlock()
		}()

		ci.logger.Info("chain indexer starting", "rpc", chain.RPCURL, "start_block", chain.StartBlock)
		if err := ci.run(ciCtx); err != nil && err != context.Canceled {
			ci.logger.Error("chain indexer exited with error", "error", err)
		} else {
			ci.logger.Info("chain indexer stopped")
		}
	}()
}

// stopAll cancels all running indexers and waits for them to finish.
func (m *MultiIndexer) stopAll() {
	m.mu.Lock()
	var toWait []<-chan struct{}
	for _, rc := range m.active {
		rc.cancel()
		toWait = append(toWait, rc.done)
	}
	m.mu.Unlock()
	for _, done := range toWait {
		<-done
	}
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

	// TaskCreated must commit before any handler that mutates an existing task
	// row. If a task is created and assigned (or its status changes) within the
	// same block batch, running them in parallel can let the update fire before
	// the row exists, dropping it as ErrNotFound. Process creations first, then
	// fan out the remaining handlers in parallel.
	created, err := ci.handler.handleTaskCreated(ctx, client, ci.chain.ChainID, fromBlock, toBlock)
	if err != nil {
		return fmt.Errorf("event processing [%d-%d]: %w", fromBlock, toBlock, err)
	}
	res.eventsCreated = created

	// updated is written from multiple goroutines, so it must be atomic.
	var updated atomic.Int64
	g, gCtx := errgroup.WithContext(ctx)

	runUpdate := func(fn func(context.Context, int64, uint64, uint64) (int, error)) {
		g.Go(func() error {
			n, err := fn(gCtx, ci.chain.ChainID, fromBlock, toBlock)
			updated.Add(int64(n))
			return err
		})
	}

	runUpdate(func(c context.Context, id int64, f, t uint64) (int, error) {
		return ci.handler.handleTaskAssigned(c, client, id, f, t)
	})
	runUpdate(func(c context.Context, id int64, f, t uint64) (int, error) {
		return ci.handler.handleTaskStatusChanged(c, client, id, f, t)
	})
	runUpdate(func(c context.Context, id int64, f, t uint64) (int, error) {
		return ci.handler.handleCompletionConfirmed(c, client, id, f, t)
	})
	runUpdate(func(c context.Context, id int64, f, t uint64) (int, error) {
		return ci.handler.handleTaskCompleted(c, client, id, f, t)
	})
	runUpdate(func(c context.Context, id int64, f, t uint64) (int, error) {
		return ci.handler.handleTaskDisputed(c, client, id, f, t)
	})
	runUpdate(func(c context.Context, id int64, f, t uint64) (int, error) {
		return ci.handler.handleDisputeResolved(c, client, id, f, t)
	})
	runUpdate(func(c context.Context, id int64, f, t uint64) (int, error) {
		return ci.handler.handleWithdrawn(c, client, id, f, t)
	})
	runUpdate(func(c context.Context, id int64, f, t uint64) (int, error) {
		return ci.handler.handleDeadlineExtended(c, client, id, f, t)
	})

	// Fee events touch no task row; their counts are not tracked.
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
	res.eventsUpdated = int(updated.Load())

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
