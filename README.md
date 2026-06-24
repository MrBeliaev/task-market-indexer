# TaskMarket Indexer

Go service that listens to `TaskMarketplace` contract events on one or more EVM chains and writes the resulting state into PostgreSQL via GORM.

## Architecture

```
cmd/main.go
└── config.Load()          : env vars → Config (DATABASE_URL, POLLING_INTERVAL_MS)
└── db.Connect()           : GORM + postgres
└── config.LoadChains()    : reads enabled rows from the chain_config table
└── indexer.New()          : parses ABI, builds one ChainIndexer per chain
    └── MultiIndexer.Run() : errgroup, one goroutine per chain
        └── ChainIndexer.run()
            └── poll loop (every POLLING_INTERVAL_MS ms)
                └── eventHandler.filterLogs() × 10 events
                    └── db.Upsert* / db.Update* / db.Record*
```

Each chain tracks its progress in the `indexer_state` table. On restart, the last processed block is restored and polling continues from there.

## Events handled

| Event | DB operation |
|---|---|
| `TaskCreated` | `UpsertTaskCreated`: insert task row |
| `TaskAssigned` | `UpdateTaskAssigned`: set executor, status → ASSIGNED |
| `TaskStatusChanged` | `UpdateTaskStatus`: sync status enum |
| `CompletionConfirmed` | `UpdateConfirmations`: set client/executor confirmed flags |
| `TaskCompleted` | `UpdateTaskCompleted`: store payout and fee amounts |
| `TaskDisputed` | `UpdateTaskDisputed`: store disputing address |
| `DisputeResolved` | `UpdateDisputeResolved`: store client refund / executor payout split |
| `Withdrawn` | `RecordWithdrawal`: insert withdrawal row |
| `FeeBpsUpdated` | log only |
| `FeeRecipientUpdated` | log only |

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | n/a | PostgreSQL DSN, e.g. `postgres://user:pass@localhost:5432/taskmarket` (required) |
| `POLLING_INTERVAL_MS` | `5000` | Poll interval in milliseconds |

## Chain configuration

Chains to index are configured in the `chain_config` table (managed via the
backend's Prisma schema, `ChainConfig` model). On startup the indexer loads
all rows where `enabled = true`, ordered by `chain_id`. Each row requires:

| Column | Description |
|---|---|
| `chain_id` | EVM chain ID, must be positive and unique |
| `rpc_url` | JSON-RPC endpoint for this chain |
| `contract_address` | `TaskMarketplace` contract address (valid hex address) |
| `start_block` | Block to start indexing from if no `indexer_state` row exists yet |
| `enabled` | Set to `false` to stop indexing a chain without deleting its config |

At least one enabled chain is required. The indexer exits with an error if
the table is empty or has no enabled rows. Toggling `enabled` or editing a
row takes effect on the next restart (no hot reload).

Example: enable a chain via `psql`:

```sql
INSERT INTO chain_config (chain_id, rpc_url, contract_address, start_block, enabled, updated_at)
VALUES (11155111, 'https://ethereum-sepolia-rpc.publicnode.com', '0x84c68038f4524C84ECF7c0EB3CF0bceD3ADCB152', 11119226, true, NOW())
ON CONFLICT (chain_id) DO UPDATE SET enabled = true;
```

## Running locally

```sh
# Start Postgres (or use docker-compose from the repo root)
docker compose up -d postgres

export DATABASE_URL="postgres://postgres:postgres@localhost:5432/taskmarket?sslmode=disable"

# Make sure chain_config has at least one enabled row (see above)

go run ./cmd/main.go
```

## Docker

```sh
docker build -t taskmarket-indexer .
docker run --env-file .env taskmarket-indexer
```

## Development

```sh
# Tests
go test ./...

# Linter (requires golangci-lint v2)
golangci-lint run ./...
```

Tests use `go-sqlmock`, so no real database is required.
