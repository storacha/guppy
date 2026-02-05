# Guppy

Go client library and CLI for Storacha network - enterprise-scale, resumable uploads with parallel processing.

## Quick Reference

```bash
make build          # Build ./guppy binary
make test           # Run tests
make migration NAME=x  # Create migration
```

## Structure

- `main.go` - Entry point (OTel, signals, pprof)
- `cmd/` - Cobra CLI commands (account, delegation, gateway, proof, space, upload, etc.)
- `pkg/client/` - Storacha network client, UCAN proofs, delegations
- `pkg/preparation/` - Core upload logic: scan → DAG → shards → upload
  - `sqlrepo/` - SQLite persistence (WAL mode, Goose migrations)
  - `spaces/`, `uploads/`, `sources/`, `scans/`, `dags/`, `blobs/`
- `pkg/agentstore/` - Identity/delegation persistence (MemStore, FsStore)
- `pkg/verification/` - Content verification with indexer queries
- `pkg/config/` - Viper config (TOML, env vars with `GUPPY_` prefix)
- `pkg/bus/` - Event bus for component communication

## Key Patterns

**Functional Options**: `client.NewClient(client.WithStore(...), client.WithPrincipal(...))`

**Repository Pattern**: Each pkg defines `Repo` interface, `sqlrepo.Repo` implements all

**API/Repo Separation**: Business logic in `API` structs, data access via `Repo` interfaces

**UCAN Capabilities**: `go-ucanto` for capability-based auth, delegations stored in agent store

## Key Dependencies

- `github.com/storacha/go-ucanto` - UCAN implementation
- `github.com/storacha/go-libstoracha` - Storacha capabilities
- `github.com/ipfs/boxo`, `go-cid`, `go-ipld-*` - IPFS/IPLD
- `github.com/ipld/go-car` - CAR format
- `modernc.org/sqlite` - Pure Go SQLite
- `github.com/spf13/cobra` + `viper` - CLI/config
- `github.com/charmbracelet/bubbletea` - TUI

## Config

Data dir: `~/.storacha/guppy/` | Config: `~/.config/guppy/config.toml`

Networks: `STORACHA_NETWORK` (forge, hot, warm-staging)

## Conventions

- Logging: `var log = logging.Logger("pkg/name")` with `log.Infow()`, `log.Warnw()`
- Tracing: `var tracer = otel.Tracer("pkg/name")`
- Errors: Context-wrapped with `%w`, `HandledCliError` for CLI
- Tests: Interface mocks, `testutil` packages, `t.TempDir()` for fs ops
