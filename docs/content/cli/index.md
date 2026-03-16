# CLI Reference

Guppy provides a command-line interface for uploading, retrieving, and managing data on the Storacha network.

## Usage

```
guppy [command] [flags]
```

## Global Flags

| Flag | Description | Default |
|------|-------------|---------|
| `--data-dir <path>` | Directory for config and data store | `~/.storacha/guppy` |
| `--database-url <url>` | PostgreSQL connection URL (uses SQLite if not set) | |
| `--config <path>` | Path to config file | |
| `--network, -n <name>` | Network preset name (`forge`, `hot`, `warm-staging`) | |
| `--ui` | Use the Guppy TUI | |
| `--upload-service-did <did>` | Override network upload service DID | |
| `--upload-service-url <url>` | Override network upload service URL | |
| `--receipts-url <url>` | Override receipts service URL | |
| `--indexer-did <did>` | Override indexing service DID | |
| `--indexer-url <url>` | Override indexing service URL | |

## Commands

### [login](login.md)

Authenticate with a Storacha account.

### [whoami](whoami.md)

Print information about the local agent.

### [version](version.md)

Print the version of Guppy.

### [reset](reset.md)

Reset agent proofs (log out from all accounts).

### [list](list.md)

List uploads in a space.

### [retrieve](retrieve.md)

Retrieve a file or directory by its CID.

### [verify](verify.md)

Verify a DAG's integrity and correctness.

### [account](account/index.md)

Manage logged-in accounts.

### [space](space/index.md)

Create and manage spaces.

### [upload](upload/index.md)

Upload data to the Storacha network.

### [delegation](delegation/index.md)

Delegate capabilities to other agents.

### [proof](proof/index.md)

Manage proofs delegated to this agent.

### [gateway](gateway/index.md)

Run an IPFS Gateway on the Storacha network.

### [blob](blob/index.md)

List blobs in a space.

### [unixfs](unixfs/index.md)

Inspect UnixFS content in a space.
