# upload

Upload data to a Storacha space.

**Aliases:** `upload`, `up`

The upload process works in stages:

1. **Scan** - Guppy scans the source paths to discover files
2. **DAG** - Files are broken into UnixFS nodes and organized into a DAG (Directed Acyclic Graph)
3. **Shard** - Nodes are packed into shards for efficient transport
4. **Upload** - Shards are uploaded to the Storacha network in parallel

All progress is tracked in a local database. If the process is interrupted at any stage, re-running the same command resumes from where it left off.

## Usage

```
guppy upload <space> [source-path-or-name...] [flags]
```

## Arguments

| Argument | Description |
|----------|-------------|
| `space` | Space DID or name |
| `source-path-or-name` | Optional specific sources to upload (uploads all sources if omitted) |

## Flags

| Flag | Description | Default |
|------|-------------|---------|
| `--all` | Upload all sources even if specific ones are provided as arguments | |
| `--retry` | Automatically retry failed uploads | |
| `--parallelism <n>` | Number of parallel shard uploads | `6` |
| `--assume-unchanged-sources` | Skip filesystem rescan if a completed scan exists (faster resume) | |

## Example

Upload all sources for a space:

```bash
guppy upload "my data"
```

Upload with higher parallelism:

```bash
guppy upload "my data" --parallelism 12
```

Resume an interrupted upload without re-scanning:

```bash
guppy upload "my data" --assume-unchanged-sources
```

## Subcommands

### [source](source/index.md)

Manage upload sources (local paths associated with a space).

### [check](check.md)

Check upload integrity and completeness.

## What's Next

After uploading, verify your data:

```bash
guppy verify <root-cid>
```

Or list your uploads:

```bash
guppy ls <space>
```
