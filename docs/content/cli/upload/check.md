# upload check

Check upload integrity and completeness.

Runs a comprehensive validation of your uploads with six check types:

1. **Upload Scanned** - Verifies filesystem and DAG scans completed
2. **File System Integrity** - Validates filesystem structure and DAG integrity
3. **Node Integrity** - Ensures all nodes have upload records
4. **Node Completeness** - Verifies all nodes are packed into shards
5. **Shard Completeness** - Verifies all shards are uploaded and indexed
6. **Index Completeness** - Verifies all indexes are uploaded

By default, runs in dry-run mode and only reports issues. Use `--repair` to automatically fix problems.

## Usage

```
guppy upload check [space] [source-path-or-name] [flags]
```

## Arguments

All arguments are optional. Omit both to check all uploads.

| Argument | Description |
|----------|-------------|
| `space` | Space DID or name (check all spaces if omitted) |
| `source-path-or-name` | Specific source to check (check all sources in space if omitted) |

## Flags

| Flag | Description |
|------|-------------|
| `--repair` | Automatically fix issues found (default is dry-run) |

## Examples

Check all uploads:

```bash
guppy upload check
```

Check a specific space:

```bash
guppy upload check "my data"
```

Check and repair:

```bash
guppy upload check "my data" --repair
```
