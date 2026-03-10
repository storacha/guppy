# upload

Upload data to a Storacha space.

**Aliases:** `upload`, `up`

Guppy processes the upload in stages:

1. **Scan** — Walks the source paths to discover files
2. **DAG** — Breaks files into a DAG of [UnixFS nodes](https://docs.ipfs.tech/concepts/file-systems/#unix-file-system-unixfs)
3. **Shard & Index** — Packs nodes into shards for storage, and creates indexes into the shards
4. **Upload** — Uploads shards and indexes to the Storacha network

These stages form a pipeline, so as soon as a file is discovered, it will be broken into UnixFS nodes, and then those nodes will be packed into shards. As shards are filled, they will be indexed and then uploaded. Shard uploads happen in parallel.

All progress is tracked in a local SQLite database. If the process is interrupted at any stage, just run the `upload` command on the space again to resume.

### Catching changed sources

By default, when resuming, `upload` will scan the entire source again, in case any files have changed. The result will be a complete, consistent snapshot of the source. Any work that was in progress in later stages will continue, but the upload will not complete until the re-scan has confirmed that nothing has changed.

If you know the source data hasn't changed, you can use `--assume-unchanged-sources`. This will skip scanning files and directories which have been scanned before, so interrupted filesystem scans will resume rather than restart, and completed filesystem scans will not be repeated.

```bash
guppy upload my-data --assume-unchanged-sources
```

Guppy will also verify during upload that the data it's sending is what was scanned and hashed originally. If something's become inconsistent, those files will be marked as un-scanned, and resuming the upload will perform a new scan on them. You can also use `--retry` to automatically retry the upload after hitting such an issue.

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
guppy upload my-data
```

Upload with higher parallelism:

```bash
guppy upload my-data --parallelism 12
```

Resume an interrupted upload without re-scanning:

```bash
guppy upload my-data --assume-unchanged-sources
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
