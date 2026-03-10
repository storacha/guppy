# upload source add

Add a local filesystem path as an upload source for a space.

Associates a path with a space so that `guppy upload` will include it. You can optionally set a custom name and shard size.

## Usage

```
guppy upload source add <space> <path> [flags]
```

## Arguments

| Argument | Description |
|----------|-------------|
| `space` | Space DID or name |
| `path` | Local filesystem path to the data |

## Flags

| Flag | Description |
|------|-------------|
| `--name <name>` | Alias name for the source |
| `--shard-size <size>` | Shard size (e.g., `1024`, `512B`, `100K`, `50M`, `2G`) |

## Example

Add a directory as a source:

```bash
guppy upload source add my-data /path/to/dataset
```

Add with a custom name and shard size:

```bash
guppy upload source add my-data /path/to/dataset --name "photos" --shard-size 50M
```

## What's Next

Start the upload:

```bash
guppy upload my-data
```

See [`guppy upload`](../index.md) for details.
