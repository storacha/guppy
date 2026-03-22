# list

List uploads in a space.

Displays all uploads as CIDs (Content Identifiers). Optionally shows the shard CIDs under each upload root.

This is a network operation. Your agent must be authorized to `upload/list` before running this command. This typically delegated to any account granted access to a space.

**Aliases:** `ls`, `list`

## Usage

```
guppy ls <space> [flags]
```

## Arguments

| Argument | Description |
|----------|-------------|
| `space` | Space DID or name |

## Flags

| Flag | Description |
|------|-------------|
| `--proof <path>` | Path to a CAR archive containing additional UCAN proofs |
| `--shards` | Display shard CIDs under each upload root |

## Examples

List uploads by space name:

```bash
guppy ls my-space
```

List uploads by space DID:

```bash
guppy ls did:key:z6MksCX5PdUgHv83cmDE2DfCrR1WHG9MmZPRKSvTi8Ca297V
```

List uploads with shard details:

```bash
guppy ls my-space --shards
```
