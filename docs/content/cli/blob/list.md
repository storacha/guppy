# blob list

List blobs stored in a space.

**Aliases:** `list`, `ls`

## Usage

```
guppy blob list <space> [flags]
```

## Arguments

| Argument | Description |
|----------|-------------|
| `space` | Space DID or name |

## Flags

| Flag | Description |
|------|-------------|
| `--proof <path>` | Path to a CAR archive containing UCAN proofs |
| `--long, -l` | Display detailed blob information |
| `--human, -H` | Use human-readable size format (with `--long`) |
| `--json` | Output as newline-delimited JSON |

## Examples

List blobs:

```bash
guppy blob list my-data
```

List with sizes in human-readable format:

```bash
guppy blob list my-data --long --human
```

Output as JSON:

```bash
guppy blob list my-data --json
```
