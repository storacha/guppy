# space info

Get information about a space, including its associated providers and provisioning status.

This is a network operation. Your agent must be authorized to `space/info` before running this command. This typically delegated to any account granted access to a space.

## Usage

```
guppy space info <space> [flags]
```

## Arguments

| Argument | Description |
|----------|-------------|
| `space` | Space DID or name |

## Flags

| Flag | Description |
|------|-------------|
| `--json` | Output in JSON format |

## Example

```bash
guppy space info my-data
```
