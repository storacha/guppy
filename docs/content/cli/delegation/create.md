# delegation create

Delegate capabilities for a space to another agent.

Outputs a CAR-encoded UCAN delegation that grants the specified capabilities to the audience DID. The delegation can be written to a file or piped to another process.

## Usage

```
guppy delegation create <space> <audience-did> [flags]
```

## Arguments

| Argument | Description |
|----------|-------------|
| `space` | Space DID or name |
| `audience-did` | DID of the agent to delegate capabilities to |

## Flags

| Flag | Description |
|------|-------------|
| `--can, -c <ability>` | Ability to delegate (required, can be specified multiple times) |
| `--expiration, -e <timestamp>` | Unix timestamp for delegation expiration (0 = no expiration) |
| `--output, -o <path>` | Path to write the delegation CAR file (stdout if not set) |

## Example

Delegate upload capability to another agent:

```bash
guppy delegation create my-data did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK \
  --can space/blob/add \
  --can space/index/add \
  --can filecoin/offer \
  --can upload/add \
  -o delegation.car
```

The recipient can then add this delegation as a proof:

```bash
guppy proof add delegation.car
```

See [`guppy proof add`](../proof/index.md) for details.
