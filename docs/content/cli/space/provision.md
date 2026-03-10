# space provision

Provision a space with a customer account for billing purposes.

Associates a space with a customer account so that storage usage is billed to that account. This is typically handled automatically by [`guppy space generate`](generate.md), but can be done separately if needed.

## Usage

```
guppy space provision <space> <email-address>
```

## Arguments

| Argument | Description |
|----------|-------------|
| `space` | Space DID or name |
| `email-address` | Customer account email address |

## Example

```bash
guppy space provision my-data billing@example.com
```
