# space list

List all Storacha spaces the local agent is aware of, which is all of the spaces it has some kind of access to. Typically, this consists of the spaces which have granted access to an account which the local agent is logged in as.

If your account has been granted access to a space since the local agent logged in, that space won't appear yet. Use [`guppy login`](../login.md) again to fetch the latest spaces you have access to.

**Aliases:** `list`, `ls`

## Usage

```
guppy space list [flags]
```

## Flags

| Flag | Description |
|------|-------------|
| `--json` | Output in JSON format |

## Example

```bash
guppy space list
```
