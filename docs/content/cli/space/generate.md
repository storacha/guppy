# space generate

Generate a new Storacha space.

Creates a new space, provisions it to your logged-in account (for billing), and grants your account access to the space. The space is stored locally.

If you are logged into multiple accounts, use `--grant-to` and `--provision-to` to specify which account to use.

## Prerequisites

You must be logged in with [`guppy login`](../login.md) before generating a space.

## Usage

```
guppy space generate [flags]
```

## Flags

| Flag | Description |
|------|-------------|
| `--name <name>` | Human-readable name for the space |
| `--grant-to <did>` | Account DID to grant access to (optional if only one account logged in) |
| `--provision-to <did>` | Account DID to provision the space to (optional if only one account logged in) |
| `--output-key, -k` | Output the space's private key (sensitive!) |

## Example

```bash
guppy space generate --name "my data"
```

## What's Next

Add a source and start uploading:

```bash
guppy upload source add <space> /path/to/data
guppy upload <space>
```

See [`guppy upload`](../upload/index.md) for details.
