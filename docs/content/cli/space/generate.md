# space generate

Generate a new Storacha space.

Creates a new space, provisions it to your logged-in account (for billing), and grants your account access to the space. The delegations are stored in the network and in your local store.

If you are logged into multiple accounts, use `--grant-to` and `--provision-to` to specify which account to use for each.

Spaces can always be referred to in other commands by their full DID. They can also be referred to by their name, if set with `--name`.

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
guppy space generate --name my-data
```
