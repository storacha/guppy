# login

Authenticate with a Storacha account.

This initiates an authorization email flow. Guppy sends a request to the Storacha network, which emails you a confirmation link. Clicking the link authorizes your local Guppy agent to act on behalf of your account.

You can log into multiple accounts by running this command more than once with different email addresses. Re-running with an existing account will refresh your delegations, which is useful if you've been granted access to new spaces.

## Usage

```
guppy login <email-address>
```

## Arguments

| Argument | Description |
|----------|-------------|
| `email-address` | Your Storacha account email address |

## Example

```bash
guppy login racha@storacha.network
```

## What's Next

After logging in, create a space to store your data:

```bash
guppy space generate --name my-data
```

See [`guppy space generate`](space/generate.md) for details.
