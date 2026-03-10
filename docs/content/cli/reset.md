# reset

Reset agent proofs.

Removes all proofs and delegations from the local store, effectively logging out from all accounts. Your agent DID is preserved — only the authorization proofs are removed.

After resetting, you'll need to run [`guppy login`](login.md) again to re-authorize.

## Usage

```
guppy reset
```
