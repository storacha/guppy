# verify

Verify the integrity and correctness of a DAG (Directed Acyclic Graph).

Launches a TUI that shows verification progress including block counts, shards verified, and origin nodes checked. Use this to confirm that uploaded data is complete and correctly stored on the network.

This is a network operation. Your agent must be authorized to `space/content/retrieve` from a space containing the data before running this command. This typically delegated to any account granted access to a space.

Note that this command retrieves data to verify it, and will incur egress charges.

## Usage

```
guppy verify <root-cid>
```

## Arguments

| Argument | Description |
|----------|-------------|
| `root-cid` | The root CID of the DAG to verify |

## Example

```bash
guppy verify bafybeibhybbpoqakv7pfj5nlrpmldkgiuksmbi3t2cnhxqxnqvbzkhyzjy
```
