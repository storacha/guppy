# retrieve

Retrieve a file or directory by its CID from the Storacha network.

Downloads content from a space and writes it to the specified output path.

**Aliases:** `retrieve`, `get`

## Usage

```
guppy retrieve <space> <content-path> <output-path>
```

## Arguments

| Argument | Description |
|----------|-------------|
| `space` | Space DID or name |
| `content-path` | CID or content path to retrieve |
| `output-path` | Local path to write the retrieved content |

The `content-path` supports several formats:

- `/ipfs/<cid>[/<subpath>]`
- `ipfs://<cid>[/<subpath>]`
- `<cid>[/<subpath>]`

Using a subpath retrieves a specific file or directory beneath the root CID.

## Example

Retrieve a root CID:

```bash
guppy retrieve myspace bafybeibhybbpoqakv7pfj5nlrpmldkgiuksmbi3t2cnhxqxnqvbzkhyzjy ./output
```

Retrieve a file at a subpath:

```bash
guppy retrieve myspace bafybeibhybbpoqakv7pfj5nlrpmldkgiuksmbi3t2cnhxqxnqvbzkhyzjy/photos/cat.jpg ./cat.jpg
```
