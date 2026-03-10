# unixfs ls

List the contents of a UnixFS directory stored on the Storacha network.

## Usage

```
guppy unixfs ls <space-did> <cid-path> [flags]
```

## Arguments

| Argument | Description |
|----------|-------------|
| `space-did` | Space DID |
| `cid-path` | CID path in the format `<cid>[/<subpath>]` |

## Flags

| Flag | Description |
|------|-------------|
| `--long, -l` | Use long listing format with mode, size, date, and name |

## Examples

List a directory:

```bash
guppy unixfs ls did:key:z6Mkk89bC3JrVqKie71YEcc5M1SMVxuCgNx6zLZ8SYJsxALi bafybeibhybbpoqakv7pfj5nlrpmldkgiuksmbi3t2cnhxqxnqvbzkhyzjy
```

List with details:

```bash
guppy unixfs ls did:key:z6Mkk89bC3JrVqKie71YEcc5M1SMVxuCgNx6zLZ8SYJsxALi bafybeibhybbpoqakv7pfj5nlrpmldkgiuksmbi3t2cnhxqxnqvbzkhyzjy --long
```

List a subdirectory:

```bash
guppy unixfs ls did:key:z6Mkk89bC3JrVqKie71YEcc5M1SMVxuCgNx6zLZ8SYJsxALi bafybeibhybbpoqakv7pfj5nlrpmldkgiuksmbi3t2cnhxqxnqvbzkhyzjy/photos
```
