# Troubleshooting

## Retriable upload issues

Sometimes `guppy` will notice something inconsistent during the upload, like data that changed during the upload process. When this happens, it will print an error and explain that you can simply resume the upload to resolve the problem. To automatically retry in those cases, pass `--retry` to [`guppy upload`](./cli/upload/index.md).

### Bad database states

If the database does get into an incorrect state for some reason, or if you're ever concerned it has, you can check the local upload records to make sure they're consistent with [`guppy upload check`](./cli/upload/check.md). This will run through several integrity checks to ensure the database is in a good state and the upload can continue. Pass `--repair` to solve any found problems, if possible.

## DAG issues in the uploaded data

Once uploaded, your root CID should be the root of a UnixFS DAG representing the source data, and all of the nodes of that DAG should be available in your space. You can verify your data is complete and correctly stored using [`guppy verify`](./cli/verify.md):

```bash
guppy verify <root-cid>
```

Note that this requires reading the data from your space, which will incur egress charges. `<root-cid>` is the CID of the root of the DAG to verify, which can be the root of the entire upload or of some file or directory within the upload, which will verify the contents of that file or directory.

You can list your space's uploads to see their root CIDs:

```bash
guppy ls my-data
```