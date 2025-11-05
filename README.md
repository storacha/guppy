# guppy

A Storacha client in golang. ⚠️ Heavily WIP.

## Install

```sh
go get github.com/storacha/guppy
```

## Usage

Guppy can be used as a Go library or as a CLI command.

### Client library

There are two ways to use the client library: you can get a user to interactively log in, or bring a prepared, authorized identity.

To have the user log in, use `(*client.Client) RequestAccess()` to have the service ask the user to authenticate, and `(*client.Client) PollClaim()` to notice when they do. ([Example](examples/loginflow/loginflow.go))

To bring your own pre-authorized identity, instantiate the client with the option `client.WithPrincipal(signer)`. ([Example](examples/byoidentity/byoidentity.go)) You'll first need to [generate a DID](#generate-a-did) and then [delegate capabilities](#obtain-proofs) to that identity.

### CLI

The CLI will automatically generate an identity for you and store it in `~/.storacha/store.json`, or in another directory given with `--storachaDir`. Like the library, there are two ways to authenticate the CLI client: interactively, or by authorizing in advance.

To authorize interactively, use `go run ./cmd login` and follow the prompts.

To authorize in advance, use `go run ./cmd whoami` to see the client's DID and then [delegate capabilities](#obtain-proofs) to that identity. Then, pass the proofs you create on the command line whenever you use the CLI.


## Uploads in Guppy vs the Storacha JS CLI

While both Guppy and the [Storacha JS CLI](https://github.com/storacha/upload-service/tree/main/packages/cli) provide access to the Storacha Network, they serve different use cases and are optimized for different scenarios:

### Design Goals

**Storacha JS CLI**: Designed for easy integration in web applications and straightforward file uploads. Perfect for developers who need simple, quick uploads and standard web-based workflows.

**Guppy**: Built specifically for enterprise-scale, large data uploads with a focus on efficiency, reliability, and incremental updates. Ideal for organizations managing substantial datasets that change over time.

### Key Differences

Guppy provides several enterprise-focused features that set it apart:

**Resumable Uploads**: Guppy can resume interrupted uploads, ensuring that large datasets don't need to be re-uploaded from scratch if something goes wrong.

<!-- [Not yet implemented:] **Multiple Data Source Support**: Pull data from various sources including:
- Local filesystems
- Remote data sources
- Cloud storage providers
- Other network locations -->

<!-- [Not yet implemented:] **Incremental & Updatable Uploads**: Rather than treating each upload as independent, Guppy supports incremental uploads that only transfer what has changed since the last upload, dramatically reducing bandwidth and time for large, evolving datasets. -->

**Parallel Processing**: Built-in parallel processing and concurrent data pulling capabilities to maximize throughput for large uploads.

### Space Concepts

In Guppy, a Storacha space is associated with settings and one or more data sources. First, `guppy upload sources add <space-did> <data-path>`, then `guppy upload <space-did>`. The configuration for a space, as well as all intermediate progress, is stored in a database (`~/.storacha/preparation.db`, configurable with the `--db` option).

<!-- [Not yet implemented:] In Guppy, a "space" represents more than just a collection of uploads. Each space is configured with a set of data sources, and Guppy treats uploads intelligently:

- **Initial Upload**: The first upload from a data source captures the complete dataset
- **Subsequent Uploads**: All following uploads are treated as "updates" that only include changes (additions, modifications, deletions) since the last upload
- **Change Detection**: Guppy automatically detects what has changed and only processes the delta, making updates extremely efficient even for massive datasets

This approach makes Guppy particularly well-suited for scenarios like:
- Regular backups of large databases
- Synchronizing evolving datasets
- Enterprise content management systems
- Any situation where large amounts of data change incrementally over time -->

## How to

### Generate a DID

You can use `ucan-key` to generate a private key and DID for use with the library. Install Node.js and then use the `ucan-key` module:

```sh
npx ucan-key ed
```

Output should look something like:

```sh
# did:key:z6Mkh9TtUbFJcUHhMmS9dEbqpBbHPbL9oxg1zziWn1CYCNZ2
MgCb+bRGl02JqlWMPUxCyntxlYj0T/zLtR2tn8LFvw6+Yke0BKAP/OUu2tXpd+tniEoOzB3pxqxHZpRhrZl1UYUeraT0=
```

You can use the private key (the line starting `Mg...`) in the CLI by setting the environment variable `GUPPY_PRIVATE_KEY`. Alternatively you can use it programmatically after parsing it:

```go
package main

import "github.com/web3-storage/go-ucanto/principal/ed25519/signer"

signer, _ := signer.Parse("MgCb+bRGl02JqlWMPUxCyntxlYj0T/zLtR2tn8LFvw6+Yke0BKAP/OUu2tXpd+tniEoOzB3pxqxHZpRhrZl1UYUeraT0=")
```

### Obtain proofs

Proofs are delegations to your DID enabling it to perform tasks. Currently the best way to obtain proofs that will allow you to interact with the Storacha Network is to use the Storacha JS CLI:

1. [Generate a DID](#generate-a-did) and make a note of it (the string starting with `did:key:...`)
2. Install w3 CLI:
    ```sh
    npm install -g @storacha/cli
    ```
3. Create a space:
    ```sh
    storacha space create <NAME>
    ```
4. Delegate capabilities to your DID:
    ```sh
    storacha delegation create -c 'store/*' -c 'upload/*' <DID>`
    ```

## API

[pkg.go.dev Reference](https://pkg.go.dev/github.com/storacha/guppy)

## Contributing

Feel free to join in. All welcome. Please [open an issue](https://github.com/storacha/guppy/issues)!

## License

Dual-licensed under [MIT + Apache 2.0](LICENSE.md)
