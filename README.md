# guppy

> *Storacha's go uploader—and more!* 🐔❤️🐟

A Storacha client in golang. Guppy includes both a CLI command and an underlying Go library which can be used directly.

**New Contributor?** Check out [CONTRIBUTING.md](./CONTRIBUTING.md) for detailed instructions and troubleshooting to get up and running.

## Install

```sh
go install github.com/storacha/guppy
```

### Differences from [@storacha/cli](https://www.npmjs.com/package/@storacha/cli)

There's [another CLI for Storacha written in JS](https://www.npmjs.com/package/@storacha/cli). While both provide access to the Storacha Network, they serve different use cases and are optimized for different scenarios.

* **Storacha JS CLI**: Designed for easy integration in web applications and straightforward file uploads. Perfect for developers who need simple, quick uploads and standard web-based workflows.

* **Guppy**: Built specifically for enterprise-scale, large data uploads with a focus on efficiency, reliability, and incremental updates. Ideal for organizations managing substantial datasets that change over time.

Guppy provides several enterprise-focused features that set it apart:

* **Resumable Uploads**: Guppy can resume interrupted uploads, ensuring that large datasets don't need to be re-uploaded from scratch if something goes wrong.

<!-- [Not yet implemented:] **Multiple Data Source Support**: Pull data from various sources including:
- Local filesystems
- Remote data sources
- Cloud storage providers
- Other network locations -->

<!-- [Not yet implemented:] **Incremental & Updatable Uploads**: Rather than treating each upload as independent, Guppy supports incremental uploads that only transfer what has changed since the last upload, dramatically reducing bandwidth and time for large, evolving datasets. -->

* **Parallel Processing**: Built-in parallel processing and concurrent data pulling capabilities to maximize throughput for large uploads.


### Usage

#### Login

Guppy will automatically generate an identity for you. First, you'll need to authorize that identity to act on behalf of your account:

```sh
$ guppy login <account-email-address>
```

This will ask Storacha to send you an email with a link to click. Clicking that link will confirm to the network that Guppy is authorized to act as you. You can log your identity into multiple accounts at once, if necessary.

Your identity, authorizing proofs, and other state is kept in `~/.storacha/guppy` by default. You can set a different directory with `--guppy-dir`.

#### Spaces

Before you can upload data, you'll need a Storacha space to put it in. To create one:

```sh
$ guppy space generate
```

A space is identified by a `did:key:` DID. Your account will provision the space for billing purposes, and will be granted full authorization to use the space.

You can get a list of all spaces you are known to have access to with:

```sh
$ guppy space list
```

#### Uploading

Guppy's uploader is where most of the work happens. To upload data, you first associate one or more **sources** with a **space**, and then run the uploader for that space.

```sh
$ guppy upload source add <space> <path>
$ guppy upload <space>
```

> [!WARNING]  
> Multiple sources for the same space are not well supported yet, mainly by the UI. This will improve shortly.

The uploader will scan the data source, break up its data into UnixFS nodes, pack those nodes into shards, and store those shards on the Storacha network within the space. Along the way, everything is tracked in a database file in the `--guppy-dir`. If at any time the process is interrupted, it can be restarted by simply running the same command again:

```sh
$ guppy upload <space>
```

The uploader will pick up where it left off, quickly scanning to make sure it's aware of any changes to the underlying source data. This ensures that the final root CID returned will point to a complete, consistent view of the data source.

#### Retrieving

Guppy can then be used to retrieve content from the network. 

```sh
$ retrieve <space> <content-path> <output-path>
```

The `<content-path>` can take any of these forms:

* `/ipfs/<cid>[/<subpath>]`
* `ipfs://<cid>[/<subpath>]`
* `<cid>[/<subpath>]`

`bafybeibhybbpoqakv7pfj5nlrpmldkgiuksmbi3t2cnhxqxnqvbzkhyzjy` will retrieve the file or directory named by that CID, while `bafybeibhybbpoqakv7pfj5nlrpmldkgiuksmbi3t2cnhxqxnqvbzkhyzjy/a/subpath` will retrieve the file or directory at `a/subpath` beneath that CID.

The named content will be written to `<output-path>`.


## Client library

There are two ways to use the client library: you can get a user to interactively log in, or bring a prepared, authorized identity.

To have the user log in, use `(*client.Client) RequestAccess()` to have the service ask the user to authenticate, and `(*client.Client) PollClaim()` to notice when they do. ([Example](examples/loginflow/loginflow.go))

To bring your own pre-authorized identity, instantiate the client with the option `client.WithPrincipal(signer)`. ([Example](examples/byoidentity/byoidentity.go)) You'll first need to [generate a DID](#generate-a-did) and then [delegate capabilities](#obtain-proofs) to that identity.

### API

[pkg.go.dev Reference](https://pkg.go.dev/github.com/storacha/guppy)

## Contributing

Feel free to join in. All welcome. Please [open an issue](https://github.com/storacha/guppy/issues)! PRs are also welcome, but please confirm with the maintainers before doing significant work; we'd hate your efforts to go to waste on work that's already being done, or that's going to run into an issue we're already thinking about.

See [CONTRIBUTING.md](./CONTRIBUTING.md) for technical and logistical details about how to contribute.

## License

Dual-licensed under [MIT + Apache 2.0](LICENSE.md)
