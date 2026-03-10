# upload source

Manage upload sources for a space.

A **source** is a local filesystem path associated with a space. Guppy scans sources to discover files to upload.

You can add multiple sources to a space. Each one will become a separate upload in the space, each with its own root CID.

The association of sources to spaces is purely local. Adding a source to a space doesn't affect the space itself on the network, and doesn't affect other instances of `guppy` on other machines pointing to the same space.

## Usage

```
guppy upload source [command]
```

## Subcommands

### [add](add.md)

Add a source to a space.

### [list](list.md)

List sources added to a space.
