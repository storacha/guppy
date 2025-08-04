package upload

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/ipfs/go-cid"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"github.com/storacha/go-ucanto/core/car"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/ipld"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/guppy/internal/cmdutil"
	"github.com/storacha/guppy/pkg/car/sharding"
	"github.com/storacha/guppy/pkg/client"
	"github.com/urfave/cli/v2"
)

// Upload handles file and directory uploads to Storacha.
// Note: This doesn't really belong in `internal` in the long run. This was
// pulled out of `main` without much refactoring, as uploading is still
// evolving. It should end up implemented within `pkg` before long.
func Upload(cCtx *cli.Context) error {
	space := cmdutil.MustParseDID(cCtx.String("space"))
	proofs := []delegation.Delegation{}
	if cCtx.String("proof") != "" {
		proof := cmdutil.MustGetProof(cCtx.String("proof"))
		proofs = append(proofs, proof)
	}

	c := cmdutil.MustGetClient(proofs...)

	// Handle options
	isCAR := cCtx.String("car") != ""
	isJSON := cCtx.Bool("json")
	// isVerbose := cCtx.Bool("verbose")
	isWrap := cCtx.Bool("wrap")
	// shardSize := cCtx.Int("shard-size")

	var paths []string
	if isCAR {
		paths = []string{cCtx.String("car")}
	} else {
		paths = cCtx.Args().Slice()
	}

	var root ipld.Link
	if isCAR {
		fmt.Printf("Uploading %s...\n", paths[0])
		var err error
		root, err = uploadCAR(cCtx.Context, paths[0], c, space)
		if err != nil {
			return err
		}
	} else {
		if len(paths) == 1 && !isWrap {
			var err error
			root, err = uploadFile(cCtx.Context, paths[0], c, space)
			if err != nil {
				return err
			}
		} else {
			var err error
			root, err = uploadDirectory(cCtx.Context, paths, c, space)
			if err != nil {
				return err
			}
		}
	}

	if isJSON {
		fmt.Printf("{\"root\":\"%s\"}\n", root)
	} else {
		fmt.Printf("⁂ https://w3s.link/ipfs/%s\n", root)
	}

	return nil
}

func uploadCAR(ctx context.Context, path string, c *client.Client, space did.DID) (ipld.Link, error) {
	f0, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening file: %w", err)
	}
	defer f0.Close()

	var shdlnks []ipld.Link

	stat, err := f0.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat file: %w", err)
	}

	if stat.IsDir() {
		return nil, fmt.Errorf("%s is a directory, expected a car file", path)
	}

	roots, blocks, err := car.Decode(f0)
	if err != nil {
		return nil, fmt.Errorf("decoding CAR: %w", err)
	}

	if len(roots) == 0 {
		return nil, fmt.Errorf("missing root CID")
	}

	if stat.Size() < sharding.ShardSize {
		hash, err := addBlob(ctx, f0, c, space)
		if err != nil {
			return nil, err
		}

		link := cidlink.Link{Cid: cid.NewCidV1(uint64(multicodec.Car), hash)}

		shdlnks = append(shdlnks, link)
	} else {
		shds, err := sharding.NewSharder(roots, blocks)
		if err != nil {
			return nil, fmt.Errorf("sharding CAR: %w", err)
		}

		for shd, err := range shds {
			if err != nil {
				return nil, fmt.Errorf("ranging shards: %w", err)
			}

			hash, err := addBlob(ctx, shd, c, space)
			if err != nil {
				return nil, fmt.Errorf("uploading shard: %w", err)
			}

			link := cidlink.Link{Cid: cid.NewCidV1(uint64(multicodec.Car), hash)}

			shdlnks = append(shdlnks, link)
		}
	}

	// TODO: build, add and register index

	addOk, err := c.UploadAdd(
		ctx,
		space,
		roots[0],
		shdlnks,
	)

	if err != nil {
		return nil, fmt.Errorf("uploading CAR: %w", err)
	}

	return addOk.Root, nil
}

func uploadFile(ctx context.Context, path string, c *client.Client, space did.DID) (ipld.Link, error) {
	return nil, errors.New("not implemented")
}

func uploadDirectory(ctx context.Context, paths []string, c *client.Client, space did.DID) (ipld.Link, error) {
	return nil, errors.New("not implemented")
}

func addBlob(ctx context.Context, content io.Reader, c *client.Client, space did.DID) (multihash.Multihash, error) {
	contentHash, _, err := c.SpaceBlobAdd(ctx, content, space)
	if err != nil {
		return nil, err
	}

	return contentHash, nil
}
