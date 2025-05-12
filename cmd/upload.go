package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"

	"github.com/ipfs/go-cid"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	captypes "github.com/storacha/go-libstoracha/capabilities/types"
	uploadcap "github.com/storacha/go-libstoracha/capabilities/upload"
	uclient "github.com/storacha/go-ucanto/client"
	"github.com/storacha/go-ucanto/core/car"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/core/ipld"
	"github.com/storacha/go-ucanto/core/receipt"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/principal"
	"github.com/storacha/go-w3up/car/sharding"
	"github.com/storacha/go-w3up/client"
	"github.com/storacha/go-w3up/cmd/util"
	"github.com/urfave/cli/v2"
)

// upload handles file and directory uploads to Storacha
func upload(cCtx *cli.Context) error {
	signer := util.MustGetSigner()
	conn := util.MustGetConnection()
	space := util.MustParseDID(cCtx.String("space"))
	proofs := []delegation.Delegation{util.MustGetProof(cCtx.String("proof"))}
	receiptsURL := util.MustGetReceiptsURL()

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
		root, err = uploadCAR(cCtx.Context, paths[0], signer, conn, space, proofs, receiptsURL)
		if err != nil {
			return err
		}
	} else {
		if len(paths) == 1 && !isWrap {
			var err error
			root, err = uploadFile(cCtx.Context, paths[0], signer, conn, space, proofs, receiptsURL)
			if err != nil {
				return err
			}
		} else {
			var err error
			root, err = uploadDirectory(cCtx.Context, paths, signer, conn, space, proofs, receiptsURL)
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

func uploadCAR(ctx context.Context, path string, signer principal.Signer, conn uclient.Connection, space did.DID, proofs []delegation.Delegation, receiptsURL *url.URL) (ipld.Link, error) {
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
		hash, err := addBlob(ctx, f0, signer, conn, space, proofs, receiptsURL)
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

			hash, err := addBlob(ctx, shd, signer, conn, space, proofs, receiptsURL)
			if err != nil {
				return nil, fmt.Errorf("uploading shard: %w", err)
			}

			link := cidlink.Link{Cid: cid.NewCidV1(uint64(multicodec.Car), hash)}

			shdlnks = append(shdlnks, link)
		}
	}

	// TODO: build, add and register index

	// rcpt, err := client.UploadAdd(
	// 	signer,
	// 	space,
	// 	uploadadd.Caveat{
	// 		Root:   roots[0],
	// 		Shards: shdlnks,
	// 	},
	// 	client.WithConnection(conn),
	// 	client.WithProofs(proofs),
	// )
	caveats := uploadcap.AddCaveats{
		Root:   roots[0],
		Shards: shdlnks,
	}

	asProofs := []delegation.Proof{}
	for _, dlg := range proofs {
		asProofs = append(asProofs, delegation.FromDelegation(dlg))
	}

	inv, err := uploadcap.Add.Invoke(signer, conn.ID(), space.String(), caveats, delegation.WithProof(asProofs...))
	if err != nil {
		return nil, err
	}

	resp, err := uclient.Execute([]invocation.Invocation{inv}, conn)
	if err != nil {
		return nil, err
	}

	rcptlnk, ok := resp.Get(inv.Link())
	if !ok {
		return nil, fmt.Errorf("receipt not found: %s", inv.Link())
	}

	reader := receipt.NewAnyReceiptReader(captypes.Converters...)

	rcpt, err := reader.Read(rcptlnk, resp.Blocks())
	if err != nil {
		return nil, err
	}

	_, failErr := result.Unwrap(rcpt.Out())
	if failErr != nil {
		return nil, fmt.Errorf("%+v", failErr)
	}

	fmt.Printf("Uploaded %d bytes\n", stat.Size())

	return roots[0], nil
}

func uploadFile(ctx context.Context, path string, signer principal.Signer, conn uclient.Connection, space did.DID, proofs []delegation.Delegation, receiptsURL *url.URL) (ipld.Link, error) {
	return nil, errors.New("not implemented")
}

func uploadDirectory(ctx context.Context, paths []string, signer principal.Signer, conn uclient.Connection, space did.DID, proofs []delegation.Delegation, receiptsURL *url.URL) (ipld.Link, error) {
	return nil, errors.New("not implemented")
}

func addBlob(ctx context.Context, content io.Reader, signer principal.Signer, conn uclient.Connection, space did.DID, proofs []delegation.Delegation, receiptsURL *url.URL) (multihash.Multihash, error) {
	contentHash, _, err := client.BlobAdd(ctx, content, signer, space, receiptsURL, client.WithConnection(conn), client.WithProofs(proofs))
	if err != nil {
		return nil, err
	}

	return contentHash, nil
}
