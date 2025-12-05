package client

import (
	"context"
	"fmt"

	"github.com/ipfs/go-cid"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	filecoincap "github.com/storacha/go-libstoracha/capabilities/filecoin"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/did"
)

// FilecoinInfo retrieves info about a content piece in Filecoin deals.
func (c *Client) FilecoinInfo(ctx context.Context, piece cid.Cid, space did.DID) (filecoincap.InfoOk, error) {
	pieceLink := cidlink.Link{Cid: piece}
	caveats := filecoincap.InfoCaveats{
		Piece: pieceLink,
	}

	inv, err := invoke[filecoincap.InfoCaveats, filecoincap.InfoOk](
		c,
		filecoincap.Info,
		space.String(),
		caveats,
	)
	if err != nil {
		return filecoincap.InfoOk{}, fmt.Errorf("invoking `filecoin/info`: %w", err)
	}

	res, _, err := execute[filecoincap.InfoCaveats, filecoincap.InfoOk](
		ctx,
		c,
		filecoincap.Info,
		inv,
		filecoincap.InfoOkType(),
	)
	if err != nil {
		return filecoincap.InfoOk{}, fmt.Errorf("executing `filecoin/info`: %w", err)
	}

	infoOk, failErr := result.Unwrap(res)
	if failErr != nil {
		return filecoincap.InfoOk{}, fmt.Errorf("`filecoin/info` failed: %w", failErr)
	}

	return infoOk, nil
}
