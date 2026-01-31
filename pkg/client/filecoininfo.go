package client

import (
	"context"
	"fmt"

	filecoincap "github.com/storacha/go-libstoracha/capabilities/filecoin"
	"github.com/storacha/go-ucanto/core/ipld"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/did"
)

func (c *Client) FilecoinInfo(ctx context.Context, space did.DID, piece ipld.Link) (filecoincap.InfoOk, error) {
	caveats := filecoincap.InfoCaveats{
		Piece: piece,
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
