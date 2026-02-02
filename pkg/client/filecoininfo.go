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
	
	res, _, err := invokeAndExecute[filecoincap.InfoCaveats, filecoincap.InfoOk](
		ctx,
		c,
		filecoincap.Info,
		space.String(),
		caveats,
		filecoincap.InfoOkType(),
	)
	if err != nil {
		return filecoincap.InfoOk{}, fmt.Errorf("invokeAndExecute `filecoin/info`: %w", err)
	}

	infoOk, failErr := result.Unwrap(res)
	if failErr != nil {
		return filecoincap.InfoOk{}, fmt.Errorf("`filecoin/info` failed: %w", failErr)
	}

	return infoOk, nil
}
