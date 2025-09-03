package client

import (
	"context"
	"fmt"

	blobreplicacap "github.com/storacha/go-libstoracha/capabilities/blob/replica"
	spaceblobcap "github.com/storacha/go-libstoracha/capabilities/space/blob"
	captypes "github.com/storacha/go-libstoracha/capabilities/types"
	uclient "github.com/storacha/go-ucanto/client"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/core/ipld"
	"github.com/storacha/go-ucanto/core/receipt"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/did"
)

// SpaceBlobReplicate adds a blob to the service. The issuer needs proof of
// `space/blob/add` delegated capability.
//
// Required delegated capability proofs: `space/blob/replicate`
//
// The `space` is the resource the invocation applies to. It is typically the
// DID of a space.
//
// The `blob` is the blob content to be replicated. It MUST already be stored in
// the passed space.
//
// The `site` is a location commitment issued by a storage node that is
// currently storing the blob.
//
// `replicas` is the total number of replicas to instruct. i.e. the total number
// of copies the network should ensure exist. It MUST be greater than 0.
func (c *Client) SpaceBlobReplicate(ctx context.Context, space did.DID, blob captypes.Blob, site delegation.Delegation, replicas uint) ([]invocation.Invocation, error) {
	var proofs []delegation.Proof
	for _, d := range c.Proofs() {
		proofs = append(proofs, delegation.FromDelegation(d))
	}

	inv, err := spaceblobcap.Replicate.Invoke(
		c.Issuer(),
		c.connection.ID(),
		space.String(),
		spaceblobcap.ReplicateCaveats{
			Blob:     blob,
			Replicas: replicas,
			Site:     site.Link(),
		},
		delegation.WithProof(proofs...),
	)
	if err != nil {
		return nil, fmt.Errorf("invoking %s: %w", spaceblobcap.ReplicateAbility, err)
	}
	for b, err := range site.Export() {
		if err != nil {
			return nil, fmt.Errorf("exporting location commitment: %w", err)
		}
		if err = inv.Attach(b); err != nil {
			return nil, fmt.Errorf("attaching location commitment to replicate invocation: %w", err)
		}
	}

	res, err := uclient.Execute(ctx, []invocation.Invocation{inv}, c.Connection())
	if err != nil {
		return nil, fmt.Errorf("executing %s invocation: %w", spaceblobcap.ReplicateAbility, err)
	}
	rcptLink, ok := res.Get(inv.Link())
	if !ok {
		return nil, fmt.Errorf("missing receipt for %s invocation: %s", spaceblobcap.ReplicateAbility, inv.Link())
	}
	rcptReader, err := receipt.NewReceiptReaderFromTypes[spaceblobcap.ReplicateOk, ipld.Node](spaceblobcap.ReplicateOkType(), spaceblobcap.ReplicateOkType().TypeSystem().TypeByName("Any"), captypes.Converters...)
	if err != nil {
		return nil, fmt.Errorf("creating %s receipt reader: %w", spaceblobcap.ReplicateAbility, err)
	}
	rcpt, err := rcptReader.Read(rcptLink, res.Blocks())
	if err != nil {
		return nil, fmt.Errorf("reading %s receipt: %w", spaceblobcap.ReplicateAbility, err)
	}
	_, x := result.Unwrap(rcpt.Out())
	if x != nil {
		f, err := util.BindFailure(x)
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("invocation failure: %+v", f)
	}

	var transfers []invocation.Invocation
	for _, f := range rcpt.Fx().Fork() {
		inv, ok := f.Invocation()
		if !ok {
			continue
		}
		if len(inv.Capabilities()) == 0 {
			continue
		}
		cap := inv.Capabilities()[0]
		if cap.Can() == blobreplicacap.TransferAbility {
			transfers = append(transfers, inv)
		}
	}
	return transfers, nil
}
