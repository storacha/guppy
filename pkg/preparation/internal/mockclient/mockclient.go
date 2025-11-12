package mockclient

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/multiformats/go-multihash"
	filecoincap "github.com/storacha/go-libstoracha/capabilities/filecoin"
	pdpcap "github.com/storacha/go-libstoracha/capabilities/pdp"
	spaceblobcap "github.com/storacha/go-libstoracha/capabilities/space/blob"
	"github.com/storacha/go-libstoracha/capabilities/types"
	"github.com/storacha/go-libstoracha/capabilities/upload"
	"github.com/storacha/go-libstoracha/testutil"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/core/receipt/fx"
	"github.com/storacha/go-ucanto/did"
	ed25519signer "github.com/storacha/go-ucanto/principal/ed25519/signer"
	"github.com/storacha/guppy/pkg/client"
	"github.com/storacha/guppy/pkg/preparation/storacha"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

type MockClient struct {
	T                             *testing.T
	SpaceBlobAddInvocations       []spaceBlobAddInvocation
	SpaceIndexAddInvocations      []spaceIndexAddInvocation
	SpaceBlobReplicateInvocations []spaceBlobReplicateInvocation
	FilecoinOfferInvocations      []filecoinOfferInvocation
	UploadAddInvocations          []uploadAddInvocation
}

type spaceBlobAddInvocation struct {
	Space     did.DID
	BlobAdded []byte

	ReturnedPDPAccept *invocation.Invocation
	ReturnedLocation  delegation.Delegation
}

type spaceIndexAddInvocation struct {
	Space     did.DID
	IndexCID  cid.Cid
	IndexSize uint64
	RootCID   cid.Cid
}

type spaceBlobReplicateInvocation struct {
	Space              did.DID
	Blob               types.Blob
	ReplicaCount       uint
	LocationCommitment delegation.Delegation
}

type filecoinOfferInvocation struct {
	Space   did.DID
	Content ipld.Link
	Piece   ipld.Link
	Options *client.FilecoinOfferOptions
}

type uploadAddInvocation struct {
	Space  did.DID
	Root   ipld.Link
	Shards []ipld.Link
}

var _ storacha.Client = (*MockClient)(nil)

func (m *MockClient) SpaceBlobAdd(ctx context.Context, content io.Reader, space did.DID, options ...client.SpaceBlobAddOption) (client.AddedBlob, error) {
	contentBytes, err := io.ReadAll(content)
	require.NoError(m.T, err, "reading content for SpaceBlobAdd")

	location := testutil.RandomLocationDelegation(m.T)

	digest, err := multihash.Sum(contentBytes, multihash.SHA2_256, -1)
	require.NoError(m.T, err, "summing digest for SpaceBlobAdd")

	dummyPrincipal, err := ed25519signer.Generate()
	require.NoError(m.T, err)

	pdpAcceptInv, err := pdpcap.Accept.Invoke(
		dummyPrincipal,
		dummyPrincipal.DID(),
		space.DID().String(),
		pdpcap.AcceptCaveats{
			Blob: digest,
		},
	)
	require.NoError(m.T, err)
	m.SpaceBlobAddInvocations = append(m.SpaceBlobAddInvocations, spaceBlobAddInvocation{
		Space:             space,
		BlobAdded:         contentBytes,
		ReturnedPDPAccept: (*invocation.Invocation)(&pdpAcceptInv),
		ReturnedLocation:  location,
	})

	return client.AddedBlob{
		Multihash: digest,
		Location:  location,
		PDPAccept: (*invocation.Invocation)(&pdpAcceptInv),
	}, nil
}

func (m *MockClient) SpaceIndexAdd(ctx context.Context, indexCID cid.Cid, indexSize uint64, rootCID cid.Cid, space did.DID) error {
	m.SpaceIndexAddInvocations = append(m.SpaceIndexAddInvocations, spaceIndexAddInvocation{
		Space:     space,
		IndexCID:  indexCID,
		IndexSize: indexSize,
		RootCID:   rootCID,
	})

	return nil
}

func (m *MockClient) FilecoinOffer(ctx context.Context, space did.DID, content ipld.Link, piece ipld.Link, opts ...client.FilecoinOfferOption) (filecoincap.OfferOk, error) {
	m.FilecoinOfferInvocations = append(m.FilecoinOfferInvocations, filecoinOfferInvocation{
		Space:   space,
		Content: content,
		Piece:   piece,
		Options: client.NewFilecoinOfferOptions(opts),
	})

	return filecoincap.OfferOk{Piece: piece}, nil
}

func (m *MockClient) SpaceBlobReplicate(ctx context.Context, space did.DID, blob types.Blob, replicaCount uint, locationCommitment delegation.Delegation) (spaceblobcap.ReplicateOk, fx.Effects, error) {
	m.SpaceBlobReplicateInvocations = append(m.SpaceBlobReplicateInvocations, spaceBlobReplicateInvocation{
		Space:              space,
		Blob:               blob,
		ReplicaCount:       replicaCount,
		LocationCommitment: locationCommitment,
	})

	sitePromises := make([]types.Promise, replicaCount)
	for i := range sitePromises {
		siteDigest, err := multihash.Encode(fmt.Appendf(nil, "test-replicated-site-%d", i), multihash.IDENTITY)
		require.NoError(m.T, err, "encoding site digest")
		sitePromises[i] = types.Promise{
			UcanAwait: types.Await{
				Selector: ".out.ok.site",
				Link:     cidlink.Link{Cid: cid.NewCidV1(cid.Raw, siteDigest)},
			},
		}
	}
	return spaceblobcap.ReplicateOk{Site: sitePromises}, nil, nil
}

func (m *MockClient) UploadAdd(ctx context.Context, space did.DID, root ipld.Link, shards []ipld.Link) (upload.AddOk, error) {
	m.UploadAddInvocations = append(m.UploadAddInvocations, uploadAddInvocation{
		Space:  space,
		Root:   root,
		Shards: shards,
	})

	return upload.AddOk{Root: root, Shards: shards}, nil
}
