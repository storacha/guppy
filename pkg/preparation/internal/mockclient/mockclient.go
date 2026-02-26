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
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"

	"github.com/storacha/guppy/pkg/client"
	"github.com/storacha/guppy/pkg/preparation/storacha"
)

type MockClient struct {
	T *testing.T

	// If set, these errors will be immediately returned by the corresponding
	// methods, to simulate failure. These may be set and removed between calls.
	SpaceBlobAddError       error
	SpaceIndexAddError      error
	SpaceBlobReplicateError error
	FilecoinOfferError      error
	UploadAddError          error

	// Output
	SpaceBlobAddInvocations       []spaceBlobAddInvocation
	SpaceIndexAddInvocations      []spaceIndexAddInvocation
	SpaceBlobReplicateInvocations []spaceBlobReplicateInvocation
	FilecoinOfferInvocations      []filecoinOfferInvocation
	UploadAddInvocations          []uploadAddInvocation
	UploadRemoveInvocations       []uploadRemoveInvocation
}

type spaceBlobAddInvocation struct {
	Space     did.DID
	BlobAdded []byte

	ReturnedPDPAccept invocation.Invocation
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

type uploadRemoveInvocation struct {
	Space did.DID
	Root  ipld.Link
}

var _ storacha.Client = (*MockClient)(nil)

func (m *MockClient) SpaceBlobAdd(ctx context.Context, content io.Reader, space did.DID, options ...client.SpaceBlobAddOption) (client.AddedBlob, error) {
	cfg := client.NewSpaceBlobAddConfig(options...)

	contentBytes, err := io.ReadAll(content)
	require.NoError(m.T, err, "reading content for SpaceBlobAdd")

	if m.SpaceBlobAddError != nil {
		m.SpaceBlobAddInvocations = append(m.SpaceBlobAddInvocations, spaceBlobAddInvocation{
			Space:             space,
			BlobAdded:         contentBytes,
			ReturnedPDPAccept: nil,
			ReturnedLocation:  nil,
		})
		return client.AddedBlob{}, m.SpaceBlobAddError
	}

	location := testutil.RandomLocationDelegation(m.T)

	digest := cfg.PrecomputedDigest()
	if len(digest) == 0 {
		digest, err = multihash.Sum(contentBytes, multihash.SHA2_256, -1)
		require.NoError(m.T, err, "summing digest for SpaceBlobAdd")
	}

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
		ReturnedPDPAccept: pdpAcceptInv,
		ReturnedLocation:  location,
	})

	return client.AddedBlob{
		Digest:    digest,
		Size:      uint64(len(contentBytes)),
		Location:  location,
		PDPAccept: pdpAcceptInv,
	}, nil
}

func (m *MockClient) SpaceIndexAdd(ctx context.Context, indexCID cid.Cid, indexSize uint64, rootCID cid.Cid, space did.DID) error {
	m.SpaceIndexAddInvocations = append(m.SpaceIndexAddInvocations, spaceIndexAddInvocation{
		Space:     space,
		IndexCID:  indexCID,
		IndexSize: indexSize,
		RootCID:   rootCID,
	})

	if m.SpaceIndexAddError != nil {
		return m.SpaceIndexAddError
	}

	return nil
}

func (m *MockClient) FilecoinOffer(ctx context.Context, space did.DID, content ipld.Link, piece ipld.Link, opts ...client.FilecoinOfferOption) (filecoincap.OfferOk, error) {
	m.FilecoinOfferInvocations = append(m.FilecoinOfferInvocations, filecoinOfferInvocation{
		Space:   space,
		Content: content,
		Piece:   piece,
		Options: client.NewFilecoinOfferOptions(opts),
	})

	if m.FilecoinOfferError != nil {
		return filecoincap.OfferOk{}, m.FilecoinOfferError
	}

	return filecoincap.OfferOk{Piece: piece}, nil
}

func (m *MockClient) SpaceBlobReplicate(ctx context.Context, space did.DID, blob types.Blob, replicaCount uint, locationCommitment delegation.Delegation) (spaceblobcap.ReplicateOk, fx.Effects, error) {
	m.SpaceBlobReplicateInvocations = append(m.SpaceBlobReplicateInvocations, spaceBlobReplicateInvocation{
		Space:              space,
		Blob:               blob,
		ReplicaCount:       replicaCount,
		LocationCommitment: locationCommitment,
	})

	if m.SpaceBlobReplicateError != nil {
		return spaceblobcap.ReplicateOk{}, nil, m.SpaceBlobReplicateError
	}

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

	if m.UploadAddError != nil {
		return upload.AddOk{}, m.UploadAddError
	}

	return upload.AddOk{Root: root, Shards: shards}, nil
}

func (m *MockClient) UploadRemove(ctx context.Context, space did.DID, root ipld.Link) (upload.RemoveOk, error) {
	m.UploadRemoveInvocations = append(m.UploadRemoveInvocations, uploadRemoveInvocation{
		Space: space,
		Root:  root,
	})

	return upload.RemoveOk{}, nil
}
