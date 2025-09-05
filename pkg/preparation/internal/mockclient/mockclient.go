package mockclient

import (
	"context"
	"io"
	"testing"

	"github.com/ipld/go-ipld-prime"
	"github.com/multiformats/go-multihash"
	"github.com/storacha/go-libstoracha/capabilities/upload"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/guppy/pkg/client"
	"github.com/storacha/guppy/pkg/preparation/storacha"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

type MockClient struct {
	T                        *testing.T
	SpaceBlobAddInvocations  []spaceBlobAddInvocation
	SpaceIndexAddInvocations []spaceIndexAddInvocation
	UploadAddInvocations     []uploadAddInvocation
}

type spaceBlobAddInvocation struct {
	Space     did.DID
	BlobAdded []byte
}

type spaceIndexAddInvocation struct {
	Space     did.DID
	IndexLink ipld.Link
}

type uploadAddInvocation struct {
	Space  did.DID
	Root   ipld.Link
	Shards []ipld.Link
}

var _ storacha.Client = (*MockClient)(nil)

func (m *MockClient) SpaceBlobAdd(ctx context.Context, content io.Reader, space did.DID, options ...client.SpaceBlobAddOption) (multihash.Multihash, delegation.Delegation, error) {
	contentBytes, err := io.ReadAll(content)
	require.NoError(m.T, err, "reading content for SpaceBlobAdd")

	m.SpaceBlobAddInvocations = append(m.SpaceBlobAddInvocations, spaceBlobAddInvocation{
		Space:     space,
		BlobAdded: contentBytes,
	})

	digest, err := multihash.Sum(contentBytes, multihash.SHA2_256, -1)
	return digest, nil, nil
}

func (m *MockClient) SpaceIndexAdd(ctx context.Context, indexLink ipld.Link, space did.DID) error {
	m.SpaceIndexAddInvocations = append(m.SpaceIndexAddInvocations, spaceIndexAddInvocation{
		Space:     space,
		IndexLink: indexLink,
	})

	return nil
}

func (m *MockClient) UploadAdd(ctx context.Context, space did.DID, root ipld.Link, shards []ipld.Link) (upload.AddOk, error) {
	m.UploadAddInvocations = append(m.UploadAddInvocations, uploadAddInvocation{
		Space:  space,
		Root:   root,
		Shards: shards,
	})

	return upload.AddOk{Root: root, Shards: shards}, nil
}
