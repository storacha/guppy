package visitor_test

import (
	"bytes"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-unixfsnode/data"
	"github.com/ipfs/go-unixfsnode/data/builder"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"github.com/storacha/guppy/pkg/preparation/dags/model"
	"github.com/storacha/guppy/pkg/preparation/dags/visitor"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo"
	"github.com/storacha/guppy/pkg/preparation/testutil"
	"github.com/storacha/guppy/pkg/preparation/types/id"
	"github.com/stretchr/testify/require"
)

// From "github.com/ipfs/go-unixfsnode/data/builder"
var (
	fileLinkProto = cidlink.LinkPrototype{
		Prefix: cid.Prefix{
			Version:  1,
			Codec:    uint64(multicodec.DagPb),
			MhType:   multihash.SHA2_256,
			MhLength: 32,
		},
	}
	leafLinkProto = cidlink.LinkPrototype{
		Prefix: cid.Prefix{
			Version:  1,
			Codec:    uint64(multicodec.Raw),
			MhType:   multihash.SHA2_256,
			MhLength: 32,
		},
	}
)

func pbNode(t *testing.T) datamodel.Node {
	t.Helper()

	node, err := builder.BuildUnixFS(func(b *builder.Builder) {
		builder.FileSize(b, 128)
		builder.BlockSizes(b, []uint64{8, 8, 8, 8, 8, 8, 8, 8})
	})

	dpbb := dagpb.Type.PBNode.NewBuilder()
	pbm, err := dpbb.BeginMap(2)
	require.NoError(t, err)
	pblb, err := pbm.AssembleEntry("Links")
	require.NoError(t, err)
	pbl, err := pblb.BeginList(0)
	require.NoError(t, err)
	err = pbl.Finish()
	require.NoError(t, err)
	err = pbm.AssembleKey().AssignString("Data")
	require.NoError(t, err)
	err = pbm.AssembleValue().AssignBytes(data.EncodeUnixFSData(node))
	require.NoError(t, err)
	err = pbm.Finish()
	require.NoError(t, err)
	return dpbb.Build()
}

func TestUnixFSFileNodeVisitorLinkSystem(t *testing.T) {
	repo := sqlrepo.New(testutil.CreateTestDB(t))
	sourceID := id.New()
	reader := visitor.ReaderPositionFromReader(bytes.NewReader([]byte("some data")))

	v := visitor.NewUnixFSFileNodeVisitor(
		t.Context(),
		repo,
		sourceID,
		"some/path",
		reader,
		func(node model.Node, data []byte) error { return nil },
	)

	t.Run("encodes a UnixFS node", func(t *testing.T) {
		pbnode := pbNode(t)

		encoderChooser := v.LinkSystem().EncoderChooser
		encoder, err := encoderChooser(fileLinkProto)
		require.NoError(t, err)
		err = encoder(pbnode, bytes.NewBuffer(nil))
		require.NoError(t, err)
	})

	t.Run("encodes a leaf node", func(t *testing.T) {
		encoderChooser := v.LinkSystem().EncoderChooser
		encoder, err := encoderChooser(leafLinkProto)
		require.NoError(t, err)
		err = encoder(basicnode.NewBytes([]byte{}), bytes.NewBuffer(nil))
		require.NoError(t, err)
	})
}

func TestUnixFSDirectoryNodeVisitorLinkSystem(t *testing.T) {
	repo := sqlrepo.New(testutil.CreateTestDB(t))

	v := visitor.NewUnixFSDirectoryNodeVisitor(
		t.Context(),
		repo,
		func(node model.Node, data []byte) error { return nil },
	)

	t.Run("encodes a UnixFS node", func(t *testing.T) {
		pbnode := pbNode(t)

		encoderChooser := v.LinkSystem().EncoderChooser
		encoder, err := encoderChooser(fileLinkProto)
		require.NoError(t, err)
		err = encoder(pbnode, bytes.NewBuffer(nil))
		require.NoError(t, err)
	})
}
