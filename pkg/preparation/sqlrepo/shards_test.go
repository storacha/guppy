package sqlrepo_test

import (
	"testing"

	"github.com/ipfs/go-cid"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/storacha/go-libstoracha/testutil"
	"github.com/storacha/go-ucanto/did"
	dagsmodel "github.com/storacha/guppy/pkg/preparation/dags/model"
	"github.com/storacha/guppy/pkg/preparation/internal/testdb"
	"github.com/storacha/guppy/pkg/preparation/shards/model"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo"
	"github.com/storacha/guppy/pkg/preparation/types/id"
	"github.com/stretchr/testify/require"
)

func TestAddNodeToShard(t *testing.T) {
	repo := sqlrepo.New(testdb.CreateTestDB(t))

	spaceDID, err := did.Parse("did:storacha:space:example")
	require.NoError(t, err)

	makeRawNodeWithSize := func(size uint64) dagsmodel.Node {
		node, _, err := repo.FindOrCreateRawNode(t.Context(), testutil.RandomCID(t).(cidlink.Link).Cid, size, spaceDID, "dir/file", id.New(), 0)
		require.NoError(t, err)
		return node
	}

	reloadShard := func(shardID id.ShardID) *model.Shard {
		shard, err := repo.GetShardByID(t.Context(), shardID)
		require.NoError(t, err)
		return shard
	}

	uploadID := id.New()
	shard, err := repo.CreateShard(t.Context(), uploadID, 100)
	require.NoError(t, err)
	require.Equal(t, uint64(100), shard.Size())

	node1 := makeRawNodeWithSize(10)
	err = repo.AddNodeToShard(t.Context(), shard.ID(), node1.CID(), spaceDID, 1)
	require.NoError(t, err)
	shard = reloadShard(shard.ID())
	require.Equal(t, uint64(100+1+10), shard.Size())

	node2 := makeRawNodeWithSize(20)
	err = repo.AddNodeToShard(t.Context(), shard.ID(), node2.CID(), spaceDID, 2)
	require.NoError(t, err)
	shard = reloadShard(shard.ID())
	require.Equal(t, uint64(100+1+10+2+20), shard.Size())

	node3 := makeRawNodeWithSize(30)
	err = repo.AddNodeToShard(t.Context(), shard.ID(), node3.CID(), spaceDID, 3)
	require.NoError(t, err)
	shard = reloadShard(shard.ID())
	require.Equal(t, uint64(100+1+10+2+20+3+30), shard.Size())

	type nodeInfo struct {
		cid    cid.Cid
		offset uint64
	}

	nodeInfos := make([]nodeInfo, 0, 3)
	err = repo.ForEachNode(t.Context(), shard.ID(), func(node dagsmodel.Node, shardOffset uint64) error {
		nodeInfos = append(nodeInfos, nodeInfo{
			cid:    node.CID(),
			offset: shardOffset,
		})
		return nil
	})
	require.NoError(t, err)

	require.Equal(t, []nodeInfo{
		{cid: node1.CID(), offset: 100 + 1},
		{cid: node2.CID(), offset: 100 + 1 + 10 + 2},
		{cid: node3.CID(), offset: 100 + 1 + 10 + 2 + 20 + 3},
	}, nodeInfos)
}
