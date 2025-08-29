package sqlrepo_test

import (
	"testing"

	"github.com/ipfs/go-cid"
	dagsmodel "github.com/storacha/guppy/pkg/preparation/dags/model"
	"github.com/storacha/guppy/pkg/preparation/shards/model"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo"
	"github.com/storacha/guppy/pkg/preparation/testutil"
	"github.com/storacha/guppy/pkg/preparation/types/id"
	"github.com/stretchr/testify/require"
)

func TestAddNodeToShard(t *testing.T) {
	repo := sqlrepo.New(testutil.CreateTestDB(t))

	makeRawNodeWithSize := func(size uint64) dagsmodel.Node {
		node, _, err := repo.FindOrCreateRawNode(t.Context(), testutil.RandomCID(t), size, "dir/file", id.New(), 0)
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
	err = repo.AddNodeToShard(t.Context(), shard.ID(), node1.CID(), 1)
	require.NoError(t, err)
	shard = reloadShard(shard.ID())
	require.Equal(t, uint64(100+1+10), shard.Size())

	node2 := makeRawNodeWithSize(20)
	err = repo.AddNodeToShard(t.Context(), shard.ID(), node2.CID(), 2)
	require.NoError(t, err)
	shard = reloadShard(shard.ID())
	require.Equal(t, uint64(100+1+10+2+20), shard.Size())

	node3 := makeRawNodeWithSize(30)
	err = repo.AddNodeToShard(t.Context(), shard.ID(), node3.CID(), 3)
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
