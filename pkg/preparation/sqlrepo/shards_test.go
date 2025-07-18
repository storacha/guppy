package sqlrepo_test

import (
	"database/sql"
	"testing"

	"github.com/ipfs/go-cid"
	configurationsmodel "github.com/storacha/guppy/pkg/preparation/configurations/model"
	"github.com/storacha/guppy/pkg/preparation/shards/model"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo/util"
	"github.com/storacha/guppy/pkg/preparation/testutil"
	"github.com/storacha/guppy/pkg/preparation/types/id"
	"github.com/stretchr/testify/require"
)

// TK: Is the shard size the max total size of the nodes, or the max size of the
// full shard CAR blob?

func TestAddNodeToUploadShard(t *testing.T) {
	const shardSize = 128
	const nodeSize = 32

	db := testutil.CreateTestDB(t)
	repo := sqlrepo.New(db)

	configuration, err := repo.CreateConfiguration(t.Context(), "Test Config", configurationsmodel.WithShardSize(shardSize))
	require.NoError(t, err)
	source, err := repo.CreateSource(t.Context(), "Test Source", ".")
	require.NoError(t, err)
	uploads, err := repo.CreateUploads(t.Context(), configuration.ID(), []id.SourceID{source.ID()})
	require.NoError(t, err)
	require.Len(t, uploads, 1)
	upload := uploads[0]
	nodeCid1 := randomCID(t)

	// with no shards, creates a new shard and adds the node to it

	openShards, err := repo.ShardsForUploadByStatus(t.Context(), upload.ID(), model.ShardStateOpen)
	require.NoError(t, err)
	require.Len(t, openShards, 0)
	_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCid1, nodeSize, "some/path", source.ID(), 0)
	require.NoError(t, err)

	err = repo.AddNodeToUploadShard(t.Context(), upload.ID(), nodeCid1)
	require.NoError(t, err)

	openShards, err = repo.ShardsForUploadByStatus(t.Context(), upload.ID(), model.ShardStateOpen)
	require.NoError(t, err)
	require.Len(t, openShards, 1)
	shard := openShards[0]

	foundNodeCids := nodesInShard(t, db, shard.ID())
	require.Len(t, foundNodeCids, 1)
	require.Equal(t, nodeCid1, foundNodeCids[0])

	// with an open shard with room, adds the node to the shard

	nodeCid2 := randomCID(t)
	_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCid2, nodeSize, "some/other/path", source.ID(), 0)
	require.NoError(t, err)

	err = repo.AddNodeToUploadShard(t.Context(), upload.ID(), nodeCid2)
	require.NoError(t, err)

	openShards, err = repo.ShardsForUploadByStatus(t.Context(), upload.ID(), model.ShardStateOpen)
	require.NoError(t, err)
	require.Len(t, openShards, 1)

	foundNodeCids = nodesInShard(t, db, shard.ID())
	require.Len(t, foundNodeCids, 2)
	require.ElementsMatch(t, []cid.Cid{nodeCid1, nodeCid2}, foundNodeCids)
}

// (Until the repo has a way to query for this itself...)
func nodesInShard(t *testing.T, db *sql.DB, shardID id.ShardID) []cid.Cid {
	rows, err := db.QueryContext(t.Context(), `SELECT node_cid FROM nodes_in_shards WHERE shard_id = ?`, shardID)
	require.NoError(t, err)
	defer rows.Close()

	var foundNodeCids []cid.Cid
	for rows.Next() {
		var foundNodeCid cid.Cid
		err = rows.Scan(util.CidScanner{Dst: &foundNodeCid})
		require.NoError(t, err)
		foundNodeCids = append(foundNodeCids, foundNodeCid)
	}
	return foundNodeCids
}
