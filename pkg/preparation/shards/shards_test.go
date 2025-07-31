package shards_test

import (
	"database/sql"
	"testing"

	"github.com/ipfs/go-cid"
	configurationsmodel "github.com/storacha/guppy/pkg/preparation/configurations/model"
	"github.com/storacha/guppy/pkg/preparation/shards"
	"github.com/storacha/guppy/pkg/preparation/shards/model"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo/util"
	"github.com/storacha/guppy/pkg/preparation/testutil"
	"github.com/storacha/guppy/pkg/preparation/types/id"
	"github.com/stretchr/testify/require"
)

func TestAddNodeToUploadShardsAndCloseUploadShards(t *testing.T) {
	db := testutil.CreateTestDB(t)
	repo := sqlrepo.New(db)
	api := shards.API{Repo: repo}

	configuration, err := repo.CreateConfiguration(t.Context(), "Test Config", configurationsmodel.WithShardSize(1<<16))
	require.NoError(t, err)
	source, err := repo.CreateSource(t.Context(), "Test Source", ".")
	require.NoError(t, err)
	uploads, err := repo.CreateUploads(t.Context(), configuration.ID(), []id.SourceID{source.ID()})
	require.NoError(t, err)
	require.Len(t, uploads, 1)
	upload := uploads[0]
	nodeCid1 := testutil.RandomCID(t)

	// with no shards, creates a new shard and adds the node to it

	openShards, err := repo.ShardsForUploadByStatus(t.Context(), upload.ID(), model.ShardStateOpen)
	require.NoError(t, err)
	require.Len(t, openShards, 0)
	_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCid1, 1<<14, "some/path", source.ID(), 0)
	require.NoError(t, err)

	created, err := api.AddNodeToUploadShards(t.Context(), upload.ID(), nodeCid1)
	require.NoError(t, err)

	require.True(t, created, "expected a new shard to be created")
	openShards, err = repo.ShardsForUploadByStatus(t.Context(), upload.ID(), model.ShardStateOpen)
	require.NoError(t, err)
	require.Len(t, openShards, 1)
	firstShard := openShards[0]

	foundNodeCids := nodesInShard(t, db, firstShard.ID())
	require.ElementsMatch(t, []cid.Cid{nodeCid1}, foundNodeCids)

	// with an open shard with room, adds the node to the shard

	nodeCid2 := testutil.RandomCID(t)
	_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCid2, 1<<14, "some/other/path", source.ID(), 0)
	require.NoError(t, err)

	created, err = api.AddNodeToUploadShards(t.Context(), upload.ID(), nodeCid2)
	require.NoError(t, err)

	require.False(t, created, "expected no new shard to be created")
	openShards, err = repo.ShardsForUploadByStatus(t.Context(), upload.ID(), model.ShardStateOpen)
	require.NoError(t, err)
	require.Len(t, openShards, 1)
	require.Equal(t, firstShard.ID(), openShards[0].ID())

	foundNodeCids = nodesInShard(t, db, firstShard.ID())
	require.ElementsMatch(t, []cid.Cid{nodeCid1, nodeCid2}, foundNodeCids)

	// with an open shard without room, closes the shard and creates another

	nodeCid3 := testutil.RandomCID(t)
	_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCid3, 1<<15, "yet/other/path", source.ID(), 0)
	require.NoError(t, err)

	created, err = api.AddNodeToUploadShards(t.Context(), upload.ID(), nodeCid3)
	require.NoError(t, err)

	require.True(t, created, "expected a new shard to be created")
	closedShards, err := repo.ShardsForUploadByStatus(t.Context(), upload.ID(), model.ShardStateClosed)
	require.NoError(t, err)
	require.Len(t, closedShards, 1)
	require.Equal(t, firstShard.ID(), closedShards[0].ID())

	foundNodeCids = nodesInShard(t, db, firstShard.ID())
	require.ElementsMatch(t, []cid.Cid{nodeCid1, nodeCid2}, foundNodeCids)

	openShards, err = repo.ShardsForUploadByStatus(t.Context(), upload.ID(), model.ShardStateOpen)
	require.NoError(t, err)
	require.Len(t, openShards, 1)
	secondShard := openShards[0]
	require.NotEqual(t, firstShard.ID(), secondShard.ID())

	foundNodeCids = nodesInShard(t, db, secondShard.ID())
	require.ElementsMatch(t, []cid.Cid{nodeCid3}, foundNodeCids)

	// finally, close the last shard with CloseUploadShards()

	err = api.CloseUploadShards(t.Context(), upload.ID())
	require.NoError(t, err)

	closedShards, err = repo.ShardsForUploadByStatus(t.Context(), upload.ID(), model.ShardStateClosed)
	require.NoError(t, err)
	require.Len(t, closedShards, 2)
	require.Equal(t, firstShard.ID(), closedShards[0].ID())

	closedShardIDs := make([]id.ShardID, 0, len(closedShards))
	for _, closedShard := range closedShards {
		closedShardIDs = append(closedShardIDs, closedShard.ID())
	}
	require.ElementsMatch(t, closedShardIDs, []id.ShardID{firstShard.ID(), secondShard.ID()})

	openShards, err = repo.ShardsForUploadByStatus(t.Context(), upload.ID(), model.ShardStateOpen)
	require.NoError(t, err)
	require.Len(t, openShards, 0)
}

// (Until the repo has a way to query for this itself...)
func nodesInShard(t *testing.T, db *sql.DB, shardID id.ShardID) []cid.Cid {
	rows, err := db.QueryContext(t.Context(), `SELECT node_cid FROM nodes_in_shards WHERE shard_id = ?`, shardID)
	require.NoError(t, err)
	defer rows.Close()

	var foundNodeCids []cid.Cid
	for rows.Next() {
		var foundNodeCid cid.Cid
		err = rows.Scan(util.DbCid(&foundNodeCid))
		require.NoError(t, err)
		foundNodeCids = append(foundNodeCids, foundNodeCid)
	}
	return foundNodeCids
}
