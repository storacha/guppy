package sqlrepo_test

import (
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
	nodeCid := randomCID(t)

	// with no shards, creates a new shard and adds the node to it

	openShards, err := repo.ShardsForUploadByStatus(t.Context(), upload.ID(), model.ShardStateOpen)
	require.NoError(t, err)
	require.Len(t, openShards, 0)

	_, _, err = repo.FindOrCreateRawNode(t.Context(), nodeCid, nodeSize, "some/path", source.ID(), 0)
	require.NoError(t, err)
	err = repo.AddNodeToUploadShard(t.Context(), upload.ID(), nodeCid)
	require.NoError(t, err)

	openShards, err = repo.ShardsForUploadByStatus(t.Context(), upload.ID(), model.ShardStateOpen)
	require.NoError(t, err)
	require.Len(t, openShards, 1)
	shard := openShards[0]

	// (Until the repo has a way to query for this itself...)
	rows, err := db.QueryContext(t.Context(), `SELECT node_cid FROM nodes_in_shards WHERE shard_id = ?`, shard.ID())
	require.NoError(t, err)
	defer rows.Close()
	var foundNodeCids []cid.Cid
	for rows.Next() {
		var foundNodeCid cid.Cid
		err = rows.Scan(util.CidScanner{Dst: &foundNodeCid})
		require.NoError(t, err)
		foundNodeCids = append(foundNodeCids, foundNodeCid)
	}
	require.Len(t, foundNodeCids, 1)
	require.Equal(t, nodeCid, foundNodeCids[0])
}
