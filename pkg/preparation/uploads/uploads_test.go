package uploads_test

// func TestIndexForUpload(t *testing.T) {
// 	db := testutil.CreateTestDB(t)
// 	repo := sqlrepo.New(db)
// 	api := uploads.API{
// 		Repo: repo,
// 	}

// 	configurationsAPI := configurations.API{Repo: repo}
// 	sourcesAPI := sources.API{Repo: repo}

// 	configuration, err := configurationsAPI.CreateConfiguration(t.Context(), "Large Upload Configuration", configurationsmodel.WithShardSize(1<<16))
// 	require.NoError(t, err)
// 	source, err := sourcesAPI.CreateSource(t.Context(), "Large Upload Source", ".")
// 	require.NoError(t, err)
// 	err = repo.AddSourceToConfiguration(t.Context(), configuration.ID(), source.ID())
// 	require.NoError(t, err)
// 	uploads, err := api.CreateUploads(t.Context(), configuration.ID())
// 	require.NoError(t, err)
// 	require.Len(t, uploads, 1, "expected exactly one upload to be created")
// 	upload := uploads[0]

// 	node1, _, err := repo.FindOrCreateRawNode(t.Context(), testutil.RandomCID(t), 21, "dir/file1", id.New(), 0)
// 	require.NoError(t, err)
// 	node2, _, err := repo.FindOrCreateRawNode(t.Context(), testutil.RandomCID(t), 21, "dir/file2", id.New(), 0)
// 	require.NoError(t, err)
// 	node3, _, err := repo.FindOrCreateRawNode(t.Context(), testutil.RandomCID(t), 26, "dir/dir2/file3", id.New(), 0)
// 	require.NoError(t, err)

// 	shard, err := repo.CreateShard(t.Context(), uploadID)

// 	err = repo.AddNodeToShard(t.Context(), shard.ID(), node1.CID())
// 	require.NoError(t, err)
// 	err = repo.AddNodeToShard(t.Context(), shard.ID(), node2.CID())
// 	require.NoError(t, err)
// 	err = repo.AddNodeToShard(t.Context(), shard.ID(), node3.CID())
// 	require.NoError(t, err)

// 	// carReader, err := api.CarForShard(t.Context(), shard.ID())
// 	// require.NoError(t, err)

// 	// // Read in the entire CAR, so we can create an [io.ReaderAt] for the
// 	// // blockstore. In the future, the reader we create could be an [io.ReaderAt]
// 	// // itself, as it technically knows enough information to jump around. However,
// 	// // that's complex, and in our use case, we're going to end up reading the
// 	// // whole thing anyway to store it in Storacha.
// 	// carBytes, err := io.ReadAll(carReader)
// 	// require.NoError(t, err)
// 	// bufferedCarReader := bytes.NewReader(carBytes)

// 	// // Try to read the CAR bytes as a CAR.
// 	// bs, err := blockstore.NewReadOnly(bufferedCarReader, nil)
// 	// require.NoError(t, err)

// 	// cidsCh, err := bs.AllKeysChan(t.Context())
// 	// require.NoError(t, err)
// 	// var cids []cid.Cid
// 	// for cid := range cidsCh {
// 	// 	cids = append(cids, cid)
// 	// }
// 	// require.Equal(t, []cid.Cid{node1.CID(), node2.CID(), node3.CID()}, cids)

// 	// var b blocks.Block

// 	// b, err = bs.Get(t.Context(), node1.CID())
// 	// require.NoError(t, err)
// 	// require.Equal(t, []byte("BLOCK DATA: dir/file1"), b.RawData())

// 	// b, err = bs.Get(t.Context(), node2.CID())
// 	// require.NoError(t, err)
// 	// require.Equal(t, []byte("BLOCK DATA: dir/file2"), b.RawData())

// 	// b, err = bs.Get(t.Context(), node3.CID())
// 	// require.NoError(t, err)
// 	// require.Equal(t, []byte("BLOCK DATA: dir/dir2/file3"), b.RawData())

// 	indexReader, err := api.IndexForUpload(t.Context(), upload.ID())
// 	index, err := blobindex.Extract(indexReader)
// 	require.NoError(t, err)
// 	require.Equal(t, 1, index.Content())
// }
