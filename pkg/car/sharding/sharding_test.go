package sharding_test

import (
	"bytes"
	"crypto/rand"
	"testing"

	"github.com/ipfs/go-cid"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/storacha/go-ucanto/core/ipld"
	"github.com/storacha/go-ucanto/core/ipld/block"
	"github.com/storacha/go-ucanto/core/ipld/hash/sha256"
	"github.com/storacha/guppy/pkg/car/sharding"
	"github.com/stretchr/testify/require"
)

func randomRawBlock(t testing.TB, size int) ipld.Block {
	t.Helper()
	b := make([]byte, size)
	_, err := rand.Read(b)
	if err != nil {
		t.Fatal(err)
	}
	d, err := sha256.Hasher.Sum(b)
	if err != nil {
		t.Fatal(err)
	}
	l := cidlink.Link{Cid: cid.NewCidV1(0x55, d.Bytes())}
	return block.NewBlock(l, b)
}

func TestSharding(t *testing.T) {
	var roots []ipld.Link
	blocks := []ipld.Block{
		randomRawBlock(t, 4000),
		randomRawBlock(t, 4000),
	}
	iterator := func(yield func(ipld.Block, error) bool) {
		for _, b := range blocks {
			if !yield(b, nil) {
				return
			}
		}
	}

	size := 5000

	shards, err := sharding.NewSharder(roots, iterator, sharding.WithShardSize(size))
	require.NoError(t, err)

	var shdbufs [][]byte
	for s, err := range shards {
		require.NoError(t, err)

		buf := new(bytes.Buffer)
		_, err = buf.ReadFrom(s)
		require.NoError(t, err)

		if len(buf.Bytes()) > size {
			t.Fatalf("shard was bigger than max size: %d > %d", len(buf.Bytes()), size)
		}

		shdbufs = append(shdbufs, buf.Bytes())
	}

	require.Len(t, shdbufs, 2, "unexpected number of shards: %d", len(shdbufs))
}
