package visitor_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/ipfs/go-unixfsnode/data/builder"
	"github.com/storacha/guppy/pkg/preparation/dags/model"
	"github.com/storacha/guppy/pkg/preparation/dags/visitor"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo"
	"github.com/storacha/guppy/pkg/preparation/testutil"
	"github.com/storacha/guppy/pkg/preparation/types/id"
	"github.com/stretchr/testify/require"
)

func TestUnixFSVisitorLinkSystem(t *testing.T) {
	repo := sqlrepo.New(testutil.CreateTestDB(t))
	sourceID := id.New()
	reader := visitor.ReaderPositionFromReader(bytes.NewReader([]byte("some data")))

	visitor := visitor.NewUnixFSVisitor(
		t.Context(),
		repo,
		sourceID,
		"some/path",
		reader,
		func(node model.Node, data []byte) error { return nil },
	)

	link, _, err := builder.BuildUnixFSFile(reader, "size-16", visitor.LinkSystem())
	require.NoError(t, err)

	fmt.Printf("link: %v\n", link)
}
