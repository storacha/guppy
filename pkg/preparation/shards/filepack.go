package shards

import (
	"context"
	"fmt"
	"io"

	"github.com/storacha/guppy/pkg/preparation/dags/model"
)

type FilepackEncoder struct {
	nodeReader NodeDataGetter
}

var _ ShardEncoder = (*FilepackEncoder)(nil)

// NewFilepackEncoder creates a new shard encoder that outputs filepack encoded shards.
func NewFilepackEncoder(nodeReader NodeDataGetter) *FilepackEncoder {
	return &FilepackEncoder{nodeReader}
}

func (f *FilepackEncoder) Encode(ctx context.Context, nodes []model.Node, w io.Writer) error {
	for _, n := range nodes {
		data, err := f.nodeReader.GetData(ctx, n)
		if err != nil {
			return fmt.Errorf("getting data for %s: %w", n.CID(), err)
		}
		_, err = w.Write(data)
		if err != nil {
			return fmt.Errorf("writing data for %s: %w", n.CID(), err)
		}
	}
	return nil
}

func (FilepackEncoder) HeaderEncodingLength() uint64 {
	return 0
}

func (FilepackEncoder) NodeEncodingLength(node model.Node) uint64 {
	return node.Size()
}
