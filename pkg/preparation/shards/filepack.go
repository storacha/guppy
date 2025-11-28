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

func (f *FilepackEncoder) WriteHeader(ctx context.Context, w io.Writer) error {
	// Filepack has no header to write.
	return nil
}

func (f *FilepackEncoder) WriteNode(ctx context.Context, node model.Node, data []byte, w io.Writer) error {
	_, err := w.Write(data)
	if err != nil {
		return fmt.Errorf("writing filepack node data: %w", err)
	}
	return nil
}

func (FilepackEncoder) HeaderEncodingLength() uint64 {
	return 0
}

func (FilepackEncoder) NodeEncodingLength(node model.Node) uint64 {
	return node.Size()
}
