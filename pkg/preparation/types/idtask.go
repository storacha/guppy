package types

import (
	"github.com/storacha/guppy/pkg/preparation/internal/worker"
	"github.com/storacha/guppy/pkg/preparation/types/id"
)

// IDTask represents a worker task that can be identified and deduplicated by an
// [id.ID].
type IDTask struct {
	ID  id.ID
	Run worker.Task
}
