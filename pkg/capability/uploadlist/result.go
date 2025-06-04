package uploadlist

import (
	_ "embed"

	"github.com/ipld/go-ipld-prime"
)

//go:embed result.ipldsch
var ResultSchema []byte

type Success struct {
	Results []Item
	Before  *string
	After   *string
	Cursor  *string
	Size    uint64
}

type Item struct {
	Root       ipld.Link
	Shards     []ipld.Link
	InsertedAt string
	UpdatedAt  string
}

type Failure struct {
	Name    *string
	Message string
	Stack   *string
}
