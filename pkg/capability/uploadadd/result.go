package uploadadd

import (
	_ "embed"

	"github.com/ipld/go-ipld-prime"
)

//go:embed result.ipldsch
var ResultSchema []byte

type Success struct {
	Root   ipld.Link
	Shards []ipld.Link
}

type Failure struct {
	Name    *string
	Message string
	Stack   *string
}
