package types

import (
	"fmt"
	"strings"

	"github.com/ipfs/go-cid"
)

type ErrEmpty struct {
	Field string
}

func (e ErrEmpty) Error() string {
	return fmt.Sprintf("%s cannot be empty", e.Field)
}

// ErrBadNode indicates that a node cannot be read or is otherwise invalid.
type ErrBadNode struct {
	cid cid.Cid
	err error
}

func NewErrBadNode(cid cid.Cid, err error) ErrBadNode {
	return ErrBadNode{cid: cid, err: err}
}

func (e ErrBadNode) Error() string {
	return fmt.Sprintf("bad node %s: %s", e.cid, e.err)
}

func (e ErrBadNode) Unwrap() error {
	return e.err
}

func (e ErrBadNode) CID() cid.Cid {
	return e.cid
}

// ErrBadNodes wraps multiple ErrBadNode errors from the same operation.
type ErrBadNodes struct {
	errs []ErrBadNode
}

func NewErrBadNodes(errs []ErrBadNode) ErrBadNodes {
	return ErrBadNodes{errs: errs}
}

func (e ErrBadNodes) Error() string {
	var messages []string
	for _, err := range e.errs {
		messages = append(messages, err.Error())
	}

	return fmt.Sprintf("bad nodes:\n%s", strings.Join(messages, "\n"))
}

func (e ErrBadNodes) Unwrap() []error {
	errs := make([]error, len(e.errs))
	for i, err := range e.errs {
		errs[i] = err
	}
	return errs
}

func (e ErrBadNodes) Errs() []ErrBadNode {
	return e.errs
}
