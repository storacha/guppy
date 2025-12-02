package types

import (
	"fmt"
	"strings"

	"github.com/ipfs/go-cid"
	"github.com/storacha/guppy/pkg/preparation/types/id"
)

type ErrEmpty struct {
	Field string
}

func (e ErrEmpty) Error() string {
	return fmt.Sprintf("%s cannot be empty", e.Field)
}

type RetriableError struct {
	err error
}

func NewRetriableError(err error) RetriableError {
	return RetriableError{err: err}
}

func (e RetriableError) Error() string {
	return fmt.Sprintf("retriable error: %s", e.err)
}

func (e RetriableError) Unwrap() error {
	return e.err
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
	errs     []ErrBadNode
	shardID  id.ShardID
	goodCIDs *cid.Set
}

func NewErrBadNodes(errs []ErrBadNode, shardID id.ShardID, goodCIDs *cid.Set) error {
	return RetriableError{err: ErrBadNodes{errs: errs, shardID: shardID, goodCIDs: goodCIDs}}
}

func (e ErrBadNodes) Error() string {
	var messages []string
	for _, err := range e.errs {
		messages = append(messages, err.Error())
	}

	return fmt.Sprintf("bad nodes:\n%s", strings.Join(messages, "\n"))
}

func (e ErrBadNodes) ShardID() id.ShardID {
	return e.shardID
}

func (e ErrBadNodes) GoodCIDs() *cid.Set {
	return e.goodCIDs
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

// ErrBadFSEntry indicates that a DAG scan cannot be read or is otherwise invalid.
type ErrBadFSEntry struct {
	fsEntryID id.FSEntryID
	err       error
}

func NewErrBadFSEntry(fsEntryID id.FSEntryID, err error) error {
	return RetriableError{err: ErrBadFSEntry{fsEntryID: fsEntryID, err: err}}
}

func (e ErrBadFSEntry) Error() string {
	return fmt.Sprintf("bad DAG scan %s: %s", e.fsEntryID, e.err)
}

func (e ErrBadFSEntry) Unwrap() error {
	return e.err
}

func (e ErrBadFSEntry) FsEntryID() id.FSEntryID {
	return e.fsEntryID
}

type ErrBadFSEntries struct {
	errs []ErrBadFSEntry
}

func NewErrBadFSEntries(errs []ErrBadFSEntry) error {
	return RetriableError{err: ErrBadFSEntries{errs: errs}}
}

func (e ErrBadFSEntries) Errs() []ErrBadFSEntry {
	return e.errs
}

func (e ErrBadFSEntries) Error() string {
	var messages []string
	for _, err := range e.errs {
		messages = append(messages, err.Error())
	}

	return fmt.Sprintf("bad DAG scans:\n%s", strings.Join(messages, "\n"))
}

func (e ErrBadFSEntries) Unwrap() []error {
	errs := make([]error, len(e.errs))
	for i, err := range e.errs {
		errs[i] = err
	}
	return errs
}
