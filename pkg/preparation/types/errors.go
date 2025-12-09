package types

import (
	"fmt"
	"strings"

	"github.com/ipfs/go-cid"
	"github.com/storacha/guppy/pkg/preparation/types/id"
)

type EmptyError struct {
	Field string
}

func (e EmptyError) Error() string {
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

// BadNodeError indicates that a node cannot be read or is otherwise invalid.
type BadNodeError struct {
	cid cid.Cid
	err error
}

func NewBadNodeError(cid cid.Cid, err error) BadNodeError {
	return BadNodeError{cid: cid, err: err}
}

func (e BadNodeError) Error() string {
	return fmt.Sprintf("bad node %s: %s", e.cid, e.err)
}

func (e BadNodeError) Unwrap() error {
	return e.err
}

func (e BadNodeError) CID() cid.Cid {
	return e.cid
}

// BadNodesError wraps multiple BadNodeError errors from the same operation.
type BadNodesError struct {
	errs     []BadNodeError
	shardID  id.ShardID
	goodCIDs *cid.Set
}

func NewBadNodesError(errs []BadNodeError, shardID id.ShardID, goodCIDs *cid.Set) error {
	return RetriableError{err: BadNodesError{errs: errs, shardID: shardID, goodCIDs: goodCIDs}}
}

func (e BadNodesError) Error() string {
	var messages []string
	for _, err := range e.errs {
		messages = append(messages, err.Error())
	}

	return fmt.Sprintf("bad nodes:\n%s", strings.Join(messages, "\n"))
}

func (e BadNodesError) ShardID() id.ShardID {
	return e.shardID
}

func (e BadNodesError) GoodCIDs() *cid.Set {
	return e.goodCIDs
}

func (e BadNodesError) Unwrap() []error {
	errs := make([]error, len(e.errs))
	for i, err := range e.errs {
		errs[i] = err
	}
	return errs
}

func (e BadNodesError) Errs() []BadNodeError {
	return e.errs
}

// BadFSEntryError indicates that a DAG scan cannot be read or is otherwise invalid.
type BadFSEntryError struct {
	fsEntryID id.FSEntryID
	err       error
}

func NewBadFSEntryError(fsEntryID id.FSEntryID, err error) error {
	return RetriableError{err: BadFSEntryError{fsEntryID: fsEntryID, err: err}}
}

func (e BadFSEntryError) Error() string {
	return fmt.Sprintf("bad DAG scan %s: %s", e.fsEntryID, e.err)
}

func (e BadFSEntryError) Unwrap() error {
	return e.err
}

func (e BadFSEntryError) FsEntryID() id.FSEntryID {
	return e.fsEntryID
}

type BadFSEntriesError struct {
	errs []BadFSEntryError
}

func NewBadFSEntriesError(errs []BadFSEntryError) error {
	return RetriableError{err: BadFSEntriesError{errs: errs}}
}

func (e BadFSEntriesError) Errs() []BadFSEntryError {
	return e.errs
}

func (e BadFSEntriesError) Error() string {
	var messages []string
	for _, err := range e.errs {
		messages = append(messages, err.Error())
	}

	return fmt.Sprintf("bad DAG scans:\n%s", strings.Join(messages, "\n"))
}

func (e BadFSEntriesError) Unwrap() []error {
	errs := make([]error, len(e.errs))
	for i, err := range e.errs {
		errs[i] = err
	}
	return errs
}

type ShardUploadError struct {
	shardID id.ShardID
	err     error
}

func NewShardUploadError(shardID id.ShardID, err error) ShardUploadError {
	return ShardUploadError{shardID: shardID, err: err}
}
func (e ShardUploadError) Error() string {
	return fmt.Sprintf("shard upload %s failed: %s", e.shardID, e.err)
}
func (e ShardUploadError) Unwrap() error {
	return e.err
}
func (e ShardUploadError) ShardID() id.ShardID {
	return e.shardID
}

type ShardUploadErrors struct {
	errs []ShardUploadError
}

func NewShardUploadErrors(errs []ShardUploadError) error {
	return RetriableError{err: ShardUploadErrors{errs: errs}}
}

func (e ShardUploadErrors) Errs() []ShardUploadError {
	return e.errs
}

func (e ShardUploadErrors) Error() string {
	var messages []string
	for _, err := range e.errs {
		messages = append(messages, err.Error())
	}

	return fmt.Sprintf("shard uploads failed:\n%s", strings.Join(messages, "\n"))
}

func (e ShardUploadErrors) Unwrap() []error {
	errs := make([]error, len(e.errs))
	for i, err := range e.errs {
		errs[i] = err
	}
	return errs
}
