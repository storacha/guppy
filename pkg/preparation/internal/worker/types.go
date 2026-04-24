package worker

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// TaskError is the error type returned by a [Task]. A TaskError may carry a
// fatal error (which causes the worker to cancel dispatch of remaining tasks)
// and/or a set of non-fatal errors (which are collected and reported after
// all tasks complete).
type TaskError interface {
	error
	NonFatalErrors() []error
	FatalError() error
	IsFatal() bool
}

// NewFatalError returns a [TaskError] carrying the given error as a fatal
// error. If err is nil, it returns nil.
func NewFatalError(err error) TaskError {
	if err == nil {
		return nil
	}
	return &taskError{fatalError: err}
}

// NewNonFatalError returns a [TaskError] carrying the given errors as
// non-fatal errors. Any nil entries are dropped; if the result would be empty,
// it returns nil.
func NewNonFatalError(errs ...error) TaskError {
	var filtered []error
	for _, err := range errs {
		if err != nil {
			filtered = append(filtered, err)
		}
	}
	if len(filtered) == 0 {
		return nil
	}
	return &taskError{nonFatalErrors: filtered}
}

type taskError struct {
	nonFatalErrors []error
	fatalError     error
}

// Task is a worker task. It returns a [TaskError] to signal the result:
//   - return nil for success,
//   - return [NewFatalError] (or any TaskError whose IsFatal is true) to abort
//     further dispatch,
//   - return [NewNonFatalError] to report errors that should be collected but
//     not abort dispatch.
type Task func(context.Context) TaskError

func (e *taskError) NonFatalErrors() []error {
	return e.nonFatalErrors
}

func (e *taskError) FatalError() error {
	return e.fatalError
}

func (e *taskError) IsFatal() bool {
	return e.fatalError != nil
}

func (e *taskError) Error() string {
	nonFatalString := ""
	if len(e.nonFatalErrors) > 0 {
		nonFatalString = "non-fatal errors:"
	}
	for _, err := range e.nonFatalErrors {
		nonFatalString += fmt.Sprintf("\n- %s", err)
	}

	fatalString := ""
	if e.fatalError != nil {
		fatalString = fmt.Sprintf("fatal error: %s", e.fatalError)
	}

	return fmt.Sprintf("worker encountered %s", strings.Join([]string{nonFatalString, fatalString}, "\n"))
}

func (e *taskError) Unwrap() []error {
	errs := make([]error, len(e.nonFatalErrors))
	copy(errs, e.nonFatalErrors)
	if e.fatalError != nil {
		errs = append(errs, e.fatalError)
	}
	return errs
}

// add folds another [TaskError] into the receiver in place.
func (e *taskError) add(other TaskError) {
	if other == nil {
		return
	}
	e.nonFatalErrors = append(e.nonFatalErrors, other.NonFatalErrors()...)
	if other.FatalError() != nil {
		e.fatalError = errors.Join(e.fatalError, other.FatalError())
	}
}

// isEmpty reports whether the taskError carries neither a fatal error nor any
// non-fatal errors.
func (e *taskError) isEmpty() bool {
	return e.fatalError == nil && len(e.nonFatalErrors) == 0
}

// Join combines two [TaskError]s. Non-fatal errors are concatenated; fatal
// errors are joined with [errors.Join]. Either operand may be nil.
func Join(a TaskError, b TaskError) TaskError {
	switch {
	case a == nil && b == nil:
		return nil
	case a == nil:
		return b
	case b == nil:
		return a
	}
	joined := &taskError{
		nonFatalErrors: append(a.NonFatalErrors(), b.NonFatalErrors()...),
		fatalError:     errors.Join(a.FatalError(), b.FatalError()),
	}
	if joined.isEmpty() {
		return nil
	}
	return joined
}
