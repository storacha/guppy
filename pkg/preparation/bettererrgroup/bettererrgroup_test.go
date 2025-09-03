package bettererrgroup_test

import (
	"regexp"
	"testing"

	"github.com/storacha/guppy/pkg/preparation/bettererrgroup"
	"github.com/stretchr/testify/require"
)

func doSomePanicking() error {
	panic("test panic")
}

func TestGroup(t *testing.T) {
	eg, ctx := bettererrgroup.WithContext(t.Context())
	eg.Go(doSomePanicking)
	err := eg.Wait()
	var pErr bettererrgroup.PanicError
	require.ErrorAs(t, err, &pErr)
	require.Equal(t, "test panic", pErr.Recovered())
	require.Regexp(t, regexp.MustCompile(`bettererrgroup_test\.doSomePanicking\(\)`), pErr.Stack())
	require.Contains(t, pErr.Error(), "panic: test panic")
	require.Contains(t, pErr.Error(), pErr.Stack())
	if ctx.Err() == nil {
		t.Fatal("expected context to be canceled, but it is not")
	}
}
