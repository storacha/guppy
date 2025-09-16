package fakefs_test

import (
	"testing"

	"github.com/storacha/guppy/internal/fakefs"
	"github.com/stretchr/testify/require"
)

func TestRand(t *testing.T) {
	t.Run("should always generate same number", func(t *testing.T) {
		r := fakefs.NewRand(0)
		require.Equal(t, r.Uint64(), r.Uint64())
	})

	t.Run("can pick from a list", func(t *testing.T) {
		r := fakefs.NewRand(0)

		elements := make([]int, 0, 10)
		for i := range 10 {
			elements = append(elements, i)
		}

		selected := make([]int, 0, 10)
		for i := range 10 {
			selected = append(selected, fakefs.Pick(r, elements, i))
		}

		require.ElementsMatch(t, elements, selected)
	})

	t.Run("should advance to the same state for the same uint64", func(t *testing.T) {
		r := fakefs.NewRand(0)
		require.Equal(t, r.Next(1).Uint64(), r.Next(1).Uint64())
		require.NotEqual(t, r.Next(1).Uint64(), r.Next(2).Uint64())
	})

	t.Run("should advance to the same state for the same string", func(t *testing.T) {
		r := fakefs.NewRand(0)
		require.Equal(t, r.NextString("abc").Uint64(), r.NextString("abc").Uint64())
		require.NotEqual(t, r.NextString("abc").Uint64(), r.NextString("def").Uint64())
	})
}
