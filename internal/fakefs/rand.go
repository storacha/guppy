package fakefs

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
)

const bigPrime = 136279841

// Rand is a simple immutable pseudo-random number generator. For a given
// instance of [Rand], [rand.Uint64] will always return the same number. To get
// a new instance of [Rand] that will return a different number, advance the
// state using [Rand.Next] or [Rand.NextString], which will return a new [Rand].
type Rand struct {
	rng rand.PCG
}

var _ rand.Source = Rand{}

func NewRand(seed uint64) Rand {
	return Rand{rng: *rand.NewPCG(seed, 0)}
}

// Uint64 returns a pseudo-random uint64. For a given instance of [Rand], this
// will always return the same number.
func (r Rand) Uint64() uint64 {
	return r.rng.Uint64()
}

// Next advances the state of the random number based on the given integer. For
// a given instance of [Rand] and integer, this will always return the same new
// instance of [Rand].
func (r Rand) Next(i uint64) Rand {
	seed := r.rng.Uint64()
	return Rand{rng: *rand.NewPCG(seed, i)}
}

// NextString advances the state of the random number based on the given string.
// For a given instance of [Rand] and string, this will always return the same
// new instance of [Rand].
func (r Rand) NextString(s string) Rand {
	hash := md5.Sum([]byte(s))
	return r.Next(binary.BigEndian.Uint64(hash[:8]))
}

// Pick returns a pseudo-random element from the given list of elements. The
// element picked is based on the given index. For a given instance of [Rand],
// list of elements, and index, this will always return the same element.
//
// Pick simulates picking element `i` from a shuffled version of `elements`. The
// index must be less than the length of the list of elements. Each possible
// index will pick a different element.
func Pick[T any](r Rand, elements []T, i int) T {
	if len(elements) == 0 {
		panic("cannot pick from empty list")
	}

	if i >= len(elements) {
		panic(fmt.Sprintf("cannot pick item %d from list of size %d", i, len(elements)))
	}

	rng := r.rng
	index := ((bigPrime * i) + rand.New(&rng).Int()) % len(elements)
	return elements[index]
}
