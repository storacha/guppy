package bubbleup

import (
	"math/rand/v2"
	"time"

	"github.com/charmbracelet/bubbles/spinner"
)

const frameCount = 20
const maxRunLength = 4

// brailleDots represents the 8 dots in a braille character. The names of
// braille characters name the dots 1-8 (as in "dots-135" for ⠕), so dot n is
// `dots[n-1]`. Note that braille dots are in an unusual order for historical
// reasons:
//
//	1 4
//	2 5
//	3 6
//	7 8
type brailleDots [8]bool

// brailleRune returns a braille rune with the given dots raised.
func brailleRune(dots brailleDots) rune {
	n := 0
	if dots[0] {
		n |= 1
	}
	if dots[1] {
		n |= 2
	}
	if dots[2] {
		n |= 4
	}
	if dots[3] {
		n |= 8
	}
	if dots[4] {
		n |= 16
	}
	if dots[5] {
		n |= 32
	}
	if dots[6] {
		n |= 64
	}
	if dots[7] {
		n |= 128
	}

	return rune(0x2800 + n)
}

// dotSequence returns a sequence of booleans consisting of `runCount` runs of
// `true` and `false`, each run having a random length between 1 and
// `maxRunLength`.
func dotSequence(rng *rand.Rand, runCount, maxRunLength int) []bool {
	dotSequence := make([]bool, 0, runCount*maxRunLength)
	for i := range runCount {
		n := rng.IntN(maxRunLength) + 1
		on := i%2 == 0
		for range n {
			dotSequence = append(dotSequence, on)
		}
	}
	return dotSequence
}

// Spinner returns a spinner that uses braille characters to create an
// impression of upwards movement.
func Spinner(rng *rand.Rand) spinner.Spinner {
	dotsL := dotSequence(rng, frameCount, maxRunLength)
	dotsR := dotSequence(rng, frameCount, maxRunLength)

	frames := make([]string, 0, 40)
	for i := range frameCount {
		var dots brailleDots

		dots[0] = dotsL[(i+0)%frameCount]
		dots[1] = dotsL[(i+1)%frameCount]
		dots[2] = dotsL[(i+2)%frameCount]
		dots[3] = dotsR[(i+0)%frameCount]
		dots[4] = dotsR[(i+1)%frameCount]
		dots[5] = dotsR[(i+2)%frameCount]
		dots[6] = dotsL[(i+3)%frameCount]
		dots[7] = dotsR[(i+3)%frameCount]

		frames = append(frames, string(brailleRune(dots)))
	}

	return spinner.Spinner{
		Frames: frames,
		FPS:    time.Second / 20,
	}
}
