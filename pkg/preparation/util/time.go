package util

import "time"

var nowFunc func() time.Time

func init() {
	ResetNowFunc()
}

// Now returns the current time, using an implementation that can be replaced
// using [SetNowFunc]. This allows the current time to be stubbed in tests.
func Now() time.Time {
	return nowFunc()
}

// SetNowFunc sets the implementation function which returns the current time.
// This is a global change, and intended for testing purposes only.
func SetNowFunc(f func() time.Time) {
	if f == nil {
		panic("SetNowFunc cannot be set to nil")
	}
	nowFunc = f
}

func ResetNowFunc() {
	nowFunc = func() time.Time {
		return time.Now()
	}
}
