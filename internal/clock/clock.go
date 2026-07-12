// Package clock provides an injectable wall clock so callers stay testable and
// [time.Now] stays confined to one place.
package clock

import "time"

// Clock reads the current wall-clock time.
type Clock interface {
	Now() time.Time
}

// System returns a Clock backed by the process wall clock.
func System() Clock {
	return systemClock{}
}

type systemClock struct{}

func (systemClock) Now() time.Time {
	return time.Now()
}
