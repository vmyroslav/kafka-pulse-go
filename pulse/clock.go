package pulse

import "time"

// Clock defines an interface for getting the current time, allowing for mocking in tests.
type Clock interface {
	Now() time.Time
}

// realClock implements the Clock interface using the real time.
type realClock struct{}

// Now returns the current time.
func (c *realClock) Now() time.Time {
	return time.Now()
}
