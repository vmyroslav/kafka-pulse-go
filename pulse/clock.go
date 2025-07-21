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

// mockClock implements the Clock interface for testing purposes.
type mockClock struct {
	currentTime time.Time
}

// Now returns the current time for the mock clock.
func (c *mockClock) Now() time.Time {
	return c.currentTime
}

// Set sets the current time for the mock clock.
func (c *mockClock) Set(t time.Time) {
	c.currentTime = t
}

// Add advances the current time for the mock clock by the specified duration.
func (c *mockClock) Add(d time.Duration) {
	c.currentTime = c.currentTime.Add(d)
}
