package pulse

import "time"

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
