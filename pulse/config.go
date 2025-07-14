package pulse

import (
	"errors"
	"log/slog"
	"time"
)

type Config struct {
	Logger       *slog.Logger
	StuckTimeout time.Duration
}

func (c *Config) Validate() error {
	if c.StuckTimeout <= 0 {
		return errors.New("stuckTimeout must be greater than 0")
	}

	return nil
}
