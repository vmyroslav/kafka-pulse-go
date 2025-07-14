package pulse

import (
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestConfig_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		config  Config
		wantErr bool
	}{
		{
			name: "valid config",
			config: Config{
				Logger:       slog.Default(),
				StuckTimeout: time.Minute,
			},
			wantErr: false,
		},
		{
			name: "valid config with nil logger",
			config: Config{
				Logger:       nil,
				StuckTimeout: time.Second,
			},
			wantErr: false,
		},
		{
			name: "valid config with IgnoreBrokerErrors false",
			config: Config{
				Logger:             slog.Default(),
				StuckTimeout:       time.Minute,
				IgnoreBrokerErrors: false,
			},
			wantErr: false,
		},
		{
			name: "valid config with IgnoreBrokerErrors true",
			config: Config{
				Logger:             slog.Default(),
				StuckTimeout:       time.Minute,
				IgnoreBrokerErrors: true,
			},
			wantErr: false,
		},
		{
			name: "invalid config - zero timeout",
			config: Config{
				Logger:       slog.Default(),
				StuckTimeout: 0,
			},
			wantErr: true,
		},
		{
			name: "invalid config - negative timeout",
			config: Config{
				Logger:       slog.Default(),
				StuckTimeout: -time.Second,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.config.Validate()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
