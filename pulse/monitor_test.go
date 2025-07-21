package pulse

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewHealthChecker(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		config  Config
		client  BrokerClient
		wantErr bool
	}{
		{
			name: "valid config and client",
			config: Config{
				Logger:       slog.Default(),
				StuckTimeout: time.Minute,
			},
			client:  &mockBrokerClient{},
			wantErr: false,
		},
		{
			name: "valid config with nil logger",
			config: Config{
				Logger:       nil,
				StuckTimeout: time.Minute,
			},
			client:  &mockBrokerClient{},
			wantErr: false,
		},
		{
			name: "invalid config - zero timeout",
			config: Config{
				Logger:       slog.Default(),
				StuckTimeout: 0,
			},
			client:  &mockBrokerClient{},
			wantErr: true,
		},
		{
			name: "nil client",
			config: Config{
				Logger:       slog.Default(),
				StuckTimeout: time.Minute,
			},
			client:  nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			hc, err := NewHealthChecker(tt.config, tt.client)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.NotNil(t, hc)
		})
	}
}

func newTestHealthChecker(config Config, client BrokerClient, clock Clock) *HealthChecker {
	hc, err := NewHealthChecker(config, client)
	if err != nil {
		panic(err)
	}

	hc.clock = clock

	return hc
}

func TestHealthChecker_Release(t *testing.T) {
	t.Parallel()

	config := Config{
		Logger:       slog.Default(),
		StuckTimeout: time.Minute,
	}
	client := &mockBrokerClient{}

	hc, err := NewHealthChecker(config, client)
	if err != nil {
		t.Fatalf("NewHealthChecker() error = %v", err)
	}

	msg := &mockMessage{
		topic:     "test-topic",
		partition: 0,
		offset:    100,
	}

	ctx := context.Background()
	hc.Track(ctx, msg)

	// verify message exists
	offsets := hc.tracker.currentOffsets()
	if _, exists := offsets["test-topic"][0]; !exists {
		t.Error("Message should exist before release")
	}

	hc.Release(ctx, "test-topic", 0)

	// verify message was released
	offsets = hc.tracker.currentOffsets()
	if _, exists := offsets["test-topic"][0]; exists {
		t.Error("Message should not exist after release")
	}
}

func TestHealthChecker_Healthy_NoTrackedMessages(t *testing.T) {
	t.Parallel()

	config := Config{
		Logger:       slog.Default(),
		StuckTimeout: time.Minute,
	}
	client := &mockBrokerClient{}

	hc, err := NewHealthChecker(config, client)
	if err != nil {
		t.Fatalf("NewHealthChecker() error = %v", err)
	}

	ctx := context.Background()

	healthy, err := hc.Healthy(ctx)
	if err != nil {
		t.Errorf("Healthy() unexpected error = %v", err)
	}

	if !healthy {
		t.Error("Healthy() should return true when no messages are tracked")
	}
}

func TestHealthChecker_Healthy_RecentMessages(t *testing.T) {
	t.Parallel()

	config := Config{
		Logger:       slog.Default(),
		StuckTimeout: time.Minute,
	}
	client := &mockBrokerClient{}

	hc, err := NewHealthChecker(config, client)
	if err != nil {
		t.Fatalf("NewHealthChecker() error = %v", err)
	}

	msg := &mockMessage{
		topic:     "test-topic",
		partition: 0,
		offset:    100,
	}

	ctx := context.Background()
	hc.Track(ctx, msg)

	healthy, err := hc.Healthy(ctx)
	if err != nil {
		t.Errorf("Healthy() unexpected error = %v", err)
	}

	if !healthy {
		t.Error("Healthy() should return true for recently tracked messages")
	}
}

func TestHealthChecker_Healthy_StuckConsumer(t *testing.T) {
	t.Parallel()

	config := Config{
		Logger:       slog.Default(),
		StuckTimeout: 100 * time.Millisecond,
	}

	client := &mockBrokerClient{
		offsets: map[string]map[int32]int64{
			"test-topic": {
				0: 200, // latest offset is 200, but consumer is at 100
			},
		},
	}

	clock := &mockClock{currentTime: time.Now()}
	hc := newTestHealthChecker(config, client, clock)

	// add old message to tracker
	oldMessage := &mockMessage{
		topic:     "test-topic",
		partition: 0,
		offset:    100,
	}

	hc.Track(context.Background(), oldMessage)

	// advance time to make the message stale
	clock.Add(config.StuckTimeout + time.Millisecond)

	ctx := context.Background()

	healthy, err := hc.Healthy(ctx)
	assert.NoError(t, err)
	assert.False(t, healthy, "Healthy() should return false for stuck consumer")
}

func TestHealthChecker_Healthy_IdleConsumer(t *testing.T) {
	t.Parallel()

	config := Config{
		Logger:       slog.Default(),
		StuckTimeout: 100 * time.Millisecond,
	}

	// mock broker where consumer is caught up (latest offset = consumer offset)
	client := &mockBrokerClient{
		offsets: map[string]map[int32]int64{
			"test-topic": {
				0: 100, // Latest offset is 100, consumer is at 100 (caught up)
			},
		},
	}

	clock := &mockClock{currentTime: time.Now()}
	hc := newTestHealthChecker(config, client, clock)

	// add message to tracker with old timestamp
	oldMessage := &mockMessage{
		topic:     "test-topic",
		partition: 0,
		offset:    100,
	}

	hc.Track(context.Background(), oldMessage)

	// advance time to make the message stale
	clock.Add(config.StuckTimeout + time.Millisecond)

	ctx := context.Background()

	healthy, err := hc.Healthy(ctx)
	assert.NoError(t, err)
	assert.True(t, healthy, "Healthy() should return true for idle but caught up consumer")
}

func TestHealthChecker_Healthy_BrokerError_FailByDefault(t *testing.T) {
	t.Parallel()

	config := Config{
		Logger:       slog.Default(),
		StuckTimeout: 100 * time.Millisecond,
		// IgnoreBrokerErrors defaults to false (fail on errors)
	}

	// broker that returns error
	client := &mockBrokerClient{
		err: errors.New("broker connection failed"),
	}

	clock := &mockClock{currentTime: time.Now()}
	hc := newTestHealthChecker(config, client, clock)

	// old message to trigger broker call
	oldMessage := &mockMessage{
		topic:     "test-topic",
		partition: 0,
		offset:    100,
	}

	hc.Track(context.Background(), oldMessage)

	// advance time to make the message stale
	clock.Add(config.StuckTimeout + time.Millisecond)

	ctx := context.Background()

	healthy, err := hc.Healthy(ctx)
	assert.Error(t, err, "Healthy() should return error on broker failure by default")
	assert.False(t, healthy, "Healthy() should return false on broker errors by default")
}

func TestHealthChecker_Healthy_BrokerError_IgnoreWhenConfigured(t *testing.T) {
	t.Parallel()

	config := Config{
		Logger:             slog.Default(),
		StuckTimeout:       100 * time.Millisecond,
		IgnoreBrokerErrors: true, // Explicitly ignore broker errors
	}

	// broker that returns error
	client := &mockBrokerClient{
		err: errors.New("broker connection failed"),
	}

	clock := &mockClock{currentTime: time.Now()}
	hc := newTestHealthChecker(config, client, clock)

	// old message to trigger broker call
	oldMessage := &mockMessage{
		topic:     "test-topic",
		partition: 0,
		offset:    100,
	}

	hc.Track(context.Background(), oldMessage)

	// advance time to make the message stale
	clock.Add(config.StuckTimeout + time.Millisecond)

	ctx := context.Background()

	healthy, err := hc.Healthy(ctx)
	assert.NoError(t, err)
	assert.True(t, healthy, "Healthy() should return true when configured to ignore broker errors")
}

func TestHealthChecker_DefaultBrokerErrorBehavior(t *testing.T) {
	t.Parallel()

	// Test that IgnoreBrokerErrors defaults to false (fail on errors)
	config := Config{
		Logger:       slog.Default(),
		StuckTimeout: time.Minute,
		// IgnoreBrokerErrors not set - should default to false (fail on errors)
	}

	client := &mockBrokerClient{}

	hc, err := NewHealthChecker(config, client)
	if err != nil {
		t.Fatalf("NewHealthChecker() error = %v", err)
	}

	// Check that the internal field is set to false by default (fail on errors)
	if hc.ignoreBrokerErrors {
		t.Error("ignoreBrokerErrors should default to false (fail on errors)")
	}

	// Test explicit true setting (ignore errors)
	configIgnore := Config{
		Logger:             slog.Default(),
		StuckTimeout:       time.Minute,
		IgnoreBrokerErrors: true,
	}

	hcIgnore, err := NewHealthChecker(configIgnore, client)
	if err != nil {
		t.Fatalf("NewHealthChecker() error = %v", err)
	}

	if !hcIgnore.ignoreBrokerErrors {
		t.Error("ignoreBrokerErrors should be true when explicitly set to true")
	}

	// Test explicit false setting (fail on errors)
	configFail := Config{
		Logger:             slog.Default(),
		StuckTimeout:       time.Minute,
		IgnoreBrokerErrors: false,
	}

	hcFail, err := NewHealthChecker(configFail, client)
	if err != nil {
		t.Fatalf("NewHealthChecker() error = %v", err)
	}

	if hcFail.ignoreBrokerErrors {
		t.Error("ignoreBrokerErrors should be false when explicitly set to false")
	}
}

func TestHealthChecker_Healthy_MultiPartition_SingleUnhealthy(t *testing.T) {
	t.Parallel()

	config := Config{
		Logger:       slog.Default(),
		StuckTimeout: 100 * time.Millisecond,
	}

	// mock broker with mixed partition states
	// - topic-a/partition-0: consumer caught up (offset 100, latest 101)
	// - topic-a/partition-1: consumer behind (offset 150, latest 200)
	// - topic-b/partition-0: consumer caught up (offset 100, latest 101)
	client := &mockBrokerClient{
		offsets: map[string]map[int32]int64{
			"topic-a": {
				0: 101, // partition 0 caught up
				1: 200, // partition 1 has new messages available
			},
			"topic-b": {
				0: 101, // partition 0 caught up
			},
		},
	}

	clock := &mockClock{currentTime: time.Now()}
	hc := newTestHealthChecker(config, client, clock)

	healthyMsg1 := &mockMessage{
		topic:     "topic-a",
		partition: 0,
		offset:    100,
	}
	staleMsg := &mockMessage{
		topic:     "topic-a",
		partition: 1,
		offset:    150,
	}
	healthyMsg2 := &mockMessage{
		topic:     "topic-b",
		partition: 0,
		offset:    100,
	}

	// track all messages
	hc.Track(context.Background(), healthyMsg1)
	hc.Track(context.Background(), staleMsg)
	hc.Track(context.Background(), healthyMsg2)

	// advance time to make the stale message trigger the check
	clock.Add(config.StuckTimeout + time.Millisecond)

	ctx := context.Background()

	healthy, err := hc.Healthy(ctx)
	assert.NoError(t, err)
	assert.False(t, healthy, "Healthy() should return false when any partition is stuck behind available messages")
}

type mockBrokerClient struct {
	offsets map[string]map[int32]int64
	err     error
}

func (m *mockBrokerClient) GetLatestOffset(_ context.Context, topic string, partition int32) (int64, error) {
	if m.err != nil {
		return 0, m.err
	}

	if topicOffsets, ok := m.offsets[topic]; ok {
		if offset, exists := topicOffsets[partition]; exists {
			return offset, nil
		}
	}

	return 0, errors.New("partition not found")
}
