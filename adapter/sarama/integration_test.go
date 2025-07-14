package sarama

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmyroslav/kafka-pulse-go/pulse"
)

// TestSaramaIntegration_SinglePartitionLag demonstrates integration test with mock Sarama client
func TestSaramaIntegration_SinglePartitionLag(t *testing.T) {
	t.Run("AdapterIntegration", func(t *testing.T) {
		mockClient := &mockSaramaClient{
			offsets: map[string]map[int32]int64{
				"orders": {
					0: 100,
					1: 200,
				},
				"payments": {
					0: 150,
				},
			},
		}

		adapter := NewClientAdapter(mockClient)

		hc, err := pulse.NewHealthChecker(pulse.Config{
			Logger:       slog.Default(),
			StuckTimeout: 100 * time.Millisecond,
		}, adapter)
		require.NoError(t, err)

		ctx := context.Background()

		orderMsg := &Message{
			ConsumerMessage: &sarama.ConsumerMessage{
				Topic:     "orders",
				Partition: 0,
				Offset:    99, // behind latest (100)
			},
		}
		hc.Track(ctx, orderMsg)

		paymentMsg := &Message{
			ConsumerMessage: &sarama.ConsumerMessage{
				Topic:     "payments",
				Partition: 0,
				Offset:    149, // behind latest (150)
			},
		}
		hc.Track(ctx, paymentMsg)

		// initially healthy
		healthy, err := hc.Healthy(ctx)
		assert.NoError(t, err)
		assert.True(t, healthy, "Should be healthy initially")

		// wait for timeout
		time.Sleep(150 * time.Millisecond)

		// update mock to show new messages available
		mockClient.offsets["orders"][0] = 110   // new messages available
		mockClient.offsets["payments"][0] = 150 // no new messages

		healthy, err = hc.Healthy(ctx)
		assert.NoError(t, err)
		assert.False(t, healthy, "Should be unhealthy due to stuck orders partition")
	})
}

// TestSaramaIntegration_MultiPartitionLag tests multi-partition lag scenarios
func TestSaramaIntegration_MultiPartitionLag(t *testing.T) {
	t.Run("MixedPartitionStates", func(t *testing.T) {
		// create mock client with multiple partitions in different states
		mockClient := &mockSaramaClient{
			offsets: map[string]map[int32]int64{
				"high-throughput": {
					0: 200, // partition 0: consumer caught up
					1: 250, // partition 1: consumer behind (stuck)
					2: 200, // partition 2: consumer caught up (idle)
					3: 200, // partition 3: not tracked
					4: 200, // partition 4: not tracked
				},
			},
		}

		adapter := NewClientAdapter(mockClient)
		hc, err := pulse.NewHealthChecker(pulse.Config{
			Logger:       slog.Default(),
			StuckTimeout: 100 * time.Millisecond,
		}, adapter)
		require.NoError(t, err)

		ctx := context.Background()

		// partition 0: healthy (recent)
		healthyMsg := &Message{
			ConsumerMessage: &sarama.ConsumerMessage{
				Topic:     "high-throughput",
				Partition: 0,
				Offset:    199,
			},
		}
		hc.Track(ctx, healthyMsg)

		// partition 1: stuck behind (will become stale after timeout)
		stuckMsg := &Message{
			ConsumerMessage: &sarama.ConsumerMessage{
				Topic:     "high-throughput",
				Partition: 1,
				Offset:    150,
			},
		}
		hc.Track(ctx, stuckMsg)

		// partition 2: idle but caught up (will become stale after timeout)
		idleMsg := &Message{
			ConsumerMessage: &sarama.ConsumerMessage{
				Topic:     "high-throughput",
				Partition: 2,
				Offset:    199,
			},
		}
		hc.Track(ctx, idleMsg)

		// wait for timeout to make partitions 1 and 2 stale
		time.Sleep(150 * time.Millisecond)

		healthy, err := hc.Healthy(ctx)
		assert.NoError(t, err)
		assert.False(t, healthy, "Should be unhealthy due to stuck partition 1")
	})
}

// mockSaramaClient is a minimal mock implementation of sarama.Client for testing
type mockSaramaClient struct {
	offsets map[string]map[int32]int64
	err     error
}

func (m *mockSaramaClient) GetOffset(topic string, partition int32, time int64) (int64, error) {
	if m.err != nil {
		return 0, m.err
	}

	if topicOffsets, ok := m.offsets[topic]; ok {
		if offset, exists := topicOffsets[partition]; exists {
			return offset, nil
		}
	}

	return 0, sarama.ErrUnknownTopicOrPartition
}

// stubs
func (m *mockSaramaClient) Config() *sarama.Config                                  { return nil }
func (m *mockSaramaClient) Brokers() []*sarama.Broker                               { return nil }
func (m *mockSaramaClient) Broker(int32) (*sarama.Broker, error)                    { return nil, nil }
func (m *mockSaramaClient) Topics() ([]string, error)                               { return nil, nil }
func (m *mockSaramaClient) Partitions(string) ([]int32, error)                      { return nil, nil }
func (m *mockSaramaClient) WritablePartitions(string) ([]int32, error)              { return nil, nil }
func (m *mockSaramaClient) Leader(string, int32) (*sarama.Broker, error)            { return nil, nil }
func (m *mockSaramaClient) Replicas(string, int32) ([]int32, error)                 { return nil, nil }
func (m *mockSaramaClient) InSyncReplicas(string, int32) ([]int32, error)           { return nil, nil }
func (m *mockSaramaClient) OfflineReplicas(string, int32) ([]int32, error)          { return nil, nil }
func (m *mockSaramaClient) RefreshBrokers([]string) error                           { return nil }
func (m *mockSaramaClient) RefreshMetadata(...string) error                         { return nil }
func (m *mockSaramaClient) GetMetadata() (*sarama.MetadataResponse, error)          { return nil, nil }
func (m *mockSaramaClient) Coordinator(string) (*sarama.Broker, error)              { return nil, nil }
func (m *mockSaramaClient) RefreshCoordinator(string) error                         { return nil }
func (m *mockSaramaClient) InitProducerID() (*sarama.InitProducerIDResponse, error) { return nil, nil }
func (m *mockSaramaClient) LeastLoadedBroker() *sarama.Broker                       { return nil }
func (m *mockSaramaClient) Close() error                                            { return nil }
func (m *mockSaramaClient) Closed() bool                                            { return false }
func (m *mockSaramaClient) Controller() (*sarama.Broker, error)                     { return nil, nil }
func (m *mockSaramaClient) RefreshController() (*sarama.Broker, error)              { return nil, nil }
func (m *mockSaramaClient) LeaderAndEpoch(string, int32) (*sarama.Broker, int32, error) {
	return nil, 0, nil
}
func (m *mockSaramaClient) TransactionCoordinator(string) (*sarama.Broker, error) { return nil, nil }
func (m *mockSaramaClient) RefreshTransactionCoordinator(string) error            { return nil }
func (m *mockSaramaClient) PartitionNotReadable(string, int32) bool               { return false }
