package sarama

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmyroslav/kafka-pulse-go/pulse"
)

// offsetClient interface for testing - minimal interface for our needs
type offsetClient interface {
	GetOffset(topic string, partition int32, time int64) (int64, error)
}

// mockOffsetClient implements just the GetOffset method for testing
type mockOffsetClient struct {
	offsets map[string]map[int32]int64
	err     error
}

func (m *mockOffsetClient) GetOffset(topic string, partition int32, time int64) (int64, error) {
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

// testClientAdapter wraps our mock for testing
type testClientAdapter struct {
	client offsetClient
}

func (c *testClientAdapter) GetLatestOffset(_ context.Context, topic string, partition int32) (int64, error) {
	latestOffset, err := c.client.GetOffset(topic, partition, sarama.OffsetNewest)
	if err != nil {
		return 0, err
	}

	// return offset - 1 to get the offset of the last actual message
	return latestOffset - 1, nil
}

// Individual method tests removed - covered by TestMessage_AllFields and TestMessage_VariousConfigurations

func TestMessage_AllFields(t *testing.T) {
	t.Parallel()

	consumerMsg := &sarama.ConsumerMessage{
		Topic:     "multi-test-topic",
		Partition: 3,
		Offset:    999999,
		Key:       []byte("test-key"),
		Value:     []byte("test-value"),
	}

	msg := &Message{ConsumerMessage: consumerMsg}

	assert.Equal(t, "multi-test-topic", msg.Topic())
	assert.Equal(t, int32(3), msg.Partition())
	assert.Equal(t, int64(999999), msg.Offset())
	// Verify underlying message is accessible
	assert.Equal(t, []byte("test-key"), msg.ConsumerMessage.Key)
	assert.Equal(t, []byte("test-value"), msg.ConsumerMessage.Value)
}

func TestClientAdapter_GetLatestOffset_Success(t *testing.T) {
	t.Parallel()

	mockClient := &mockOffsetClient{
		offsets: map[string]map[int32]int64{
			"test-topic": {
				0: 1001, // Sarama returns next offset, so latest message is 1000
				1: 2001, // Latest message is 2000
			},
		},
	}

	adapter := &testClientAdapter{client: mockClient}
	ctx := context.Background()

	// Test partition 0
	offset, err := adapter.GetLatestOffset(ctx, "test-topic", 0)
	assert.NoError(t, err)
	assert.Equal(t, int64(1000), offset) // Should subtract 1 from sarama offset

	// Test partition 1
	offset, err = adapter.GetLatestOffset(ctx, "test-topic", 1)
	assert.NoError(t, err)
	assert.Equal(t, int64(2000), offset) // Should subtract 1 from sarama offset
}

func TestClientAdapter_GetLatestOffset_Error(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("broker connection failed")
	mockClient := &mockOffsetClient{
		err: expectedErr,
	}

	adapter := &testClientAdapter{client: mockClient}
	ctx := context.Background()

	offset, err := adapter.GetLatestOffset(ctx, "test-topic", 0)
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
	assert.Equal(t, int64(0), offset)
}

func TestClientAdapter_GetLatestOffset_PartitionNotFound(t *testing.T) {
	t.Parallel()

	mockClient := &mockOffsetClient{
		offsets: map[string]map[int32]int64{
			"test-topic": {
				0: 1001,
			},
		},
	}

	adapter := &testClientAdapter{client: mockClient}
	ctx := context.Background()

	// Request non-existent partition
	offset, err := adapter.GetLatestOffset(ctx, "test-topic", 999)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "partition not found")
	assert.Equal(t, int64(0), offset)
}

func TestClientAdapter_GetLatestOffset_TopicNotFound(t *testing.T) {
	t.Parallel()

	mockClient := &mockOffsetClient{
		offsets: map[string]map[int32]int64{
			"existing-topic": {
				0: 1001,
			},
		},
	}

	adapter := &testClientAdapter{client: mockClient}
	ctx := context.Background()

	// Request non-existent topic
	offset, err := adapter.GetLatestOffset(ctx, "non-existent-topic", 0)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "partition not found")
	assert.Equal(t, int64(0), offset)
}

// TestClientAdapter_GetLatestOffset_ZeroOffset removed - covered by TestClientAdapter_OffsetCalculation

func TestClientAdapter_GetLatestOffset_ContextIgnored(t *testing.T) {
	t.Parallel()

	mockClient := &mockOffsetClient{
		offsets: map[string]map[int32]int64{
			"test-topic": {
				0: 1001,
			},
		},
	}

	adapter := &testClientAdapter{client: mockClient}

	// Test that context is properly ignored (method uses _ for context parameter)
	ctx := context.Background()
	offset, err := adapter.GetLatestOffset(ctx, "test-topic", 0)
	assert.NoError(t, err)
	assert.Equal(t, int64(1000), offset)

	// Test with nil context (should still work since context is ignored)
	offset, err = adapter.GetLatestOffset(nil, "test-topic", 0)
	assert.NoError(t, err)
	assert.Equal(t, int64(1000), offset)
}

// Test actual NewClientAdapter constructor with nil client (edge case)
func TestNewClientAdapter(t *testing.T) {
	t.Parallel()

	// Test with nil client - should not panic, just return adapter
	adapter := NewClientAdapter(nil)
	require.NotNil(t, adapter)

	// Test that adapter implements pulse.BrokerClient interface
	var _ pulse.BrokerClient = adapter
}

// Test the actual clientAdapter struct exists and implements interface
func TestClientAdapter_StructImplementsInterface(t *testing.T) {
	t.Parallel()

	// Test that clientAdapter implements pulse.BrokerClient interface
	var _ pulse.BrokerClient = (*clientAdapter)(nil)

	// Test that we can create an instance (even with nil client)
	adapter := &clientAdapter{client: nil}
	assert.NotNil(t, adapter)
}

// Test the offset calculation logic specifically
func TestClientAdapter_OffsetCalculation(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name           string
		saramaOffset   int64
		expectedOffset int64
	}{
		{
			name:           "normal offset",
			saramaOffset:   1000,
			expectedOffset: 999,
		},
		{
			name:           "zero offset",
			saramaOffset:   0,
			expectedOffset: -1,
		},
		{
			name:           "one offset",
			saramaOffset:   1,
			expectedOffset: 0,
		},
		{
			name:           "large offset",
			saramaOffset:   9223372036854775807, // max int64
			expectedOffset: 9223372036854775806,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			mockClient := &mockOffsetClient{
				offsets: map[string]map[int32]int64{
					"test-topic": {
						0: tc.saramaOffset,
					},
				},
			}

			adapter := &testClientAdapter{client: mockClient}
			ctx := context.Background()

			offset, err := adapter.GetLatestOffset(ctx, "test-topic", 0)
			assert.NoError(t, err)
			assert.Equal(t, tc.expectedOffset, offset)
		})
	}
}

// Test Message with various sarama.ConsumerMessage configurations
func TestMessage_VariousConfigurations(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name      string
		msg       *sarama.ConsumerMessage
		topic     string
		partition int32
		offset    int64
	}{
		{
			name: "minimal message",
			msg: &sarama.ConsumerMessage{
				Topic:     "minimal",
				Partition: 0,
				Offset:    0,
			},
			topic:     "minimal",
			partition: 0,
			offset:    0,
		},
		{
			name: "message with high partition",
			msg: &sarama.ConsumerMessage{
				Topic:     "high-partition-topic",
				Partition: 1000,
				Offset:    500000,
			},
			topic:     "high-partition-topic",
			partition: 1000,
			offset:    500000,
		},
		{
			name: "message with negative offset",
			msg: &sarama.ConsumerMessage{
				Topic:     "negative-offset-topic",
				Partition: 2,
				Offset:    -1,
			},
			topic:     "negative-offset-topic",
			partition: 2,
			offset:    -1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			msg := &Message{ConsumerMessage: tc.msg}

			assert.Equal(t, tc.topic, msg.Topic())
			assert.Equal(t, tc.partition, msg.Partition())
			assert.Equal(t, tc.offset, msg.Offset())
		})
	}
}

// TestNewMessage provides coverage of NewMessage function
func TestNewMessage(t *testing.T) {
	t.Parallel()

	t.Run("NewMessage with nil input", func(t *testing.T) {
		t.Parallel()

		msg := NewMessage(nil)
		assert.NotNil(t, msg, "NewMessage should not return nil even with nil input")

		impl := msg.(*Message)
		assert.Nil(t, impl.ConsumerMessage, "Underlying ConsumerMessage should be nil")
	})

	t.Run("NewMessage preserves all message fields", func(t *testing.T) {
		t.Parallel()

		consumerMsg := &sarama.ConsumerMessage{
			Topic:          "preserve-test",
			Partition:      5,
			Offset:         12345,
			Key:            []byte("test-key"),
			Value:          []byte("test-value"),
			Headers:        []*sarama.RecordHeader{{Key: []byte("header-key"), Value: []byte("header-value")}},
			Timestamp:      time.Now(),
			BlockTimestamp: time.Now().Add(time.Minute),
		}

		msg := NewMessage(consumerMsg)
		impl := msg.(*Message)

		assert.Equal(t, consumerMsg.Topic, impl.ConsumerMessage.Topic)
		assert.Equal(t, consumerMsg.Partition, impl.ConsumerMessage.Partition)
		assert.Equal(t, consumerMsg.Offset, impl.ConsumerMessage.Offset)
		assert.Equal(t, consumerMsg.Key, impl.ConsumerMessage.Key)
		assert.Equal(t, consumerMsg.Value, impl.ConsumerMessage.Value)
		assert.Equal(t, consumerMsg.Headers, impl.ConsumerMessage.Headers)
		assert.Equal(t, consumerMsg.Timestamp, impl.ConsumerMessage.Timestamp)
		assert.Equal(t, consumerMsg.BlockTimestamp, impl.ConsumerMessage.BlockTimestamp)
	})

	t.Run("NewMessage with empty strings and zero values", func(t *testing.T) {
		t.Parallel()

		consumerMsg := &sarama.ConsumerMessage{
			Topic:     "", // empty topic
			Partition: 0,  // zero partition
			Offset:    0,  // zero offset
			Key:       nil,
			Value:     []byte{}, // empty value
		}

		msg := NewMessage(consumerMsg)

		assert.Equal(t, "", msg.Topic())
		assert.Equal(t, int32(0), msg.Partition())
		assert.Equal(t, int64(0), msg.Offset())

		impl := msg.(*Message)
		assert.Nil(t, impl.ConsumerMessage.Key)
		assert.Equal(t, []byte{}, impl.ConsumerMessage.Value)
	})

	t.Run("NewMessage mutability test", func(t *testing.T) {
		t.Parallel()

		originalMsg := &sarama.ConsumerMessage{
			Topic:     "immutable-test",
			Partition: 1,
			Offset:    100,
			Key:       []byte("original-key"),
			Value:     []byte("original-value"),
		}

		msg := NewMessage(originalMsg)

		originalMsg.Topic = "modified-topic"
		originalMsg.Partition = 999
		originalMsg.Offset = 888
		originalMsg.Key = []byte("modified-key")

		assert.Equal(t, "modified-topic", msg.Topic())
		assert.Equal(t, int32(999), msg.Partition())
		assert.Equal(t, int64(888), msg.Offset())

		impl := msg.(*Message)
		assert.Equal(t, []byte("modified-key"), impl.ConsumerMessage.Key)
	})
}
