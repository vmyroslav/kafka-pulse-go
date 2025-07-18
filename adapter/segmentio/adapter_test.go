package segmentio

import (
	"math"
	"testing"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
)

func TestMessage_Wrapper(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name              string
		kafkaMsg          kafka.Message
		expectedTopic     string
		expectedPartition int32
		expectedOffset    int64
	}

	testCases := []testCase{
		{
			name: "valid message",
			kafkaMsg: kafka.Message{
				Topic:     "test-topic",
				Partition: 1,
				Offset:    100,
				Key:       []byte("key"),
			},
			expectedTopic:     "test-topic",
			expectedPartition: 1,
			expectedOffset:    100,
		},
		{
			name: "empty topic",
			kafkaMsg: kafka.Message{
				Topic:     "",
				Partition: 2,
				Offset:    200,
			},
			expectedTopic:     "",
			expectedPartition: 2,
			expectedOffset:    200,
		},
		{
			name: "zero offset",
			kafkaMsg: kafka.Message{
				Topic:     "zero-offset-topic",
				Partition: 3,
				Offset:    0,
			},
			expectedTopic:     "zero-offset-topic",
			expectedPartition: 3,
			expectedOffset:    0,
		},
		{
			name: "negative offset",
			kafkaMsg: kafka.Message{
				Topic:     "negative-topic",
				Partition: 0,
				Offset:    -1,
			},
			expectedTopic:     "negative-topic",
			expectedPartition: 0,
			expectedOffset:    -1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			msg := NewMessage(tc.kafkaMsg)

			assert.Equal(t, tc.expectedTopic, msg.Topic())
			assert.Equal(t, tc.expectedPartition, msg.Partition())
			assert.Equal(t, tc.expectedOffset, msg.Offset())

			// verify that the underlying message is still accessible
			if tc.kafkaMsg.Key != nil {
				impl := msg.(*Message)
				assert.Equal(t, tc.kafkaMsg.Key, impl.Message.Key)
			}
		})
	}
}

func TestNewMessage(t *testing.T) {
	t.Parallel()

	kafkaMsg := kafka.Message{
		Topic:     "test-topic",
		Partition: 5,
		Offset:    42,
		Key:       []byte("test-key"),
		Value:     []byte("test-value"),
	}

	msg := NewMessage(kafkaMsg)

	assert.IsType(t, &Message{}, msg)

	// test that the underlying message is preserved
	impl := msg.(*Message)
	assert.Equal(t, kafkaMsg, impl.Message)
}

func TestNewClientAdapter(t *testing.T) {
	t.Parallel()

	br := []string{"localhost:9092", "localhost:9093"}
	adapter := NewClientAdapter(br)

	assert.IsType(t, &clientAdapter{}, adapter)

	impl := adapter.(*clientAdapter)
	assert.Equal(t, br, impl.brokers)
}

func TestMessage_BoundaryValues(t *testing.T) {
	t.Parallel()

	t.Run("maximum values", func(t *testing.T) {
		t.Parallel()

		kafkaMsg := kafka.Message{
			Topic:     "max-value-topic",
			Partition: math.MaxInt32,
			Offset:    math.MaxInt64,
		}

		msg := NewMessage(kafkaMsg)

		assert.Equal(t, "max-value-topic", msg.Topic())
		assert.Equal(t, int32(math.MaxInt32), msg.Partition())
		assert.Equal(t, int64(math.MaxInt64), msg.Offset())
	})

	t.Run("minimum values", func(t *testing.T) {
		t.Parallel()

		kafkaMsg := kafka.Message{
			Topic:     "",
			Partition: 0,
			Offset:    math.MinInt64,
		}

		msg := NewMessage(kafkaMsg)

		assert.Equal(t, "", msg.Topic())
		assert.Equal(t, int32(0), msg.Partition())
		assert.Equal(t, int64(math.MinInt64), msg.Offset())
	})

	t.Run("partition conversion from int to int32", func(t *testing.T) {
		t.Parallel()

		testCases := []struct {
			name          string
			partition     int
			expectedInt32 int32
		}{
			{"zero partition", 0, 0},
			{"positive partition", 42, 42},
			{"max int32 partition", math.MaxInt32, math.MaxInt32},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()

				kafkaMsg := kafka.Message{
					Topic:     "test-topic",
					Partition: tc.partition,
					Offset:    100,
				}

				msg := NewMessage(kafkaMsg)
				assert.Equal(t, tc.expectedInt32, msg.Partition())
			})
		}
	})
}
