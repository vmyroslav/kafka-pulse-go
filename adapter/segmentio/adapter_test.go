package segmentio

import (
	"context"
	"io"
	"math"
	"testing"
	"time"

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
	adapter, err := NewClientAdapter(br)
	assert.NoError(t, err)

	assert.Equal(t, br, adapter.brokers)
	assert.True(t, adapter.ownDialer)
	assert.NotNil(t, adapter.dialer)
}

func TestClientAdapter_Constructors(t *testing.T) {
	t.Parallel()

	t.Run("NewClientAdapter with nil brokers", func(t *testing.T) {
		t.Parallel()
		adapter, err := NewClientAdapter(nil)
		assert.Error(t, err)
		assert.Nil(t, adapter)
	})

	t.Run("NewClientAdapter with empty brokers", func(t *testing.T) {
		t.Parallel()
		adapter, err := NewClientAdapter([]string{})
		assert.NoError(t, err)
		assert.NotNil(t, adapter)
		assert.Empty(t, adapter.brokers)
		assert.True(t, adapter.ownDialer)
		assert.NotNil(t, adapter.dialer)
	})

	t.Run("NewClientAdapterWithDialer with nil dialer returns error", func(t *testing.T) {
		t.Parallel()
		adapter, err := NewClientAdapterWithDialer(nil, []string{"localhost:9092"})
		assert.Error(t, err)
		assert.Nil(t, adapter)
	})

	t.Run("NewClientAdapterWithDialer with nil brokers", func(t *testing.T) {
		t.Parallel()
		dialer := &kafka.Dialer{Timeout: 5 * time.Second}

		adapter, err := NewClientAdapterWithDialer(dialer, nil)
		assert.Error(t, err)
		assert.Nil(t, adapter)
	})
}

func TestClientAdapter_Close(t *testing.T) {
	t.Parallel()

	t.Run("Close() with owned dialer", func(t *testing.T) {
		t.Parallel()
		adapter, err := NewClientAdapter([]string{"localhost:9092"})
		assert.NoError(t, err)

		var closer io.Closer = adapter
		assert.NotNil(t, closer)

		assert.NotNil(t, adapter.dialer)
		assert.True(t, adapter.ownDialer)

		err = adapter.Close()
		assert.NoError(t, err)

		assert.Nil(t, adapter.dialer)
	})

	t.Run("Close() multiple times", func(t *testing.T) {
		t.Parallel()
		adapter, err := NewClientAdapter([]string{"localhost:9092"})
		assert.NoError(t, err)

		err = adapter.Close()
		assert.NoError(t, err)
		assert.Nil(t, adapter.dialer)

		err = adapter.Close()
		assert.NoError(t, err)
		assert.Nil(t, adapter.dialer)
	})
}

func TestClientAdapter_GetLatestOffset_NoBrokers(t *testing.T) {
	t.Parallel()

	t.Run("empty brokers list", func(t *testing.T) {
		t.Parallel()
		adapter, err := NewClientAdapter([]string{})
		assert.NoError(t, err)

		_, err = adapter.GetLatestOffset(context.Background(), "test-topic", 0)
		assert.Error(t, err)
	})

	t.Run("nil brokers list", func(t *testing.T) {
		t.Parallel()
		_, err := NewClientAdapter(nil)
		assert.Error(t, err)
	})
}

func TestClientAdapter_ThreadSafety(t *testing.T) {
	t.Parallel()

	adapter, err := NewClientAdapter([]string{"localhost:9092"})
	assert.NoError(t, err)

	done := make(chan bool, 10)

	for i := 0; i < 10; i++ {
		go func() {
			defer func() { done <- true }()

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			_, _ = adapter.GetLatestOffset(ctx, "test-topic", 0)
		}()
	}

	// wait for all goroutines to complete
	for i := 0; i < 10; i++ {
		<-done
	}

	err = adapter.Close()
	assert.NoError(t, err)
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

// TestNewMessage_Comprehensive provides comprehensive coverage of NewMessage function
func TestNewMessage_Comprehensive(t *testing.T) {
	t.Parallel()

	t.Run("NewMessage preserves all message fields", func(t *testing.T) {
		t.Parallel()

		testTime := time.Now()
		kafkaMsg := kafka.Message{
			Topic:     "preserve-test",
			Partition: 5,
			Offset:    12345,
			Key:       []byte("test-key"),
			Value:     []byte("test-value"),
			Headers:   []kafka.Header{{Key: "header-key", Value: []byte("header-value")}},
			Time:      testTime,
		}

		msg := NewMessage(kafkaMsg)
		impl := msg.(*Message)

		assert.Equal(t, kafkaMsg.Topic, impl.Message.Topic)
		assert.Equal(t, kafkaMsg.Partition, impl.Message.Partition)
		assert.Equal(t, kafkaMsg.Offset, impl.Message.Offset)
		assert.Equal(t, kafkaMsg.Key, impl.Message.Key)
		assert.Equal(t, kafkaMsg.Value, impl.Message.Value)
		assert.Equal(t, kafkaMsg.Headers, impl.Message.Headers)
		assert.Equal(t, kafkaMsg.Time, impl.Message.Time)
	})

	t.Run("NewMessage with complex headers", func(t *testing.T) {
		t.Parallel()

		headers := []kafka.Header{
			{Key: "content-type", Value: []byte("application/json")},
			{Key: "user-id", Value: []byte("12345")},
			{Key: "empty-header", Value: []byte{}},
			{Key: "nil-value", Value: nil},
		}

		kafkaMsg := kafka.Message{
			Topic:     "headers-test",
			Partition: 0,
			Offset:    100,
			Headers:   headers,
		}

		msg := NewMessage(kafkaMsg)
		impl := msg.(*Message)

		assert.Equal(t, headers, impl.Message.Headers)
		assert.Len(t, impl.Message.Headers, 4)
		assert.Equal(t, "content-type", impl.Message.Headers[0].Key)
		assert.Equal(t, []byte("application/json"), impl.Message.Headers[0].Value)
		assert.Nil(t, impl.Message.Headers[3].Value)
	})

	t.Run("NewMessage immutability test", func(t *testing.T) {
		t.Parallel()

		originalMsg := kafka.Message{
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

		assert.Equal(t, "immutable-test", msg.Topic())
		assert.Equal(t, int32(1), msg.Partition())
		assert.Equal(t, int64(100), msg.Offset())

		impl := msg.(*Message)
		assert.Equal(t, []byte("original-key"), impl.Message.Key)
	})
}
