package confluentic

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmyroslav/kafka-pulse-go/pulse"
)

func TestMessage_Wrapper(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name              string
		kafkaMsg          *kafka.Message
		expectedTopic     string
		expectedPartition int32
		expectedOffset    int64
	}

	stringPtr := func(s string) *string {
		if s == "" {
			return nil
		}
		return &s
	}

	testCases := []testCase{
		{
			name: "valid message",
			kafkaMsg: &kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: stringPtr("test-topic"), Partition: 1, Offset: 100},
				Key:            []byte("key"),
			},
			expectedTopic:     "test-topic",
			expectedPartition: 1,
			expectedOffset:    100,
		},
		{
			name: "nil topic",
			kafkaMsg: &kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: nil, Partition: 2, Offset: 200},
			},
			expectedTopic:     "",
			expectedPartition: 2,
			expectedOffset:    200,
		},
		{
			name: "special offset value",
			kafkaMsg: &kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: stringPtr("special-topic"), Partition: 3, Offset: kafka.OffsetEnd},
			},
			expectedTopic:     "special-topic",
			expectedPartition: 3,
			expectedOffset:    int64(kafka.OffsetEnd),
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

func TestClientAdapter_Implementation(t *testing.T) {
	t.Parallel()

	var (
		ctx            = context.Background()
		topicSingleMsg = "topic-single-message"
		topicMultiPart = "topic-multi-partition"
	)

	mockCluster, err := kafka.NewMockCluster(2)
	require.NoError(t, err, "Failed to create mock cluster")

	bootstrapServers := mockCluster.BootstrapServers()
	require.NotEmpty(t, bootstrapServers, "Mock cluster did not provide bootstrap servers in time")

	configMap := &kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
	}

	p, err := kafka.NewProducer(configMap)
	require.NoError(t, err)

	// produce 1 message to the single-message topic
	err = p.Produce(&kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &topicSingleMsg, Partition: 0}}, nil)
	require.NoError(t, err, "Failed to produce message to topicSingleMsg")

	for i := 0; i < 5; i++ {
		err = p.Produce(&kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &topicMultiPart, Partition: 2}}, nil)
		require.NoError(t, err, "Failed to produce message to topicMultiPart")
	}
	p.Flush(500)
	p.Close()

	adapter := NewClientAdapter(configMap)

	t.Run("success on partition with single message", func(t *testing.T) {
		t.Parallel()

		latestOffset, err := adapter.GetLatestOffset(ctx, topicSingleMsg, 0)
		assert.NoError(t, err)
		assert.Equal(t, int64(0), latestOffset)
	})

	t.Run("new test: success on partition with multiple messages", func(t *testing.T) {
		t.Parallel()
		// after 5 messages (offsets 0-4), the high watermark is 5
		// the latest offset should be 4
		latestOffset, err := adapter.GetLatestOffset(ctx, topicMultiPart, 2)
		assert.NoError(t, err)
		assert.Equal(t, int64(4), latestOffset)
	})

	t.Run("error on non-existent topic", func(t *testing.T) {
		t.Parallel()

		_, err := adapter.GetLatestOffset(ctx, "non-existent-topic", 0)
		assert.Error(t, err)
	})

	t.Run("new test: error on non-existent partition", func(t *testing.T) {
		t.Parallel()

		// topic exists but only has partition 0
		_, err = adapter.GetLatestOffset(ctx, topicSingleMsg, 99)
		assert.Error(t, err)

		var kafkaErr kafka.Error
		require.ErrorAs(t, err, &kafkaErr)
		assert.Equal(t, kafka.ErrUnknownPartition, kafkaErr.Code())
	})
}

func TestHealthChecker_Integration_WithConfluentAdapter(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	mockCluster, err := kafka.NewMockCluster(1)
	require.NoError(t, err, "failed to create mock cluster")

	err = mockCluster.SetBrokerUp(1)
	require.NoError(t, err, "failed to set broker up in mock cluster")
	// defer mockCluster.Close()

	bootstrapServers := mockCluster.BootstrapServers()
	require.NotEmpty(t, bootstrapServers, "mock cluster did not provide bootstrap servers")

	configMap := &kafka.ConfigMap{"bootstrap.servers": bootstrapServers}

	t.Run("should be unhealthy when stale and lagging behind", func(t *testing.T) {
		t.Parallel()
		topic := "stuck-topic"

		err = mockCluster.CreateTopic(topic, 1, 1)
		require.NoError(t, err)

		hc, err := pulse.NewHealthChecker(
			pulse.Config{StuckTimeout: 100 * time.Millisecond},
			NewClientAdapter(configMap),
		)
		require.NoError(t, err, "failed to create health checker")

		p, err := kafka.NewProducer(configMap)
		require.NoError(t, err, "failed to create producer")

		err = p.Produce(
			&kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0}},
			nil,
		)
		require.NoError(t, err, "failed to produce message to topic")
		p.Flush(1000)

		hc.Track(ctx, NewMessage(&kafka.Message{TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: 0,
			Offset:    0,
		}}))

		// wait for the timestamp to become "stale"
		time.Sleep(150 * time.Millisecond)

		// produce a new message to make the consumer "lag behind".
		err = p.Produce(&kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0}}, nil)
		require.NoError(t, err, "failed to produce new message to topic")
		p.Flush(1000)
		p.Close()

		// give the broker a moment to update its watermark to avoid race conditions.
		time.Sleep(100 * time.Millisecond)

		healthy, err := hc.Healthy(ctx)
		assert.NoError(t, err)
		assert.False(t, healthy)
	})

	t.Run("a running consumer should be unhealthy when it gets stuck", func(t *testing.T) {
		t.Parallel()

		topic := fmt.Sprintf("live-consumer-topic-%d", 1)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		hc, err := pulse.NewHealthChecker(
			pulse.Config{StuckTimeout: 100 * time.Millisecond},
			NewClientAdapter(configMap),
		)
		require.NoError(t, err)

		p, err := kafka.NewProducer(configMap)
		require.NoError(t, err)
		defer p.Close()

		consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
			"bootstrap.servers": bootstrapServers,
			"group.id":          fmt.Sprintf("test-group-%s", topic),
			"auto.offset.reset": "earliest",
		})
		require.NoError(t, err)
		defer func(consumer *kafka.Consumer) {
			_ = consumer.Close()
		}(consumer)

		err = consumer.SubscribeTopics([]string{topic}, nil)
		require.NoError(t, err)

		messageProcessed := make(chan struct{})

		go func() {
			// this consumer will read only ONE message and then get "stuck".
			msg, err := consumer.ReadMessage(5 * time.Second)
			require.NoError(t, err, "failed to read message from consumer")

			// use the health monitor inside the consumer loop
			hc.Track(ctx, NewMessage(msg))

			// signal to the main test that the first message has been processed
			messageProcessed <- struct{}{}

			// the consumer now stops reading, simulating being stuck
			<-ctx.Done()
		}()

		// produce the first message for our consumer to process
		err = p.Produce(&kafka.Message{TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: 0,
		}}, nil)
		require.NoError(t, err)
		p.Flush(1000)

		// wait until the consumer has processed and tracked the first message.
		select {
		case <-messageProcessed:
		// Success!
		case <-time.After(5 * time.Second):
			t.Fatal("Timeout: consumer never processed the first message")
		}

		// at this point, the consumer is healthy but its timestamp is about to become stale
		time.Sleep(150 * time.Millisecond) // wait for longer than the StuckTimeout

		// produce a new message. The consumer is stuck and will NOT process this
		// makes the consumer "lagging behind"
		err = p.Produce(&kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0}}, nil)
		require.NoError(t, err)

		healthy, err := hc.Healthy(ctx)
		assert.NoError(t, err)
		assert.False(t, healthy, "the stuck consumer should be reported as unhealthy")
	})

	t.Run("should be healthy when idle but caught up", func(t *testing.T) {
		t.Parallel()

		topic := "idle-topic"
		hc, _ := pulse.NewHealthChecker(pulse.Config{StuckTimeout: 100 * time.Millisecond}, NewClientAdapter(configMap))

		// produce and track a message, Consumer is now caught up.
		p, err := kafka.NewProducer(configMap)
		require.NoError(t, err, "failed to create producer")

		err = p.Produce(&kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0}}, nil)
		require.NoError(t, err, "failed to produce message to topic")
		p.Flush(1000)
		p.Close()

		hc.Track(ctx, NewMessage(&kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0, Offset: 0}}))

		// wait for longer than the StuckTimeout.
		time.Sleep(150 * time.Millisecond)

		// still healthy because it's idle but not lagging.
		healthy, err := hc.Healthy(ctx)
		assert.NoError(t, err)
		assert.True(t, healthy)
	})
}
