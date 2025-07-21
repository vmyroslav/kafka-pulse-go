package confluentic

import (
	"context"
	"fmt"
	"sync"
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

func TestHealthChecker_WithClientAdapter(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	mockCluster, configMap := setupMockCluster(t)

	t.Run("should be unhealthy when stale and lagging behind", func(t *testing.T) {
		t.Parallel()

		topic := "stuck-topic"

		err := mockCluster.CreateTopic(topic, 1, 1)
		require.NoError(t, err)

		brokerClient := NewClientAdapter(configMap)

		hc, err := pulse.NewHealthChecker(
			pulse.Config{StuckTimeout: 100 * time.Millisecond},
			brokerClient,
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

		// produce a new message to make the consumer "lag behind"
		err = p.Produce(&kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0}}, nil)
		require.NoError(t, err, "failed to produce new message to topic")
		p.Flush(1000)
		p.Close()

		// give the broker a moment to update its watermark
		time.Sleep(100 * time.Millisecond)

		healthy, err := hc.Healthy(ctx)
		assert.NoError(t, err)
		assert.False(t, healthy)
	})

	t.Run("a running consumer should be unhealthy when it gets stuck", func(t *testing.T) {
		t.Parallel()

		topic := fmt.Sprintf("live-consumer-topic-%d", 1)

		bootstrapServers := mockCluster.BootstrapServers()

		err := mockCluster.CreateTopic(topic, 1, 1)
		require.NoError(t, err)

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
			// this consumer will read only ONE message and then get "stuck"
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

		// wait until the consumer has processed and tracked the first message
		select {
		case <-messageProcessed: // success
		case <-time.After(5 * time.Second):
			t.Fatal("timeout: consumer never processed the first message")
		}

		// at this point, the consumer is healthy but its timestamp is about to become stale
		time.Sleep(150 * time.Millisecond) // wait for longer than the StuckTimeout

		// produce a new message
		// the consumer is stuck and will NOT process this makes the consumer "lagging behind"
		err = p.Produce(&kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0}}, nil)
		require.NoError(t, err)
		p.Flush(1000)

		// give broker time to update watermarks
		time.Sleep(100 * time.Millisecond)

		healthy, err := hc.Healthy(ctx)
		assert.NoError(t, err)
		assert.False(t, healthy, "the stuck consumer should be reported as unhealthy")
	})

	t.Run("should be healthy when idle but caught up", func(t *testing.T) {
		topic := "idle-topic"

		err := mockCluster.CreateTopic(topic, 1, 1)
		require.NoError(t, err)

		hc, _ := pulse.NewHealthChecker(pulse.Config{StuckTimeout: 100 * time.Millisecond}, NewClientAdapter(configMap))

		// produce and track a message, consumer is now caught up
		p, err := kafka.NewProducer(configMap)
		require.NoError(t, err, "failed to create producer")

		err = p.Produce(&kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0}}, nil)
		require.NoError(t, err, "failed to produce message to topic")
		p.Flush(1000)
		p.Close()

		hc.Track(ctx, NewMessage(&kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0, Offset: 0}}))

		// wait for longer than the StuckTimeout
		time.Sleep(150 * time.Millisecond)

		// still healthy because it's idle but not lagging
		healthy, err := hc.Healthy(ctx)
		assert.NoError(t, err)
		assert.True(t, healthy)
	})

	t.Run("multi-partition tracking - should be healthy when all partitions are caught up", func(t *testing.T) {
		t.Parallel()

		topic := "multi-partition-healthy-topic"
		numPartitions := 3

		err := mockCluster.CreateTopic(topic, numPartitions, 1)
		require.NoError(t, err)

		hc, err := pulse.NewHealthChecker(
			pulse.Config{StuckTimeout: 100 * time.Millisecond},
			NewClientAdapter(configMap),
		)
		require.NoError(t, err)

		p, err := kafka.NewProducer(configMap)
		require.NoError(t, err)
		defer p.Close()

		// produce messages to all partitions
		for partition := 0; partition < numPartitions; partition++ {
			err = p.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: int32(partition)},
			}, nil)
			require.NoError(t, err)
		}
		p.Flush(1000)

		// track messages from all partitions
		for partition := 0; partition < numPartitions; partition++ {
			hc.Track(ctx, NewMessage(&kafka.Message{
				TopicPartition: kafka.TopicPartition{
					Topic:     &topic,
					Partition: int32(partition),
					Offset:    0,
				},
			}))
		}

		healthy, err := hc.Healthy(ctx)
		assert.NoError(t, err)
		assert.True(t, healthy)
	})

	t.Run("multi-partition tracking - should be unhealthy when one partition is stuck", func(t *testing.T) {
		t.Parallel()

		topic := "multi-partition-stuck-topic"
		numPartitions := 3
		hc, err := pulse.NewHealthChecker(
			pulse.Config{StuckTimeout: 100 * time.Millisecond},
			NewClientAdapter(configMap),
		)
		require.NoError(t, err)

		p, err := kafka.NewProducer(configMap)
		require.NoError(t, err)
		defer p.Close()

		// produce initial messages to all partitions
		for partition := 0; partition < numPartitions; partition++ {
			err = p.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: int32(partition)},
			}, nil)
			require.NoError(t, err)
		}
		p.Flush(1000)

		// track messages from all partitions
		for partition := 0; partition < numPartitions; partition++ {
			hc.Track(ctx, NewMessage(&kafka.Message{
				TopicPartition: kafka.TopicPartition{
					Topic:     &topic,
					Partition: int32(partition),
					Offset:    0,
				},
			}))
		}

		// wait for timestamps to become stale
		time.Sleep(150 * time.Millisecond)

		// produce a new message to partition 1 only (making it lag behind)
		err = p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 1},
		}, nil)
		require.NoError(t, err)
		p.Flush(1000)

		// give broker time to update watermarks
		time.Sleep(100 * time.Millisecond)

		// should be unhealthy because partition 1 is lagging
		healthy, err := hc.Healthy(ctx)
		assert.NoError(t, err)
		assert.False(t, healthy)
	})

	t.Run("multi-partition tracking - should handle mixed partition states correctly", func(t *testing.T) {
		t.Parallel()

		topic := "multi-partition-mixed-topic"
		hc, err := pulse.NewHealthChecker(
			pulse.Config{StuckTimeout: 100 * time.Millisecond},
			NewClientAdapter(configMap),
		)
		require.NoError(t, err)

		p, err := kafka.NewProducer(configMap)
		require.NoError(t, err)
		defer p.Close()

		// produce different numbers of messages to different partitions
		err = p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0},
		}, nil)
		require.NoError(t, err)

		for i := 0; i < 3; i++ {
			err = p.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 1},
			}, nil)
			require.NoError(t, err)
		}

		for i := 0; i < 2; i++ {
			err = p.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 2},
			}, nil)
			require.NoError(t, err)
		}
		p.Flush(1000)

		// track latest messages from all partitions (all caught up)
		hc.Track(ctx, NewMessage(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0, Offset: 0},
		}))
		hc.Track(ctx, NewMessage(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 1, Offset: 2},
		}))
		hc.Track(ctx, NewMessage(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 2, Offset: 1},
		}))

		// should be healthy - all partitions are caught up
		healthy, err := hc.Healthy(ctx)
		assert.NoError(t, err)
		assert.True(t, healthy)

		// now track an older message from partition 1, making it lag
		hc.Track(ctx, NewMessage(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 1, Offset: 0},
		}))

		// wait for timestamp to become stale
		time.Sleep(150 * time.Millisecond)

		// should be unhealthy because partition 1 is now lagging
		healthy, err = hc.Healthy(ctx)
		assert.NoError(t, err)
		assert.False(t, healthy)
	})
}

func TestHealthChecker_ConsumerGroup_WithConfluentAdapter(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	mockCluster, err := kafka.NewMockCluster(1)
	require.NoError(t, err, "failed to create mock cluster")

	err = mockCluster.SetBrokerUp(1)
	require.NoError(t, err, "failed to set broker up in mock cluster")

	bootstrapServers := mockCluster.BootstrapServers()
	require.NotEmpty(t, bootstrapServers, "mock cluster did not provide bootstrap servers")

	configMap := &kafka.ConfigMap{"bootstrap.servers": bootstrapServers}

	t.Run("consumer should track multiple partitions independently", func(t *testing.T) {
		t.Parallel()

		topic := "multi-partition-consumer-topic"

		hc, err := pulse.NewHealthChecker(
			pulse.Config{StuckTimeout: 100 * time.Millisecond},
			NewClientAdapter(configMap),
		)
		require.NoError(t, err)

		// produce messages to multiple partitions
		p, err := kafka.NewProducer(configMap)
		require.NoError(t, err)
		defer p.Close()

		for partition := 0; partition < 2; partition++ {
			for i := 0; i < 3; i++ {
				err = p.Produce(&kafka.Message{
					TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: int32(partition)},
				}, nil)
				require.NoError(t, err)
			}
		}
		p.Flush(1000)

		// consumer tracks messages from both partitions
		hc.Track(ctx, NewMessage(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0, Offset: 2},
		}))

		hc.Track(ctx, NewMessage(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 1, Offset: 2},
		}))

		// consumer should be healthy when caught up on all assigned partitions
		healthy, err := hc.Healthy(ctx)
		assert.NoError(t, err)
		assert.True(t, healthy, "consumer should be healthy when caught up on all partitions")
	})

	t.Run("consumer group rebalancing scenario - should handle partition reassignment", func(t *testing.T) {
		t.Parallel()

		topic := "rebalance-topic"
		groupID := "rebalance-group"

		hc, err := pulse.NewHealthChecker(
			pulse.Config{StuckTimeout: 100 * time.Millisecond},
			NewClientAdapter(configMap),
		)
		require.NoError(t, err)

		consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
			"bootstrap.servers": bootstrapServers,
			"group.id":          groupID,
			"auto.offset.reset": "earliest",
		})
		require.NoError(t, err)
		defer func(consumer *kafka.Consumer) {
			_ = consumer.Close()
		}(consumer)

		// produce messages to multiple partitions
		p, err := kafka.NewProducer(configMap)
		require.NoError(t, err)
		defer p.Close()

		for partition := 0; partition < 3; partition++ {
			for i := 0; i < 2; i++ {
				err = p.Produce(&kafka.Message{
					TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: int32(partition)},
				}, nil)
				require.NoError(t, err)
			}
		}
		p.Flush(1000)

		// simulate consumer processing messages from multiple partitions (before rebalance)
		hc.Track(ctx, NewMessage(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0, Offset: 1},
		}))
		hc.Track(ctx, NewMessage(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 1, Offset: 1},
		}))
		hc.Track(ctx, NewMessage(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 2, Offset: 1},
		}))

		// consumer should be healthy
		healthy, err := hc.Healthy(ctx)
		assert.NoError(t, err)
		assert.True(t, healthy)

		// simulate rebalancing by releasing some partitions
		hc.Release(ctx, topic, 1)
		hc.Release(ctx, topic, 2)

		// consumer should still be healthy for remaining partition
		healthy, err = hc.Healthy(ctx)
		assert.NoError(t, err)
		assert.True(t, healthy)

		// produce new messages to the released partitions
		for i := 0; i < 2; i++ {
			err = p.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 1},
			}, nil)
			require.NoError(t, err)
		}
		p.Flush(1000)

		// consumer should still be healthy since it's not tracking the released partitions
		healthy, err = hc.Healthy(ctx)
		assert.NoError(t, err)
		assert.True(t, healthy)
	})

	t.Run("consumer group with different auto.offset.reset settings", func(t *testing.T) {
		t.Parallel()

		topic := "offset-reset-topic"
		groupID := "offset-reset-group"

		hc, err := pulse.NewHealthChecker(
			pulse.Config{StuckTimeout: 100 * time.Millisecond},
			NewClientAdapter(configMap),
		)
		require.NoError(t, err)

		// create consumers with different offset reset strategies
		consumerEarliest, err := kafka.NewConsumer(&kafka.ConfigMap{
			"bootstrap.servers": bootstrapServers,
			"group.id":          groupID + "-earliest",
			"auto.offset.reset": "earliest",
		})
		require.NoError(t, err)
		defer func(consumerEarliest *kafka.Consumer) {
			_ = consumerEarliest.Close()
		}(consumerEarliest)

		consumerLatest, err := kafka.NewConsumer(&kafka.ConfigMap{
			"bootstrap.servers": bootstrapServers,
			"group.id":          groupID + "-latest",
			"auto.offset.reset": "latest",
		})
		require.NoError(t, err)
		defer func(consumerLatest *kafka.Consumer) {
			_ = consumerLatest.Close()
		}(consumerLatest)

		// produce some messages
		p, err := kafka.NewProducer(configMap)
		require.NoError(t, err)
		defer p.Close()

		for i := 0; i < 5; i++ {
			err = p.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0},
			}, nil)
			require.NoError(t, err)
		}
		p.Flush(1000)

		// simulate the earliest consumer tracking from beginning
		hc.Track(ctx, NewMessage(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0, Offset: 0},
		}))

		// wait for timestamp to become stale
		time.Sleep(150 * time.Millisecond)

		// should be unhealthy because consumer is behind
		healthy, err := hc.Healthy(ctx)
		assert.NoError(t, err)
		assert.False(t, healthy)

		// track the latest message
		hc.Track(ctx, NewMessage(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0, Offset: 4},
		}))

		// should be healthy again
		healthy, err = hc.Healthy(ctx)
		assert.NoError(t, err)
		assert.True(t, healthy)
	})

	t.Run("consumer group with manual offset management", func(t *testing.T) {
		t.Parallel()

		topic := "manual-offset-topic"
		groupID := "manual-offset-group"

		hc, err := pulse.NewHealthChecker(
			pulse.Config{StuckTimeout: 100 * time.Millisecond},
			NewClientAdapter(configMap),
		)
		require.NoError(t, err)

		// create consumer with manual offset management
		consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
			"bootstrap.servers":  bootstrapServers,
			"group.id":           groupID,
			"auto.offset.reset":  "earliest",
			"enable.auto.commit": false, // manual offset management
		})
		require.NoError(t, err)
		defer func(consumer *kafka.Consumer) {
			_ = consumer.Close()
		}(consumer)

		// produce messages
		p, err := kafka.NewProducer(configMap)
		require.NoError(t, err)
		defer p.Close()

		for i := 0; i < 3; i++ {
			err = p.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0},
			}, nil)
			require.NoError(t, err)
		}
		p.Flush(1000)

		// simulate processing messages but not committing offsets
		hc.Track(ctx, NewMessage(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0, Offset: 1},
		}))

		// should be healthy since consumer is caught up
		healthy, err := hc.Healthy(ctx)
		assert.NoError(t, err)
		assert.True(t, healthy)

		// produce more messages
		for i := 0; i < 2; i++ {
			err = p.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0},
			}, nil)
			require.NoError(t, err)
		}
		p.Flush(1000)

		// wait for timestamp to become stale
		time.Sleep(150 * time.Millisecond)

		// should be unhealthy because consumer is now behind
		healthy, err = hc.Healthy(ctx)
		assert.NoError(t, err)
		assert.False(t, healthy)
	})
}

func TestHealthChecker_Consumers_WithConfluentAdapter(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	mockCluster, err := kafka.NewMockCluster(1)
	require.NoError(t, err, "failed to create mock cluster")

	err = mockCluster.SetBrokerUp(1)
	require.NoError(t, err, "failed to set broker up in mock cluster")

	bootstrapServers := mockCluster.BootstrapServers()
	require.NotEmpty(t, bootstrapServers, "mock cluster did not provide bootstrap servers")

	configMap := &kafka.ConfigMap{"bootstrap.servers": bootstrapServers}

	t.Run("concurrent message tracking should be thread-safe", func(t *testing.T) {
		t.Parallel()

		topic := "concurrent-tracking-topic"
		numPartitions := 4
		numGoroutines := 10
		messagesPerGoroutine := 20

		hc, err := pulse.NewHealthChecker(
			pulse.Config{StuckTimeout: 200 * time.Millisecond},
			NewClientAdapter(configMap),
		)
		require.NoError(t, err)

		// produce messages to multiple partitions
		p, err := kafka.NewProducer(configMap)
		require.NoError(t, err)
		defer p.Close()

		for partition := 0; partition < numPartitions; partition++ {
			for i := 0; i < messagesPerGoroutine; i++ {
				err = p.Produce(&kafka.Message{
					TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: int32(partition)},
				}, nil)
				require.NoError(t, err)
			}
		}
		p.Flush(1000)

		// start multiple goroutines to track messages concurrently
		var wg sync.WaitGroup
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(goroutineID int) {
				defer wg.Done()
				partition := goroutineID % numPartitions

				for msgIdx := 0; msgIdx < messagesPerGoroutine; msgIdx++ {
					hc.Track(ctx, NewMessage(&kafka.Message{
						TopicPartition: kafka.TopicPartition{
							Topic:     &topic,
							Partition: int32(partition),
							Offset:    kafka.Offset(msgIdx),
						},
					}))

					// small delay to simulate processing time
					time.Sleep(10 * time.Millisecond)
				}
			}(i)
		}

		wg.Wait()

		// health check should complete without race conditions
		healthy, err := hc.Healthy(ctx)
		assert.NoError(t, err)
		assert.True(t, healthy)
	})

	t.Run("concurrent health checks should be consistent", func(t *testing.T) {
		t.Parallel()

		topic := "concurrent-health-topic"
		numHealthChecks := 50

		hc, err := pulse.NewHealthChecker(
			pulse.Config{StuckTimeout: 100 * time.Millisecond},
			NewClientAdapter(configMap),
		)
		require.NoError(t, err)

		// produce and track a message
		p, err := kafka.NewProducer(configMap)
		require.NoError(t, err)
		defer p.Close()

		err = p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0},
		}, nil)
		require.NoError(t, err)
		p.Flush(1000)

		hc.Track(ctx, NewMessage(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0, Offset: 0},
		}))

		// run multiple health checks concurrently
		var wg sync.WaitGroup
		results := make(chan bool, numHealthChecks)
		errCh := make(chan error, numHealthChecks)

		for i := 0; i < numHealthChecks; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				healthy, err := hc.Healthy(ctx)
				results <- healthy
				errCh <- err
			}()
		}

		wg.Wait()
		close(results)
		close(errCh)

		// all health checks should return consistent results
		var healthyCount int
		for healthy := range results {
			if healthy {
				healthyCount++
			}
		}

		// Check that no errCh occurred
		for err := range errCh {
			assert.NoError(t, err)
		}

		// all should be healthy
		assert.Equal(t, numHealthChecks, healthyCount)
	})

	t.Run("concurrent partition release should be thread-safe", func(t *testing.T) {
		t.Parallel()

		topic := "concurrent-release-topic"
		numPartitions := 10

		hc, err := pulse.NewHealthChecker(
			pulse.Config{StuckTimeout: 100 * time.Millisecond},
			NewClientAdapter(configMap),
		)
		require.NoError(t, err)

		// produce and track messages to all partitions
		p, err := kafka.NewProducer(configMap)
		require.NoError(t, err)
		defer p.Close()

		for partition := 0; partition < numPartitions; partition++ {
			err = p.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: int32(partition)},
			}, nil)
			require.NoError(t, err)
		}
		p.Flush(1000)

		// track messages from all partitions
		for partition := 0; partition < numPartitions; partition++ {
			hc.Track(ctx, NewMessage(&kafka.Message{
				TopicPartition: kafka.TopicPartition{
					Topic:     &topic,
					Partition: int32(partition),
					Offset:    0,
				},
			}))
		}

		// release partitions
		var wg sync.WaitGroup
		for partition := 0; partition < numPartitions; partition++ {
			wg.Add(1)
			go func(p int) {
				defer wg.Done()
				hc.Release(ctx, topic, int32(p))
			}(partition)
		}

		wg.Wait()

		// should succeed after all partitions are released
		healthy, err := hc.Healthy(ctx)
		assert.NoError(t, err)
		assert.True(t, healthy)
	})
}

func TestHealthChecker_ErrorRecovery_WithConfluentAdapter(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	mockCluster, err := kafka.NewMockCluster(1)
	require.NoError(t, err, "failed to create mock cluster")

	err = mockCluster.SetBrokerUp(1)
	require.NoError(t, err, "failed to set broker up in mock cluster")

	bootstrapServers := mockCluster.BootstrapServers()
	require.NotEmpty(t, bootstrapServers, "mock cluster did not provide bootstrap servers")

	configMap := &kafka.ConfigMap{"bootstrap.servers": bootstrapServers}

	t.Run("should recover from temporary client adapter errors", func(t *testing.T) {
		t.Parallel()

		topic := "recovery-topic"

		// create client adapter with invalid config first
		invalidConfigMap := &kafka.ConfigMap{"bootstrap.servers": "invalid-server:9092"}

		hc, err := pulse.NewHealthChecker(
			pulse.Config{StuckTimeout: 100 * time.Millisecond},
			NewClientAdapter(invalidConfigMap),
		)
		require.NoError(t, err)

		// track a message
		hc.Track(ctx, NewMessage(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0, Offset: 0},
		}))

		// wait for timestamp to become stale
		time.Sleep(150 * time.Millisecond)

		// health check should fail due to invalid config
		_, err = hc.Healthy(ctx)
		assert.Error(t, err)

		// now create a new health checker with valid config
		validHC, err := pulse.NewHealthChecker(
			pulse.Config{StuckTimeout: 100 * time.Millisecond},
			NewClientAdapter(configMap),
		)
		require.NoError(t, err)

		// produce a message to valid cluster
		p, err := kafka.NewProducer(configMap)
		require.NoError(t, err)
		defer p.Close()

		err = p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0},
		}, nil)
		require.NoError(t, err)
		p.Flush(1000)

		validHC.Track(ctx, NewMessage(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0, Offset: 0},
		}))

		healthy, err := validHC.Healthy(ctx)
		assert.NoError(t, err)
		assert.True(t, healthy)
	})

	t.Run("should handle broker unavailability gracefully", func(t *testing.T) {
		t.Parallel()

		topic := "broker-unavailable-topic"

		hc, err := pulse.NewHealthChecker(
			pulse.Config{StuckTimeout: 100 * time.Millisecond},
			NewClientAdapter(configMap),
		)
		require.NoError(t, err)

		// Produce and track a message when broker is up
		p, err := kafka.NewProducer(configMap)
		require.NoError(t, err)
		defer p.Close()

		err = p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0},
		}, nil)
		require.NoError(t, err)
		p.Flush(1000)

		hc.Track(ctx, NewMessage(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0, Offset: 0},
		}))

		// Health check should succeed when broker is up
		healthy, err := hc.Healthy(ctx)
		assert.NoError(t, err)
		assert.True(t, healthy)

		// simulate broker going down
		err = mockCluster.SetBrokerDown(1)
		require.NoError(t, err)

		// wait for timestamp to become stale
		time.Sleep(150 * time.Millisecond)

		// health check should fail when broker is down
		_, err = hc.Healthy(ctx)
		assert.Error(t, err)

		// bring broker back up
		err = mockCluster.SetBrokerUp(1)
		require.NoError(t, err)

		// give some time for the connection to recover
		time.Sleep(100 * time.Millisecond)

		// health check should recover
		healthy, err = hc.Healthy(ctx)
		assert.NoError(t, err)
		assert.True(t, healthy)
	})

	t.Run("should handle topic deletion and recreation", func(t *testing.T) {
		t.Parallel()

		topic := "topic-deletion-topic"

		hc, err := pulse.NewHealthChecker(
			pulse.Config{StuckTimeout: 100 * time.Millisecond},
			NewClientAdapter(configMap),
		)
		require.NoError(t, err)

		// create topic and produce a message
		p, err := kafka.NewProducer(configMap)
		require.NoError(t, err)
		defer p.Close()

		err = p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0},
		}, nil)
		require.NoError(t, err)
		p.Flush(1000)

		hc.Track(ctx, NewMessage(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0, Offset: 0},
		}))

		healthy, err := hc.Healthy(ctx)
		assert.NoError(t, err)
		assert.True(t, healthy)

		// release the partition (simulating topic deletion)
		hc.Release(ctx, topic, 0)

		// health check should succeed after release
		healthy, err = hc.Healthy(ctx)
		assert.NoError(t, err)
		assert.True(t, healthy)

		// recreate topic by producing a new message
		err = p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0},
		}, nil)
		require.NoError(t, err)
		p.Flush(1000)

		// track the new message
		hc.Track(ctx, NewMessage(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0, Offset: 0},
		}))

		// health check should succeed with recreated topic
		healthy, err = hc.Healthy(ctx)
		assert.NoError(t, err)
		assert.True(t, healthy)
	})
}

func TestHealthChecker_HighThroughput_WithConfluentAdapter(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	mockCluster, err := kafka.NewMockCluster(1)
	require.NoError(t, err, "failed to create mock cluster")

	err = mockCluster.SetBrokerUp(1)
	require.NoError(t, err, "failed to set broker up in mock cluster")

	bootstrapServers := mockCluster.BootstrapServers()
	require.NotEmpty(t, bootstrapServers, "mock cluster did not provide bootstrap servers")

	configMap := &kafka.ConfigMap{"bootstrap.servers": bootstrapServers}

	t.Run("backpressure scenario - slow consumer should be detected as unhealthy", func(t *testing.T) {
		t.Parallel()

		topic := "backpressure-topic"

		hc, err := pulse.NewHealthChecker(
			pulse.Config{StuckTimeout: 100 * time.Millisecond},
			NewClientAdapter(configMap),
		)
		require.NoError(t, err)

		p, err := kafka.NewProducer(configMap)
		require.NoError(t, err)
		defer p.Close()

		messageCount := 100
		for i := 0; i < messageCount; i++ {
			err = p.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0},
				Key:            []byte(fmt.Sprintf("key-%d", i)),
				Value:          []byte(fmt.Sprintf("message-%d", i)),
			}, nil)
			require.NoError(t, err)
		}
		p.Flush(2000)

		// simulate slow consumer that only processes a few messages
		slowConsumerOffset := int64(5) // only processed 6 messages out of 100
		hc.Track(ctx, NewMessage(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0, Offset: kafka.Offset(slowConsumerOffset)},
		}))

		// wait for timestamp to become stale
		time.Sleep(150 * time.Millisecond)

		// consumer should be unhealthy due to significant lag
		healthy, err := hc.Healthy(ctx)
		assert.NoError(t, err)
		assert.False(t, healthy, "slow consumer should be unhealthy due to backpressure")

		// now simulate consumer catching up
		catchUpOffset := int64(messageCount - 1) // caught up to last message
		hc.Track(ctx, NewMessage(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0, Offset: kafka.Offset(catchUpOffset)},
		}))

		// consumer should now be healthy
		healthy, err = hc.Healthy(ctx)
		assert.NoError(t, err)
		assert.True(t, healthy, "consumer should be healthy after catching up")
	})

	t.Run("burst traffic scenario - should handle traffic spikes correctly", func(t *testing.T) {
		t.Parallel()

		topic := "burst-traffic-topic"

		hc, err := pulse.NewHealthChecker(
			pulse.Config{StuckTimeout: 200 * time.Millisecond},
			NewClientAdapter(configMap),
		)
		require.NoError(t, err)

		p, err := kafka.NewProducer(configMap)
		require.NoError(t, err)
		defer p.Close()

		// phase 1: low traffic - produce a few messages
		for i := 0; i < 5; i++ {
			err = p.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0},
			}, nil)
			require.NoError(t, err)
		}
		p.Flush(1000)

		// consumer keeps up with low traffic
		hc.Track(ctx, NewMessage(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0, Offset: 4},
		}))

		// should be healthy during low traffic
		healthy, err := hc.Healthy(ctx)
		assert.NoError(t, err)
		assert.True(t, healthy)

		// phase 2: traffic burst - sudden spike in messages
		burstSize := 50
		for i := 0; i < burstSize; i++ {
			err = p.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0},
			}, nil)
			require.NoError(t, err)
		}
		p.Flush(2000)

		// consumer hasn't caught up to the burst yet (still at offset 4)
		time.Sleep(250 * time.Millisecond) // wait longer than StuckTimeout

		// should be unhealthy due to burst lag
		healthy, err = hc.Healthy(ctx)
		assert.NoError(t, err)
		assert.False(t, healthy, "consumer should be unhealthy during traffic burst")

		// phase 3: Consumer processes burst messages
		hc.Track(ctx, NewMessage(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0, Offset: kafka.Offset(5 + burstSize - 1)},
		}))

		// should be healthy after processing burst
		healthy, err = hc.Healthy(ctx)
		assert.NoError(t, err)
		assert.True(t, healthy, "consumer should be healthy after processing burst")
	})

	t.Run("idle period followed by activity - should distinguish between stuck and idle", func(t *testing.T) {
		t.Parallel()

		topic := "idle-activity-topic"

		hc, err := pulse.NewHealthChecker(
			pulse.Config{StuckTimeout: 100 * time.Millisecond},
			NewClientAdapter(configMap),
		)
		require.NoError(t, err)

		p, err := kafka.NewProducer(configMap)
		require.NoError(t, err)
		defer p.Close()

		// phase 1: Initial activity
		err = p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0},
		}, nil)
		require.NoError(t, err)
		p.Flush(1000)

		hc.Track(ctx, NewMessage(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0, Offset: 0},
		}))

		// should be healthy initially
		healthy, err := hc.Healthy(ctx)
		assert.NoError(t, err)
		assert.True(t, healthy)

		// phase 2: Long idle period - no new messages
		time.Sleep(200 * time.Millisecond) // wait longer than StuckTimeout

		// should still be healthy because consumer is caught up (idle, not stuck)
		healthy, err = hc.Healthy(ctx)
		assert.NoError(t, err)
		assert.True(t, healthy, "consumer should be healthy during idle period")

		// phase 3: Activity resumes
		err = p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0},
		}, nil)
		require.NoError(t, err)
		p.Flush(1000)

		// consumer doesn't immediately process the new message
		time.Sleep(150 * time.Millisecond) // wait longer than StuckTimeout

		// now should be unhealthy because there's a new message but consumer is stuck
		healthy, err = hc.Healthy(ctx)
		assert.NoError(t, err)
		assert.False(t, healthy, "consumer should be unhealthy when stuck after idle period")

		// consumer processes the new message
		hc.Track(ctx, NewMessage(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0, Offset: 1},
		}))

		// should be healthy again
		healthy, err = hc.Healthy(ctx)
		assert.NoError(t, err)
		assert.True(t, healthy, "consumer should be healthy after processing new message")
	})

	t.Run("multiple partition burst scenario - should handle uneven load distribution", func(t *testing.T) {
		t.Parallel()

		topic := "multi-partition-burst-topic"
		numPartitions := 4

		hc, err := pulse.NewHealthChecker(
			pulse.Config{StuckTimeout: 100 * time.Millisecond},
			NewClientAdapter(configMap),
		)
		require.NoError(t, err)

		p, err := kafka.NewProducer(configMap)
		require.NoError(t, err)
		defer p.Close()

		// phase 1: Balanced load - produce messages evenly across partitions
		for partition := 0; partition < numPartitions; partition++ {
			for i := 0; i < 5; i++ {
				err = p.Produce(&kafka.Message{
					TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: int32(partition)},
				}, nil)
				require.NoError(t, err)
			}
		}
		p.Flush(1000)

		// consumer keeps up with all partitions
		for partition := 0; partition < numPartitions; partition++ {
			hc.Track(ctx, NewMessage(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: int32(partition), Offset: 4},
			}))
		}

		// should be healthy with balanced load
		healthy, err := hc.Healthy(ctx)
		assert.NoError(t, err)
		assert.True(t, healthy)

		// phase 2: Uneven burst - heavy load on partition 1 only
		heavyLoadPartition := 1
		for i := 0; i < 30; i++ {
			err = p.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: int32(heavyLoadPartition)},
			}, nil)
			require.NoError(t, err)
		}
		p.Flush(1000)

		// consumer processes other partitions normally but gets stuck on heavy partition
		for partition := 0; partition < numPartitions; partition++ {
			if partition != heavyLoadPartition {
				hc.Track(ctx, NewMessage(&kafka.Message{
					TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: int32(partition), Offset: 4},
				}))
			} else {
				// consumer only processed a few messages from heavy partition
				hc.Track(ctx, NewMessage(&kafka.Message{
					TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: int32(partition), Offset: 10},
				}))
			}
		}

		// wait for timestamp to become stale
		time.Sleep(150 * time.Millisecond)

		// should be unhealthy due to lag on heavy partition
		healthy, err = hc.Healthy(ctx)
		assert.NoError(t, err)
		assert.False(t, healthy, "consumer should be unhealthy due to uneven load")

		// consumer catches up on heavy partition
		hc.Track(ctx, NewMessage(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: int32(heavyLoadPartition), Offset: 34}, // 5 + 30 - 1
		}))

		// should be healthy again
		healthy, err = hc.Healthy(ctx)
		assert.NoError(t, err)
		assert.True(t, healthy, "consumer should be healthy after catching up on heavy partition")
	})
}

// setupMockCluster creates a mock cluster for testing
func setupMockCluster(t *testing.T) (*kafka.MockCluster, *kafka.ConfigMap) {
	t.Helper()

	mockCluster, err := kafka.NewMockCluster(1)
	require.NoError(t, err, "failed to create mock cluster")

	err = mockCluster.SetBrokerUp(1)
	require.NoError(t, err, "failed to set broker up in mock cluster")

	bootstrapServers := mockCluster.BootstrapServers()
	require.NotEmpty(t, bootstrapServers, "mock cluster did not provide bootstrap servers")

	configMap := &kafka.ConfigMap{"bootstrap.servers": bootstrapServers}

	return mockCluster, configMap
}
