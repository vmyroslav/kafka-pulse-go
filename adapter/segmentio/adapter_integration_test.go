package segmentio_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	kafkacontainer "github.com/testcontainers/testcontainers-go/modules/kafka"
	segmentioadapter "github.com/vmyroslav/kafka-pulse-go/adapter/segmentio"
	"github.com/vmyroslav/kafka-pulse-go/pulse"
)

var (
	dockerImage = "confluentinc/confluent-local:7.5.0"
	brokers     []string
)

// Helper function to create a topic
func createTopic(topicName string, partitions int) {
	conn, err := kafka.Dial("tcp", brokers[0])
	if err != nil {
		panic(fmt.Sprintf("Failed to connect to Kafka: %v", err))
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		panic(fmt.Sprintf("Failed to get controller: %v", err))
	}
	controllerConn, err := kafka.Dial("tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	if err != nil {
		panic(fmt.Sprintf("Failed to connect to controller: %v", err))
	}
	defer controllerConn.Close()

	topicConfig := kafka.TopicConfig{
		Topic:             topicName,
		NumPartitions:     partitions,
		ReplicationFactor: 1,
	}

	err = controllerConn.CreateTopics(topicConfig)
	if err != nil {
		// Check if it's already exists error, which is ok
		if !strings.Contains(err.Error(), "already exists") {
			log.Printf("Topic creation error for %s: %v", topicName, err)
		} else {
			log.Printf("Topic %s already exists", topicName)
		}
	} else {
		log.Printf("Successfully created topic %s with %d partitions", topicName, partitions)
	}

	// Wait for topic metadata to propagate to all brokers by checking topic availability
	adapter := segmentioadapter.NewClientAdapter(brokers)
	ctx := context.Background()

	// Use a loop with timeout instead of fixed sleep
	timeout := time.After(5 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			log.Printf("Warning: Topic %s may not be fully available after 5s timeout", topicName)
			return
		case <-ticker.C:
			_, err := adapter.GetLatestOffset(ctx, topicName, 0)
			if err == nil {
				log.Printf("Topic %s is now available", topicName)
				return
			}
		}
	}
}

func TestMain(m *testing.M) {
	ctx := context.Background()

	// Start Kafka container
	kafkaContainer, err := kafkacontainer.Run(ctx, dockerImage,
		kafkacontainer.WithClusterID("test-cluster"),
	)
	if err != nil {
		log.Fatal(err)
	}

	brokers, err = kafkaContainer.Brokers(ctx)
	if err != nil {
		log.Fatal(err)
	}

	code := m.Run()

	if err = testcontainers.TerminateContainer(kafkaContainer); err != nil {
		log.Printf("failed to terminate container: %s", err)
	}

	os.Exit(code)
}

func TestClientAdapterIntegration_Implementation(t *testing.T) {
	t.Parallel()
	var (
		ctx            = context.Background()
		topicSingleMsg = "segmentio-topic-single-message"
		topicMultiPart = "segmentio-topic-multi-partition"
	)

	conn, err := kafka.Dial("tcp", brokers[0])
	require.NoError(t, err)
	defer func(conn *kafka.Conn) {
		_ = conn.Close()
	}(conn)

	controller, err := conn.Controller()
	require.NoError(t, err)
	controllerConn, err := kafka.Dial("tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	require.NoError(t, err)
	defer func(controllerConn *kafka.Conn) {
		_ = controllerConn.Close()
	}(controllerConn)

	topicConfigs := []kafka.TopicConfig{
		{
			Topic:             topicSingleMsg,
			NumPartitions:     1,
			ReplicationFactor: 1,
		},
		{
			Topic:             topicMultiPart,
			NumPartitions:     3,
			ReplicationFactor: 1,
		},
	}

	err = controllerConn.CreateTopics(topicConfigs...)
	require.NoError(t, err, "Failed to create topics")

	// Wait for topic metadata to propagate across the cluster
	adapter := segmentioadapter.NewClientAdapter(brokers)
	assert.Eventually(t, func() bool {
		_, err := adapter.GetLatestOffset(ctx, topicSingleMsg, 0)
		return err == nil
	}, 5*time.Second, 100*time.Millisecond, "topic metadata should propagate")

	writer := &kafka.Writer{
		Addr:                   kafka.TCP(brokers[0]),
		AllowAutoTopicCreation: true,
		RequiredAcks:           kafka.RequireAll,
		Async:                  false,
	}
	defer func(writer *kafka.Writer) {
		_ = writer.Close()
	}(writer)

	// produce 1 message to the single-message topic partition 0
	err = writer.WriteMessages(ctx, kafka.Message{
		Topic:     topicSingleMsg,
		Partition: 0,
		Value:     []byte("test message"),
	})
	require.NoError(t, err, "Failed to produce message to topicSingleMsg")

	// produce 5 messages to topicMultiPart - let Kafka choose partitions
	messages := make([]kafka.Message, 5)
	for i := 0; i < 5; i++ {
		messages[i] = kafka.Message{
			Topic: topicMultiPart,
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(fmt.Sprintf("message-%d", i)),
		}
	}
	err = writer.WriteMessages(ctx, messages...)
	require.NoError(t, err, "Failed to produce messages to topicMultiPart")

	// wait for messages to be committed to Kafka by checking offsets
	assert.Eventually(t, func() bool {
		latestOffset, err := adapter.GetLatestOffset(ctx, topicMultiPart, 0)
		if err != nil {
			return false
		}
		return latestOffset >= 0 // at least one message should be available
	}, 2*time.Second, 100*time.Millisecond, "messages should be committed to Kafka")

	t.Run("success on partition with single message", func(t *testing.T) {
		latestOffset, err := adapter.GetLatestOffset(ctx, topicSingleMsg, 0)
		assert.NoError(t, err)
		assert.Equal(t, int64(0), latestOffset)
	})

	t.Run("success on partition with multiple messages", func(t *testing.T) {
		// check all partitions to see which ones have messages
		assert.Eventually(t, func() bool {
			for partition := int32(0); partition < 3; partition++ {
				latestOffset, err := adapter.GetLatestOffset(ctx, topicMultiPart, partition)
				if err == nil && latestOffset >= 0 {
					t.Logf("Partition %d has latest offset: %d", partition, latestOffset)
					return true
				}
			}
			return false
		}, 2*time.Second, 100*time.Millisecond, "Expected at least one partition to have messages")
	})
}

func TestHealthCheckerIntegration_WithClientAdapter(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("should be unhealthy when stale and lagging behind", func(t *testing.T) {
		topic := "segmentio-stuck-topic"
		createTopic(topic, 1)

		brokerClient := segmentioadapter.NewClientAdapter(brokers)

		hc, err := pulse.NewHealthChecker(
			pulse.Config{StuckTimeout: 100 * time.Millisecond},
			brokerClient,
		)
		require.NoError(t, err, "failed to create health checker")

		writer := &kafka.Writer{
			Addr:                   kafka.TCP(brokers[0]),
			Balancer:               &kafka.LeastBytes{},
			AllowAutoTopicCreation: true,
		}
		defer writer.Close()

		err = writer.WriteMessages(ctx, kafka.Message{
			Topic:     topic,
			Partition: 0,
			Value:     []byte("initial message"),
		})
		require.NoError(t, err, "failed to produce message to topic")

		hc.Track(ctx, segmentioadapter.NewMessage(kafka.Message{
			Topic:     topic,
			Partition: 0,
			Offset:    0,
		}))

		// consumer should still be healthy before producing new message (no lag yet)
		assert.Eventually(t, func() bool {
			healthy, err := hc.Healthy(ctx)
			return err == nil && healthy
		}, 200*time.Millisecond, 20*time.Millisecond, "consumer should be healthy before new message")

		// produce a new message to make the consumer "lag behind"
		err = writer.WriteMessages(ctx, kafka.Message{
			Topic:     topic,
			Partition: 0,
			Value:     []byte("new message"),
		})
		require.NoError(t, err, "failed to produce new message to topic")

		// consumer should be unhealthy after broker updates watermark
		assert.Eventually(t, func() bool {
			healthy, err := hc.Healthy(ctx)
			return err == nil && !healthy
		}, 1*time.Second, 50*time.Millisecond)
	})

	t.Run("a running consumer should be unhealthy when it gets stuck", func(t *testing.T) {
		topic := fmt.Sprintf("live-consumer-topic-%d", time.Now().UnixNano())
		createTopic(topic, 1)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		hc, err := pulse.NewHealthChecker(
			pulse.Config{StuckTimeout: 100 * time.Millisecond},
			segmentioadapter.NewClientAdapter(brokers),
		)
		require.NoError(t, err)

		writer := &kafka.Writer{
			Addr:                   kafka.TCP(brokers[0]),
			Balancer:               &kafka.LeastBytes{},
			AllowAutoTopicCreation: true,
		}
		defer func(writer *kafka.Writer) {
			_ = writer.Close()
		}(writer)

		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers:   brokers,
			Topic:     topic,
			GroupID:   fmt.Sprintf("test-group-%s", topic),
			Partition: 0,
			MinBytes:  1,
			MaxBytes:  10e6,
		})
		defer func(reader *kafka.Reader) {
			_ = reader.Close()
		}(reader)

		messageProcessed := make(chan struct{})
		consumerError := make(chan error, 1)

		go func() {
			defer close(consumerError)
			// this consumer will read only ONE message and then get "stuck"
			msg, err := reader.ReadMessage(ctx)
			if err != nil {
				select {
				case consumerError <- err:
				default:
				}
				return
			}

			// use the health monitor inside the consumer loop
			hc.Track(ctx, segmentioadapter.NewMessage(msg))

			// signal to the main test that the first message has been processed
			select {
			case messageProcessed <- struct{}{}:
			case <-ctx.Done():
				return
			}

			// the consumer now stops reading, simulating being stuck
			<-ctx.Done()
		}()

		// produce the first message for our consumer to process
		err = writer.WriteMessages(ctx, kafka.Message{
			Topic:     topic,
			Partition: 0,
			Value:     []byte("first message"),
		})
		require.NoError(t, err)

		// wait until the consumer has processed and tracked the first message
		select {
		case <-messageProcessed: // success
		case err := <-consumerError:
			if err != nil {
				t.Fatalf("consumer error: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("timeout: consumer never processed the first message")
		}

		// at this point, the consumer should still be healthy even after StuckTimeout (no new messages yet)
		assert.Eventually(t, func() bool {
			healthy, err := hc.Healthy(ctx)
			return err == nil && healthy
		}, 200*time.Millisecond, 20*time.Millisecond, "consumer should still be healthy when no new messages")

		// produce a new message
		// the consumer is stuck and will NOT process this makes the consumer "lagging behind"
		err = writer.WriteMessages(ctx, kafka.Message{
			Topic:     topic,
			Partition: 0,
			Value:     []byte("second message"),
		})
		require.NoError(t, err)

		// the stuck consumer should be reported as unhealthy
		assert.Eventually(t, func() bool {
			healthy, err := hc.Healthy(ctx)
			return err == nil && !healthy
		}, 1*time.Second, 50*time.Millisecond, "the stuck consumer should be reported as unhealthy")
	})

	t.Run("should be healthy when idle but caught up", func(t *testing.T) {
		topic := "segmentio-idle-topic"
		createTopic(topic, 1)

		hc, _ := pulse.NewHealthChecker(pulse.Config{StuckTimeout: 100 * time.Millisecond}, segmentioadapter.NewClientAdapter(brokers))

		// produce and track a message, consumer is now caught up
		writer := &kafka.Writer{
			Addr:                   kafka.TCP(brokers[0]),
			Balancer:               &kafka.LeastBytes{},
			AllowAutoTopicCreation: true,
		}
		defer writer.Close()

		err := writer.WriteMessages(ctx, kafka.Message{
			Topic:     topic,
			Partition: 0,
			Value:     []byte("message"),
		})
		require.NoError(t, err, "failed to produce message to topic")

		hc.Track(ctx, segmentioadapter.NewMessage(kafka.Message{
			Topic:     topic,
			Partition: 0,
			Offset:    0,
		}))

		// still healthy because it's idle but not lagging (even after StuckTimeout)
		assert.Eventually(t, func() bool {
			healthy, err := hc.Healthy(ctx)
			return err == nil && healthy
		}, 1*time.Second, 50*time.Millisecond, "should be healthy when idle but caught up")
	})

	t.Run("multi-partition tracking - should be healthy when all partitions are caught up", func(t *testing.T) {
		topic := "segmentio-multi-partition-healthy-topic"
		numPartitions := 3
		createTopic(topic, numPartitions)

		hc, err := pulse.NewHealthChecker(
			pulse.Config{StuckTimeout: 100 * time.Millisecond},
			segmentioadapter.NewClientAdapter(brokers),
		)
		require.NoError(t, err)

		writer := &kafka.Writer{
			Addr:                   kafka.TCP(brokers[0]),
			Balancer:               &kafka.LeastBytes{},
			AllowAutoTopicCreation: true,
		}
		defer func(writer *kafka.Writer) {
			_ = writer.Close()
		}(writer)

		// produce messages to all partitions
		var messages []kafka.Message
		for partition := 0; partition < numPartitions; partition++ {
			messages = append(messages, kafka.Message{
				Topic:     topic,
				Partition: partition,
				Value:     []byte(fmt.Sprintf("message-p%d", partition)),
			})
		}
		err = writer.WriteMessages(ctx, messages...)
		require.NoError(t, err)

		// track messages from all partitions
		for partition := 0; partition < numPartitions; partition++ {
			hc.Track(ctx, segmentioadapter.NewMessage(kafka.Message{
				Topic:     topic,
				Partition: partition,
				Offset:    0,
			}))
		}

		healthy, err := hc.Healthy(ctx)
		assert.NoError(t, err)
		assert.True(t, healthy)
	})

	t.Run("multi-partition tracking - should be unhealthy when one partition is stuck", func(t *testing.T) {
		topic := "segmentio-multi-partition-stuck-topic"
		numPartitions := 3
		createTopic(topic, numPartitions)

		hc, err := pulse.NewHealthChecker(
			pulse.Config{StuckTimeout: 100 * time.Millisecond},
			segmentioadapter.NewClientAdapter(brokers),
		)
		require.NoError(t, err)

		writer := &kafka.Writer{
			Addr:                   kafka.TCP(brokers[0]),
			Balancer:               &kafka.LeastBytes{},
			AllowAutoTopicCreation: true,
		}
		defer writer.Close()

		// produce initial messages to all partitions
		var messages []kafka.Message
		for partition := 0; partition < numPartitions; partition++ {
			messages = append(messages, kafka.Message{
				Topic:     topic,
				Partition: partition,
				Value:     []byte(fmt.Sprintf("message-p%d", partition)),
			})
		}
		err = writer.WriteMessages(ctx, messages...)
		require.NoError(t, err)

		// track messages from all partitions
		for partition := 0; partition < numPartitions; partition++ {
			hc.Track(ctx, segmentioadapter.NewMessage(kafka.Message{
				Topic:     topic,
				Partition: partition,
				Offset:    0,
			}))
		}

		// all partitions should still be healthy before new message
		assert.Eventually(t, func() bool {
			healthy, err := hc.Healthy(ctx)
			return err == nil && healthy
		}, 200*time.Millisecond, 20*time.Millisecond, "all partitions should be healthy before new message")

		// produce a new message to partition 1 only (making it lag behind)
		err = writer.WriteMessages(ctx, kafka.Message{
			Topic:     topic,
			Partition: 1,
			Value:     []byte("new message"),
		})
		require.NoError(t, err)

		// should be unhealthy because partition 1 is lagging
		assert.Eventually(t, func() bool {
			healthy, err := hc.Healthy(ctx)
			return err == nil && !healthy
		}, 1*time.Second, 50*time.Millisecond, "should be unhealthy when one partition is stuck")
	})

	t.Run("multi-partition tracking - should handle mixed partition states correctly", func(t *testing.T) {
		topic := "segmentio-multi-partition-mixed-topic"
		createTopic(topic, 3)

		hc, err := pulse.NewHealthChecker(
			pulse.Config{StuckTimeout: 100 * time.Millisecond},
			segmentioadapter.NewClientAdapter(brokers),
		)
		require.NoError(t, err)

		writer := &kafka.Writer{
			Addr:                   kafka.TCP(brokers[0]),
			Balancer:               &kafka.LeastBytes{},
			AllowAutoTopicCreation: true,
		}
		defer writer.Close()

		// produce different numbers of messages to different partitions
		var messages []kafka.Message

		// partition 0: 1 message
		messages = append(messages, kafka.Message{
			Topic:     topic,
			Partition: 0,
			Value:     []byte("message-p0"),
		})

		// partition 1: 3 messages
		for i := 0; i < 3; i++ {
			messages = append(messages, kafka.Message{
				Topic:     topic,
				Partition: 1,
				Value:     []byte(fmt.Sprintf("message-p1-%d", i)),
			})
		}

		// partition 2: 2 messages
		for i := 0; i < 2; i++ {
			messages = append(messages, kafka.Message{
				Topic:     topic,
				Partition: 2,
				Value:     []byte(fmt.Sprintf("message-p2-%d", i)),
			})
		}

		err = writer.WriteMessages(ctx, messages...)
		require.NoError(t, err)

		// track latest messages from all partitions (all caught up)
		hc.Track(ctx, segmentioadapter.NewMessage(kafka.Message{
			Topic:     topic,
			Partition: 0,
			Offset:    0,
		}))
		hc.Track(ctx, segmentioadapter.NewMessage(kafka.Message{
			Topic:     topic,
			Partition: 1,
			Offset:    2,
		}))
		hc.Track(ctx, segmentioadapter.NewMessage(kafka.Message{
			Topic:     topic,
			Partition: 2,
			Offset:    1,
		}))

		// should be healthy - all partitions are caught up
		healthy, err := hc.Healthy(ctx)
		assert.NoError(t, err)
		assert.True(t, healthy)

		// now track an older message from partition 1, making it lag
		hc.Track(ctx, segmentioadapter.NewMessage(kafka.Message{
			Topic:     topic,
			Partition: 1,
			Offset:    0,
		}))

		// should be unhealthy because partition 1 is now lagging after timestamp becomes stale
		assert.Eventually(t, func() bool {
			healthy, err := hc.Healthy(ctx)
			return err == nil && !healthy
		}, 1*time.Second, 50*time.Millisecond, "should be unhealthy when partition 1 is lagging")
	})
}

func TestHealthCheckerIntegration_ConsumerGroup_WithSegmentioAdapter(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("consumer should track multiple partitions independently", func(t *testing.T) {
		topic := "segmentio-multi-partition-consumer-topic"
		createTopic(topic, 2)

		hc, err := pulse.NewHealthChecker(
			pulse.Config{StuckTimeout: 100 * time.Millisecond},
			segmentioadapter.NewClientAdapter(brokers),
		)
		require.NoError(t, err)

		// produce messages to multiple partitions
		writer := &kafka.Writer{
			Addr:                   kafka.TCP(brokers[0]),
			Balancer:               &kafka.LeastBytes{},
			AllowAutoTopicCreation: true,
		}
		defer writer.Close()

		var messages []kafka.Message
		for partition := 0; partition < 2; partition++ {
			for i := 0; i < 3; i++ {
				messages = append(messages, kafka.Message{
					Topic:     topic,
					Partition: partition,
					Value:     []byte(fmt.Sprintf("message-p%d-%d", partition, i)),
				})
			}
		}
		err = writer.WriteMessages(ctx, messages...)
		require.NoError(t, err)

		// consumer tracks messages from both partitions
		hc.Track(ctx, segmentioadapter.NewMessage(kafka.Message{
			Topic:     topic,
			Partition: 0,
			Offset:    2,
		}))

		hc.Track(ctx, segmentioadapter.NewMessage(kafka.Message{
			Topic:     topic,
			Partition: 1,
			Offset:    2,
		}))

		// consumer should be healthy when caught up on all assigned partitions
		healthy, err := hc.Healthy(ctx)
		assert.NoError(t, err)
		assert.True(t, healthy, "consumer should be healthy when caught up on all partitions")
	})

	t.Run("consumer group rebalancing scenario - should handle partition reassignment", func(t *testing.T) {
		topic := "segmentio-rebalance-topic"
		createTopic(topic, 3)

		hc, err := pulse.NewHealthChecker(
			pulse.Config{StuckTimeout: 100 * time.Millisecond},
			segmentioadapter.NewClientAdapter(brokers),
		)
		require.NoError(t, err)

		// produce messages to multiple partitions
		writer := &kafka.Writer{
			Addr:                   kafka.TCP(brokers[0]),
			Balancer:               &kafka.LeastBytes{},
			AllowAutoTopicCreation: true,
		}
		defer writer.Close()

		var messages []kafka.Message
		for partition := 0; partition < 3; partition++ {
			for i := 0; i < 2; i++ {
				messages = append(messages, kafka.Message{
					Topic:     topic,
					Partition: partition,
					Value:     []byte(fmt.Sprintf("message-p%d-%d", partition, i)),
				})
			}
		}
		err = writer.WriteMessages(ctx, messages...)
		require.NoError(t, err)

		// Check what the actual latest offsets are for each partition
		adapter := segmentioadapter.NewClientAdapter(brokers)

		// Wait for messages to be committed by checking offsets
		assert.Eventually(t, func() bool {
			for p := int32(0); p < 3; p++ {
				latestOffset, err := adapter.GetLatestOffset(ctx, topic, p)
				if err != nil {
					return false
				}
				if latestOffset < 0 {
					return false
				}
			}
			return true
		}, 2*time.Second, 100*time.Millisecond, "messages should be committed to all partitions")
		for p := int32(0); p < 3; p++ {
			latestOffset, err := adapter.GetLatestOffset(ctx, topic, p)
			require.NoError(t, err)
			t.Logf("Partition %d latest offset: %d", p, latestOffset)

			// Track the latest message for each partition
			hc.Track(ctx, segmentioadapter.NewMessage(kafka.Message{
				Topic:     topic,
				Partition: int(p),
				Offset:    latestOffset,
			}))
		}

		// consumer should be healthy
		healthy, err := hc.Healthy(ctx)
		assert.NoError(t, err)
		assert.True(t, healthy)

		// simulate rebalancing by releasing some partitions
		hc.Release(ctx, topic, 1)
		hc.Release(ctx, topic, 2)

		// Update partition 0 tracking to ensure it stays current
		latestOffset0, err := adapter.GetLatestOffset(ctx, topic, 0)
		require.NoError(t, err)
		hc.Track(ctx, segmentioadapter.NewMessage(kafka.Message{
			Topic:     topic,
			Partition: 0,
			Offset:    latestOffset0,
		}))

		// consumer should still be healthy for remaining partition
		assert.Eventually(t, func() bool {
			healthy, err := hc.Healthy(ctx)
			return err == nil && healthy
		}, 500*time.Millisecond, 25*time.Millisecond, "consumer should be healthy for remaining partition")

		// produce new messages to the released partitions
		err = writer.WriteMessages(ctx,
			kafka.Message{
				Topic:     topic,
				Partition: 1,
				Value:     []byte("new message 1"),
			},
			kafka.Message{
				Topic:     topic,
				Partition: 1,
				Value:     []byte("new message 2"),
			},
		)
		require.NoError(t, err)

		// Update partition 0 tracking again to ensure it stays current for final check
		// Wait for new messages to be committed by checking offset updates
		assert.Eventually(t, func() bool {
			var err error
			latestOffset0, err = adapter.GetLatestOffset(ctx, topic, 0)
			return err == nil && latestOffset0 >= 0
		}, 1*time.Second, 50*time.Millisecond, "messages should be committed to partition 0")
		hc.Track(ctx, segmentioadapter.NewMessage(kafka.Message{
			Topic:     topic,
			Partition: 0,
			Offset:    latestOffset0,
		}))

		// consumer should still be healthy since it's not tracking the released partitions
		healthy, err = hc.Healthy(ctx)
		assert.NoError(t, err)
		if !healthy {
			t.Logf("Health check failed unexpectedly - consumer should be healthy after releasing partitions 1 and 2")
		}
		assert.True(t, healthy)
	})

	t.Run("backpressure scenario - slow consumer should be detected as unhealthy", func(t *testing.T) {
		topic := "segmentio-backpressure-topic"
		createTopic(topic, 1)

		hc, err := pulse.NewHealthChecker(
			pulse.Config{StuckTimeout: 100 * time.Millisecond},
			segmentioadapter.NewClientAdapter(brokers),
		)
		require.NoError(t, err)

		writer := &kafka.Writer{
			Addr:                   kafka.TCP(brokers[0]),
			Balancer:               &kafka.LeastBytes{},
			AllowAutoTopicCreation: true,
		}
		defer writer.Close()

		messageCount := 100
		var messages []kafka.Message
		for i := 0; i < messageCount; i++ {
			messages = append(messages, kafka.Message{
				Topic:     topic,
				Partition: 0,
				Key:       []byte(fmt.Sprintf("key-%d", i)),
				Value:     []byte(fmt.Sprintf("message-%d", i)),
			})
		}
		err = writer.WriteMessages(ctx, messages...)
		require.NoError(t, err)

		// simulate slow consumer that only processes a few messages
		slowConsumerOffset := int64(5) // only processed 6 messages out of 100
		hc.Track(ctx, segmentioadapter.NewMessage(kafka.Message{
			Topic:     topic,
			Partition: 0,
			Offset:    slowConsumerOffset,
		}))

		// consumer should be unhealthy due to significant lag after timestamp becomes stale
		assert.Eventually(t, func() bool {
			healthy, err := hc.Healthy(ctx)
			return err == nil && !healthy
		}, 1*time.Second, 50*time.Millisecond, "slow consumer should be unhealthy due to backpressure")

		// now simulate consumer catching up
		catchUpOffset := int64(messageCount - 1) // caught up to last message
		hc.Track(ctx, segmentioadapter.NewMessage(kafka.Message{
			Topic:     topic,
			Partition: 0,
			Offset:    catchUpOffset,
		}))

		// consumer should now be healthy
		healthy, err := hc.Healthy(ctx)
		assert.NoError(t, err)
		assert.True(t, healthy, "consumer should be healthy after catching up")
	})
}
