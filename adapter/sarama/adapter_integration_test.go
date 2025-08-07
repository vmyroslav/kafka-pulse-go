package sarama

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	kafkacontainer "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/vmyroslav/kafka-pulse-go/pulse"
)

var (
	dockerImage  = "confluentinc/confluent-local:7.8.3"
	brokers      []string
	saramaClient sarama.Client

	saramaConfigVersion = sarama.V3_8_1_0
)

func TestMain(m *testing.M) {
	ctx := context.Background()

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

	config := sarama.NewConfig()
	config.Version = saramaConfigVersion
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll

	saramaClient, err = sarama.NewClient(brokers, config)
	if err != nil {
		log.Fatal(err)
	}

	code := m.Run()

	if saramaClient != nil {
		if err = saramaClient.Close(); err != nil {
			log.Printf("failed to close sarama client: %s", err)
		}
	}

	if err = testcontainers.TerminateContainer(kafkaContainer); err != nil {
		log.Printf("failed to terminate container: %s", err)
	}

	os.Exit(code)
}

func TestClientAdapterIntegration_Implementation(t *testing.T) {
	t.Parallel()
	var (
		ctx            = context.Background()
		topicSingleMsg = "sarama-topic-single-message"
		topicMultiPart = "sarama-topic-multi-partition"
	)

	createTopic(t, topicSingleMsg, 1)
	createTopic(t, topicMultiPart, 3)

	producer, err := sarama.NewSyncProducer(brokers, saramaClient.Config())
	require.NoError(t, err, "Failed to create producer")
	defer func() {
		if err := producer.Close(); err != nil {
			log.Printf("Failed to close producer: %v", err)
		}
	}()

	// produce 1 message to the single-message topic partition 0
	_, _, err = producer.SendMessage(&sarama.ProducerMessage{
		Topic:     topicSingleMsg,
		Partition: 0,
		Value:     sarama.StringEncoder("test message"),
	})
	require.NoError(t, err, "Failed to produce message to topicSingleMsg")

	// produce 5 messages to topicMultiPart - let Sarama choose partitions
	for i := 0; i < 5; i++ {
		_, _, err = producer.SendMessage(&sarama.ProducerMessage{
			Topic: topicMultiPart,
			Key:   sarama.StringEncoder(fmt.Sprintf("key-%d", i)),
			Value: sarama.StringEncoder(fmt.Sprintf("message-%d", i)),
		})
		require.NoError(t, err, "Failed to produce message to topicMultiPart")
	}

	adapter := NewClientAdapter(saramaClient)

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
		}, 2*time.Second, 100*time.Millisecond, "expected at least one partition to have messages")
	})

	t.Run("handles non-existent partition gracefully", func(t *testing.T) {
		_, err = adapter.GetLatestOffset(ctx, topicSingleMsg, 999)
		require.Error(t, err, "expected error for non-existent partition")
	})
}

func TestHealthCheckerIntegration_WithClientAdapter(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("should be unhealthy when stale and lagging behind", func(t *testing.T) {
		topic := "sarama-stuck-topic"
		createTopic(t, topic, 1)

		hc := createHealthChecker(t)

		producer, err := sarama.NewSyncProducer(brokers, saramaClient.Config())
		require.NoError(t, err, "failed to create producer")
		defer func(producer sarama.SyncProducer) {
			_ = producer.Close()
		}(producer)

		// produce messages to create lag
		messageCount := 10
		for i := 0; i < messageCount; i++ {
			_, _, err = producer.SendMessage(&sarama.ProducerMessage{
				Topic:     topic,
				Partition: 0,
				Value:     sarama.StringEncoder(fmt.Sprintf("message-%d", i)),
			})
			require.NoError(t, err, "failed to produce message to topic")
		}

		// consumer tracks an older message (behind)
		hc.Track(ctx, NewMessage(&sarama.ConsumerMessage{
			Topic:     topic,
			Partition: 0,
			Offset:    1, // only processed second message
		}))

		// wait for messages to be committed and consumer to be detected as unhealthy
		assert.Eventually(t, func() bool {
			healthy, err := hc.Healthy(ctx)
			if err != nil {
				return false
			}

			return !healthy // we want unhealthy
		}, 3*time.Second, 200*time.Millisecond, "consumer should be unhealthy when lagging behind")
	})

	t.Run("stuck consumer simulation - should be unhealthy when lagging significantly", func(t *testing.T) {
		topic := fmt.Sprintf("sarama-stuck-simulation-%d", time.Now().UnixNano())
		createTopic(t, topic, 1)

		hc := createHealthChecker(t)

		producer, err := sarama.NewSyncProducer(brokers, saramaClient.Config())
		require.NoError(t, err)
		defer producer.Close()

		// produce many messages to create a backlog
		messageCount := 50
		for i := 0; i < messageCount; i++ {
			_, _, err = producer.SendMessage(&sarama.ProducerMessage{
				Topic:     topic,
				Partition: 0,
				Value:     sarama.StringEncoder(fmt.Sprintf("message-%d", i)),
			})
			require.NoError(t, err)
		}

		// simulate a stuck consumer that only processed the first few messages
		hc.Track(ctx, NewMessage(&sarama.ConsumerMessage{
			Topic:     topic,
			Partition: 0,
			Offset:    2, // only processed 3 messages out of 50
		}))

		// consumer should be unhealthy due to significant lag
		assert.Eventually(t, func() bool {
			healthy, err := hc.Healthy(ctx)
			return err == nil && !healthy
		}, 2*time.Second, 100*time.Millisecond, "consumer should be unhealthy when significantly lagging")
	})

	t.Run("should be healthy when idle but caught up", func(t *testing.T) {
		topic := "sarama-idle-topic"
		createTopic(t, topic, 1)

		hc := createHealthChecker(t)

		// produce and track a message, consumer is now caught up
		producer, err := sarama.NewSyncProducer(brokers, saramaClient.Config())
		require.NoError(t, err)
		defer producer.Close()

		_, _, err = producer.SendMessage(&sarama.ProducerMessage{
			Topic:     topic,
			Partition: 0,
			Value:     sarama.StringEncoder("message"),
		})
		require.NoError(t, err, "failed to produce message to topic")

		hc.Track(ctx, NewMessage(&sarama.ConsumerMessage{
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
		topic := "sarama-multi-partition-healthy-topic"
		numPartitions := int32(3)
		createTopic(t, topic, numPartitions)

		hc := createHealthChecker(t)

		producer, err := sarama.NewSyncProducer(brokers, saramaClient.Config())
		require.NoError(t, err)
		defer producer.Close()

		// produce messages to all partitions
		for partition := int32(0); partition < numPartitions; partition++ {
			_, _, err = producer.SendMessage(&sarama.ProducerMessage{
				Topic:     topic,
				Partition: partition,
				Value:     sarama.StringEncoder(fmt.Sprintf("message-p%d", partition)),
			})
			require.NoError(t, err)
		}

		// track messages from all partitions
		for partition := int32(0); partition < numPartitions; partition++ {
			hc.Track(ctx, NewMessage(&sarama.ConsumerMessage{
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
		topic := "sarama-multi-partition-stuck"
		numPartitions := int32(3)
		createTopic(t, topic, numPartitions)

		hc := createHealthChecker(t)

		producer, err := sarama.NewSyncProducer(brokers, saramaClient.Config())
		require.NoError(t, err)
		defer producer.Close()

		// produce many messages to partition 1 to create backlog
		for i := 0; i < 20; i++ {
			_, _, err = producer.SendMessage(&sarama.ProducerMessage{
				Topic:     topic,
				Partition: 1,
				Value:     sarama.StringEncoder(fmt.Sprintf("message-p1-%d", i)),
			})
			require.NoError(t, err)
		}

		// track caught-up messages from partitions 0 and 2 (no messages produced)
		hc.Track(ctx, NewMessage(&sarama.ConsumerMessage{
			Topic:     topic,
			Partition: 0,
			Offset:    -1, // no messages in this partition, caught up
		}))
		hc.Track(ctx, NewMessage(&sarama.ConsumerMessage{
			Topic:     topic,
			Partition: 2,
			Offset:    -1, // no messages in this partition, caught up
		}))

		// track lagging message from partition 1 (only processed first few messages)
		hc.Track(ctx, NewMessage(&sarama.ConsumerMessage{
			Topic:     topic,
			Partition: 1,
			Offset:    2, // only processed 3 messages out of 20
		}))

		// should be unhealthy because partition 1 is lagging
		assert.Eventually(t, func() bool {
			healthy, err := hc.Healthy(ctx)
			return err == nil && !healthy
		}, 2*time.Second, 100*time.Millisecond, "should be unhealthy when one partition is stuck")
	})
}

func TestHealthCheckerIntegration_RealConsumerGroup_WithSaramaAdapter(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("real consumer group processing messages with health monitoring", func(t *testing.T) {
		topic := fmt.Sprintf("sarama-real-consumer-group-%d", time.Now().UnixNano())
		createTopic(t, topic, 2)

		hc := createHealthChecker(t)

		producer, err := sarama.NewSyncProducer(brokers, saramaClient.Config())
		require.NoError(t, err)
		defer func(producer sarama.SyncProducer) {
			_ = producer.Close()
		}(producer)

		config := sarama.NewConfig()
		config.Version = saramaConfigVersion
		config.Consumer.Return.Errors = true
		config.Consumer.Offsets.Initial = sarama.OffsetOldest

		groupID := fmt.Sprintf("real-test-group-%s", topic)
		consumerGroup, err := sarama.NewConsumerGroup(brokers, groupID, config)
		require.NoError(t, err)
		defer func(consumerGroup sarama.ConsumerGroup) {
			_ = consumerGroup.Close()
		}(consumerGroup)

		handler := &realConsumerGroupHandler{
			hc:        hc,
			ctx:       ctx,
			processed: make(chan int, 10),
			t:         t,
		}

		consumerCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()

		go func() {
			err = consumerGroup.Consume(consumerCtx, []string{topic}, handler)
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Fatalf("failed to consume messages: %v", err)
			}
		}()

		// produce messages to multiple partitions
		var expectedCount int
		for partition := int32(0); partition < 2; partition++ {
			for i := 0; i < 3; i++ {
				_, _, err = producer.SendMessage(&sarama.ProducerMessage{
					Topic:     topic,
					Partition: partition,
					Key:       sarama.StringEncoder(fmt.Sprintf("key-p%d-%d", partition, i)),
					Value:     sarama.StringEncoder(fmt.Sprintf("message-p%d-%d", partition, i)),
				})
				require.NoError(t, err)
				expectedCount++
			}
		}

		// wait for all messages to be processed
		processedCount := 0
		timeout := time.After(8 * time.Second)
		for processedCount < expectedCount {
			select {
			case <-handler.processed:
				processedCount++
				t.Logf("Processed message %d/%d", processedCount, expectedCount)
			case <-timeout:
				t.Fatalf("Timeout: only processed %d/%d messages", processedCount, expectedCount)
			}
		}

		require.Equal(t, expectedCount, processedCount, "should process all produced messages")

		healthy, err := hc.Healthy(ctx)
		assert.NoError(t, err)
		assert.True(t, healthy, "consumer should be healthy after processing all messages")
	})
}

// realConsumerGroupHandler processes all messages and tracks them with health checker
type realConsumerGroupHandler struct {
	hc        *pulse.HealthChecker
	ctx       context.Context
	processed chan int
	t         *testing.T
}

func (h *realConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *realConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *realConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message := <-claim.Messages():
			if message == nil {
				return nil
			}

			// track each message with health checker
			h.hc.Track(h.ctx, NewMessage(message))
			h.t.Logf("Tracked message from partition %d offset %d", message.Partition, message.Offset)

			session.MarkMessage(message, "")

			// signal that a message was processed
			select {
			case h.processed <- 1:
			default:
			}

		case <-session.Context().Done():
			return nil
		}
	}
}

func TestHealthCheckerIntegration_ConsumerGroup_WithSaramaAdapter(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("consumer should track multiple partitions independently", func(t *testing.T) {
		topic := "sarama-multi-partition-consumer-topic"
		createTopic(t, topic, 2)

		hc := createHealthChecker(t)

		// produce messages to multiple partitions
		producer, err := sarama.NewSyncProducer(brokers, saramaClient.Config())
		require.NoError(t, err)
		defer func(producer sarama.SyncProducer) {
			_ = producer.Close()
		}(producer)

		for partition := int32(0); partition < 2; partition++ {
			for i := 0; i < 3; i++ {
				_, _, err = producer.SendMessage(&sarama.ProducerMessage{
					Topic:     topic,
					Partition: partition,
					Value:     sarama.StringEncoder(fmt.Sprintf("message-p%d-%d", partition, i)),
				})
				require.NoError(t, err)
			}
		}

		// consumer tracks messages from both partitions
		hc.Track(ctx, NewMessage(&sarama.ConsumerMessage{
			Topic:     topic,
			Partition: 0,
			Offset:    2,
		}))

		hc.Track(ctx, NewMessage(&sarama.ConsumerMessage{
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
		topic := "sarama-rebalance-topic"
		createTopic(t, topic, 3)

		hc := createHealthChecker(t)

		// produce messages to multiple partitions
		producer, err := sarama.NewSyncProducer(brokers, saramaClient.Config())
		require.NoError(t, err)
		defer func(producer sarama.SyncProducer) {
			_ = producer.Close()
		}(producer)

		for partition := int32(0); partition < 3; partition++ {
			for i := 0; i < 2; i++ {
				_, _, err = producer.SendMessage(&sarama.ProducerMessage{
					Topic:     topic,
					Partition: partition,
					Value:     sarama.StringEncoder(fmt.Sprintf("message-p%d-%d", partition, i)),
				})
				require.NoError(t, err)
			}
		}

		// check what the actual latest offsets are for each partition
		adapter := NewClientAdapter(saramaClient)

		// wait at least some partitions have messages
		assert.Eventually(t, func() bool {
			totalMessages := int64(0)
			for p := int32(0); p < 3; p++ {
				latestOffset, err := adapter.GetLatestOffset(ctx, topic, p)
				if err != nil {
					continue // Skip partitions with errors
				}
				if latestOffset >= 0 {
					totalMessages += latestOffset + 1 // +1 because offset is 0-based
				}
			}

			return totalMessages >= 6
		}, 3*time.Second, 100*time.Millisecond, "expected total messages should be committed")
		// track only partitions that actually have messages
		var trackedPartitions []int32
		for p := int32(0); p < 3; p++ {
			latestOffset, err := adapter.GetLatestOffset(ctx, topic, p)
			require.NoError(t, err)

			// only track partitions that have messages (offset >= 0)
			if latestOffset >= 0 {
				trackedPartitions = append(trackedPartitions, p)
				hc.Track(ctx, NewMessage(&sarama.ConsumerMessage{
					Topic:     topic,
					Partition: p,
					Offset:    latestOffset,
				}))
			}
		}

		// consumer should be healthy
		healthy, err := hc.Healthy(ctx)
		assert.NoError(t, err)
		assert.True(t, healthy)

		// simulate rebalancing by releasing some partitions
		if len(trackedPartitions) > 1 {
			for i := 1; i < len(trackedPartitions); i++ {
				hc.Release(ctx, topic, trackedPartitions[i])
			}
		}

		// update the remaining partition tracking to ensure it stays current
		if len(trackedPartitions) > 0 {
			remainingPartition := trackedPartitions[0]
			latestOffset, err := adapter.GetLatestOffset(ctx, topic, remainingPartition)
			require.NoError(t, err)
			hc.Track(ctx, NewMessage(&sarama.ConsumerMessage{
				Topic:     topic,
				Partition: remainingPartition,
				Offset:    latestOffset,
			}))
		}

		// consumer should still be healthy for remaining partition
		assert.Eventually(t, func() bool {
			healthy, err := hc.Healthy(ctx)
			return err == nil && healthy
		}, 500*time.Millisecond, 25*time.Millisecond, "consumer should be healthy for remaining partition")

		// produce new messages to any partition (let Sarama decide)
		_, _, err = producer.SendMessage(&sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder("new message 1"),
		})
		require.NoError(t, err)

		_, _, err = producer.SendMessage(&sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder("new message 2"),
		})
		require.NoError(t, err)

		if len(trackedPartitions) > 0 {
			remainingPartition := trackedPartitions[0]
			// wait for new messages to be committed
			assert.Eventually(t, func() bool {
				latestOffset, err := adapter.GetLatestOffset(ctx, topic, remainingPartition)
				return err == nil && latestOffset >= 0
			}, 1*time.Second, 50*time.Millisecond, "messages should be committed to remaining partition")

			latestOffset, err := adapter.GetLatestOffset(ctx, topic, remainingPartition)
			require.NoError(t, err)
			hc.Track(ctx, NewMessage(&sarama.ConsumerMessage{
				Topic:     topic,
				Partition: remainingPartition,
				Offset:    latestOffset,
			}))
		}

		// consumer should still be healthy since it's not tracking the released partitions
		healthy, err = hc.Healthy(ctx)
		assert.NoError(t, err)
		assert.True(t, healthy)
	})

	t.Run("backpressure scenario - slow consumer should be detected as unhealthy", func(t *testing.T) {
		topic := "sarama-backpressure-topic"
		createTopic(t, topic, 1)

		hc := createHealthChecker(t)

		producer, err := sarama.NewSyncProducer(brokers, saramaClient.Config())
		require.NoError(t, err)
		defer func(producer sarama.SyncProducer) {
			_ = producer.Close()
		}(producer)

		messageCount := 100
		for i := 0; i < messageCount; i++ {
			_, _, err = producer.SendMessage(&sarama.ProducerMessage{
				Topic: topic,
				Key:   sarama.StringEncoder(fmt.Sprintf("key-%d", i)),
				Value: sarama.StringEncoder(fmt.Sprintf("message-%d", i)),
			})
			require.NoError(t, err)
		}

		// simulate slow consumer that only processes a few messages
		slowConsumerOffset := int64(5) // only processed 6 messages out of 100
		hc.Track(ctx, NewMessage(&sarama.ConsumerMessage{
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
		hc.Track(ctx, NewMessage(&sarama.ConsumerMessage{
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

func TestHealthCheckerIntegration_Concurrent_WithSaramaAdapter(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("concurrent message tracking should be thread-safe", func(t *testing.T) {
		topic := "sarama-concurrent-tracking-topic"
		numPartitions := int32(4)
		numGoroutines := 10
		messagesPerGoroutine := 20
		createTopic(t, topic, numPartitions)

		hc, err := pulse.NewHealthChecker(
			pulse.Config{StuckTimeout: 200 * time.Millisecond},
			NewClientAdapter(saramaClient),
		)
		require.NoError(t, err)

		// produce messages to multiple partitions
		producer, err := sarama.NewSyncProducer(brokers, saramaClient.Config())
		require.NoError(t, err)
		defer producer.Close()

		for partition := int32(0); partition < numPartitions; partition++ {
			for i := 0; i < messagesPerGoroutine; i++ {
				_, _, err = producer.SendMessage(&sarama.ProducerMessage{
					Topic:     topic,
					Partition: partition,
					Value:     sarama.StringEncoder(fmt.Sprintf("message-p%d-%d", partition, i)),
				})
				require.NoError(t, err)
			}
		}

		// start multiple goroutines to track messages concurrently
		var wg sync.WaitGroup
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(goroutineID int) {
				defer wg.Done()
				partition := int32(goroutineID % int(numPartitions))

				for msgIdx := 0; msgIdx < messagesPerGoroutine; msgIdx++ {
					hc.Track(ctx, NewMessage(&sarama.ConsumerMessage{
						Topic:     topic,
						Partition: partition,
						Offset:    int64(msgIdx),
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
		topic := "sarama-concurrent-health-topic"
		numHealthChecks := 50
		createTopic(t, topic, 1)

		hc := createHealthChecker(t)

		// produce and track a message
		producer, err := sarama.NewSyncProducer(brokers, saramaClient.Config())
		require.NoError(t, err)
		defer producer.Close()

		_, _, err = producer.SendMessage(&sarama.ProducerMessage{
			Topic:     topic,
			Partition: 0,
			Value:     sarama.StringEncoder("test message"),
		})
		require.NoError(t, err)

		hc.Track(ctx, NewMessage(&sarama.ConsumerMessage{
			Topic:     topic,
			Partition: 0,
			Offset:    0,
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

		// Check that no errors occurred
		for err := range errCh {
			assert.NoError(t, err)
		}

		// all should be healthy
		assert.Equal(t, numHealthChecks, healthyCount)
	})
}

// TestSaramaClientAdapter_OffsetBehavior verifies that the Sarama client adapter
// correctly calculates the offset of the last existing message
func TestClientAdapter_GetLatestOffset_Integration(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("GetLatestOffset with empty topic should return -1", func(t *testing.T) {
		topic := fmt.Sprintf("empty-topic-%d", time.Now().UnixNano())
		createTopic(t, topic, 1)

		adapter := NewClientAdapter(saramaClient)

		latestOffset, err := adapter.GetLatestOffset(ctx, topic, 0)
		assert.NoError(t, err)
		assert.Equal(t, int64(-1), latestOffset, "empty topic should return -1 as latest offset")
	})

	t.Run("GetLatestOffset with single message", func(t *testing.T) {
		topic := fmt.Sprintf("single-msg-topic-%d", time.Now().UnixNano())
		createTopic(t, topic, 1)

		adapter := NewClientAdapter(saramaClient)

		producer, err := sarama.NewSyncProducer(brokers, saramaClient.Config())
		require.NoError(t, err)
		defer func(producer sarama.SyncProducer) {
			_ = producer.Close()
		}(producer)

		partition, offset, err := producer.SendMessage(&sarama.ProducerMessage{
			Topic:     topic,
			Partition: 0,
			Value:     sarama.StringEncoder("test message"),
		})
		require.NoError(t, err)
		require.Equal(t, int32(0), partition)
		require.Equal(t, int64(0), offset)

		latestOffset, err := adapter.GetLatestOffset(ctx, topic, 0)
		assert.NoError(t, err)
		assert.Equal(t, int64(0), latestOffset)
	})

	t.Run("GetLatestOffset with multiple messages", func(t *testing.T) {
		topic := fmt.Sprintf("multi-msg-topic-%d", time.Now().UnixNano())
		createTopic(t, topic, 1)

		adapter := NewClientAdapter(saramaClient)

		producer, err := sarama.NewSyncProducer(brokers, saramaClient.Config())
		require.NoError(t, err)
		defer func(producer sarama.SyncProducer) {
			_ = producer.Close()
		}(producer)

		messageCount := 5
		for i := 0; i < messageCount; i++ {
			partition, offset, err := producer.SendMessage(&sarama.ProducerMessage{
				Topic:     topic,
				Partition: 0,
				Value:     sarama.StringEncoder(fmt.Sprintf("message-%d", i)),
			})
			require.NoError(t, err)
			require.Equal(t, int32(0), partition)
			require.Equal(t, int64(i), offset, "message %d should have offset %d", i, i)
		}

		// latest offset should be 4 (last message offset)
		latestOffset, err := adapter.GetLatestOffset(ctx, topic, 0)
		assert.NoError(t, err)
		assert.Equal(t, int64(4), latestOffset, "should return offset of last produced message")
	})

	t.Run("GetLatestOffset with multiple partitions", func(t *testing.T) {
		topic := fmt.Sprintf("multi-partition-topic-%d", time.Now().UnixNano())
		numPartitions := int32(3)
		createTopic(t, topic, numPartitions)

		adapter := NewClientAdapter(saramaClient)

		producer, err := sarama.NewSyncProducer(brokers, saramaClient.Config())
		require.NoError(t, err)
		defer func(producer sarama.SyncProducer) {
			_ = producer.Close()
		}(producer)

		// produce multiple messages and let Kafka distribute them across partitions
		producedMessages := 10
		producedOffsets := make(map[int32][]int64)
		for i := 0; i < producedMessages; i++ {
			producedPartition, offset, err := producer.SendMessage(&sarama.ProducerMessage{
				Topic: topic,
				Key:   sarama.StringEncoder(fmt.Sprintf("key-%d", i)),
				Value: sarama.StringEncoder(fmt.Sprintf("message-%d", i)),
			})
			require.NoError(t, err)
			producedOffsets[producedPartition] = append(producedOffsets[producedPartition], offset)
		}

		partitionsWithMessages := 0
		for partition := int32(0); partition < numPartitions; partition++ {
			offsets := producedOffsets[partition]
			if len(offsets) > 0 {
				partitionsWithMessages++
				expectedLatestOffset := offsets[len(offsets)-1] // Last produced offset

				assert.Eventually(t, func() bool {
					latestOffset, err := adapter.GetLatestOffset(ctx, topic, partition)
					if err != nil {
						t.Logf("Error getting offset for partition %d: %v", partition, err)
						return false
					}
					return latestOffset == expectedLatestOffset
				}, 3*time.Second, 100*time.Millisecond,
					"partition %d should have latest offset %d", partition, expectedLatestOffset)
			} else {
				latestOffset, err := adapter.GetLatestOffset(ctx, topic, partition)
				assert.NoError(t, err)
				assert.Equal(t, int64(-1), latestOffset, "Empty partition should return -1")
			}
		}

		assert.Greater(t, partitionsWithMessages, 0, "At least one partition should have received messages")
	})

	t.Run("GetLatestOffset error handling for non-existent partition", func(t *testing.T) {
		topic := fmt.Sprintf("error-test-topic-%d", time.Now().UnixNano())
		createTopic(t, topic, 2) // Only 2 partitions

		adapter := NewClientAdapter(saramaClient)

		_, err := adapter.GetLatestOffset(ctx, topic, 999)
		assert.Error(t, err, "Should return error for non-existent partition")
	})

	t.Run("GetLatestOffset error handling for non-existent topic", func(t *testing.T) {
		nonExistentTopic := fmt.Sprintf("definitely-non-existent-topic-with-special-chars-!@#$%%^&*()-%d", time.Now().UnixNano())

		adapter := NewClientAdapter(saramaClient)

		_, err := adapter.GetLatestOffset(ctx, nonExistentTopic, 0)
		if err == nil {
			t.Skip("Kafka cluster auto-creates topics - skipping non-existent topic test")
		} else {
			assert.Error(t, err, "should return error for non-existent topic")
			errorMsg := strings.ToLower(err.Error())
			if !strings.Contains(errorMsg, "unknown") && !strings.Contains(errorMsg, "invalid") {
				t.Logf("Unexpected error message: %s", err.Error())
			}
			assert.NotNil(t, err, "should get an error for non-existent topic")
		}
	})

	t.Run("GetLatestOffset consistency with high water mark behavior", func(t *testing.T) {
		topic := fmt.Sprintf("watermark-consistency-topic-%d", time.Now().UnixNano())
		createTopic(t, topic, 1)

		adapter := NewClientAdapter(saramaClient)

		highWaterMark, err := saramaClient.GetOffset(topic, 0, sarama.OffsetNewest)
		require.NoError(t, err)
		assert.Equal(t, int64(0), highWaterMark, "empty topic should have high water mark 0")

		latestOffset, err := adapter.GetLatestOffset(ctx, topic, 0)
		assert.NoError(t, err)
		assert.Equal(t, highWaterMark-1, latestOffset, "latest offset should be high water mark - 1")
		assert.Equal(t, int64(-1), latestOffset)

		producer, err := sarama.NewSyncProducer(brokers, saramaClient.Config())
		require.NoError(t, err)
		defer func(producer sarama.SyncProducer) {
			_ = producer.Close()
		}(producer)

		_, _, err = producer.SendMessage(&sarama.ProducerMessage{
			Topic:     topic,
			Partition: 0,
			Value:     sarama.StringEncoder("message"),
		})
		require.NoError(t, err)

		highWaterMark, err = saramaClient.GetOffset(topic, 0, sarama.OffsetNewest)
		require.NoError(t, err)
		assert.Equal(t, int64(1), highWaterMark, "topic with one message should have high water mark 1")

		latestOffset, err = adapter.GetLatestOffset(ctx, topic, 0)
		assert.NoError(t, err)
		assert.Equal(t, highWaterMark-1, latestOffset, "latest offset should be high water mark - 1")
		assert.Equal(t, int64(0), latestOffset)
	})
}

func TestSaramaClientAdapter_OffsetBehavior(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	topic := fmt.Sprintf("offset-behavior-test-%d", time.Now().UnixNano())

	createTopic(t, topic, 1)

	adapter := NewClientAdapter(saramaClient)

	t.Run("empty topic should return -1", func(t *testing.T) {
		// for an empty topic, high watermark is 0, so latest message offset should be -1
		latestOffset, err := adapter.GetLatestOffset(ctx, topic, 0)
		assert.NoError(t, err)
		assert.Equal(t, int64(-1), latestOffset, "empty topic should return -1 as latest message offset")
	})

	t.Run("topic with one message should return 0", func(t *testing.T) {
		producer, err := sarama.NewSyncProducer(brokers, saramaClient.Config())
		require.NoError(t, err)
		defer func(producer sarama.SyncProducer) {
			_ = producer.Close()
		}(producer)

		partition, offset, err := producer.SendMessage(&sarama.ProducerMessage{
			Topic:     topic,
			Partition: 0,
			Value:     sarama.StringEncoder("first message"),
		})
		require.NoError(t, err)
		require.Equal(t, int32(0), partition)
		require.Equal(t, int64(0), offset, "first message should have offset 0")

		latestOffset, err := adapter.GetLatestOffset(ctx, topic, 0)
		assert.NoError(t, err)
		assert.Equal(t, int64(0), latestOffset, "topic with one message should return 0 as latest message offset")
	})

	t.Run("topic with multiple messages should return correct latest offset", func(t *testing.T) {
		producer, err := sarama.NewSyncProducer(brokers, saramaClient.Config())
		require.NoError(t, err)
		defer func(producer sarama.SyncProducer) {
			_ = producer.Close()
		}(producer)

		var lastProducedOffset int64
		for i := 1; i < 5; i++ {
			partition, offset, err := producer.SendMessage(&sarama.ProducerMessage{
				Topic:     topic,
				Partition: 0,
				Value:     sarama.StringEncoder(fmt.Sprintf("message-%d", i)),
			})
			require.NoError(t, err)
			require.Equal(t, int32(0), partition)
			require.Equal(t, int64(i), offset, "message %d should have offset %d", i, i)
			lastProducedOffset = offset
		}

		latestOffset, err := adapter.GetLatestOffset(ctx, topic, 0)
		assert.NoError(t, err)
		assert.Equal(t, lastProducedOffset, latestOffset, "should return offset of last produced message")
		assert.Equal(t, int64(4), latestOffset, "after 5 messages (0-4), latest offset should be 4")
	})

	t.Run("verify high water mark behavior directly", func(t *testing.T) {
		highWaterMark, err := saramaClient.GetOffset(topic, 0, sarama.OffsetNewest)
		require.NoError(t, err)

		assert.Equal(t, int64(5), highWaterMark, "high water mark should be 5 (next write position)")

		latestOffset, err := adapter.GetLatestOffset(ctx, topic, 0)
		assert.NoError(t, err)
		assert.Equal(t, highWaterMark-1, latestOffset, "adapter should return high water mark minus 1")
	})

	t.Run("verify consumer can read up to latest offset", func(t *testing.T) {
		consumer, err := sarama.NewConsumer(brokers, saramaClient.Config())
		require.NoError(t, err)
		defer func(consumer sarama.Consumer) {
			_ = consumer.Close()
		}(consumer)

		partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
		require.NoError(t, err)
		defer func(partitionConsumer sarama.PartitionConsumer) {
			_ = partitionConsumer.Close()
		}(partitionConsumer)

		latestOffset, err := adapter.GetLatestOffset(ctx, topic, 0)
		require.NoError(t, err)

		var lastReadOffset int64 = -1
		messageCount := 0

		for messageCount < 5 { // we know there are 5 messages (offsets 0-4)
			select {
			case msg := <-partitionConsumer.Messages():
				lastReadOffset = msg.Offset
				messageCount++
				t.Logf("Read message with offset: %d", msg.Offset)
			case <-partitionConsumer.Errors():
				t.Fatal("Consumer error")
			case <-time.After(2 * time.Second):
				t.Fatalf("Timeout reading messages, only read %d messages", messageCount)
			}
		}

		assert.Equal(t, latestOffset, lastReadOffset, "last consumed message offset should match adapter's latest offset")
		assert.Equal(t, int64(4), lastReadOffset, "last message should have offset 4")
		assert.Equal(t, 5, messageCount, "should have read exactly 5 messages")
	})

	t.Run("demonstrate why subtraction is needed", func(t *testing.T) {
		highWaterMark, err := saramaClient.GetOffset(topic, 0, sarama.OffsetNewest)
		require.NoError(t, err)

		adapterResult, err := adapter.GetLatestOffset(ctx, topic, 0)
		require.NoError(t, err)

		consumer, err := sarama.NewConsumer(brokers, saramaClient.Config())
		require.NoError(t, err)
		defer func(consumer sarama.Consumer) {
			_ = consumer.Close()
		}(consumer)

		partitionConsumer, err := consumer.ConsumePartition(topic, 0, highWaterMark)
		require.NoError(t, err)
		defer func(partitionConsumer sarama.PartitionConsumer) {
			_ = partitionConsumer.Close()
		}(partitionConsumer)

		select {
		case msg := <-partitionConsumer.Messages():
			t.Fatalf("unexpected message at high water mark offset %d: %v", highWaterMark, msg)
		case <-time.After(500 * time.Millisecond):
		}

		consumer2, err := sarama.NewConsumer(brokers, saramaClient.Config())
		require.NoError(t, err)
		defer func(consumer2 sarama.Consumer) {
			_ = consumer2.Close()
		}(consumer2)

		partitionConsumer2, err := consumer2.ConsumePartition(topic, 0, adapterResult)
		require.NoError(t, err)
		defer func(partitionConsumer2 sarama.PartitionConsumer) {
			_ = partitionConsumer2.Close()
		}(partitionConsumer2)

		select {
		case msg := <-partitionConsumer2.Messages():
			assert.Equal(t, adapterResult, msg.Offset)
		case <-time.After(500 * time.Millisecond):
			t.Fatal("Expected to find message at adapter result offset")
		}

		assert.Equal(t, int64(1), highWaterMark-adapterResult, "Adapter should return high water mark minus 1")
	})
}

func createHealthChecker(t *testing.T) *pulse.HealthChecker {
	t.Helper()
	hc, err := pulse.NewHealthChecker(
		pulse.Config{StuckTimeout: 100 * time.Millisecond},
		NewClientAdapter(saramaClient),
	)
	require.NoError(t, err)
	return hc
}

func createTopic(t *testing.T, topicName string, partitions int32) {
	t.Helper()

	config := sarama.NewConfig()
	config.Version = saramaConfigVersion

	admin, err := sarama.NewClusterAdmin(brokers, config)
	if err != nil {
		t.Fatalf("failed to create admin client: %v", err)
	}
	defer func() {
		if err = admin.Close(); err != nil {
			t.Logf("failed to close admin client: %v", err)
		}
	}()

	topicDetail := &sarama.TopicDetail{
		NumPartitions:     partitions,
		ReplicationFactor: 1,
	}

	err = admin.CreateTopic(topicName, topicDetail, false)
	if err != nil {
		if !errors.Is(err, sarama.ErrTopicAlreadyExists) {
			t.Fatalf("failed to create topic %s: %v", topicName, err)
		}
	}

	// wait for topic metadata to propagate to all brokers
	time.Sleep(500 * time.Millisecond)
}
