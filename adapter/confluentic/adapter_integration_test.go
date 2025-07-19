package confluentic_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	kafkacontainer "github.com/testcontainers/testcontainers-go/modules/kafka"
	confluenticadapter "github.com/vmyroslav/kafka-pulse-go/adapter/confluentic"
	"github.com/vmyroslav/kafka-pulse-go/pulse"
)

var (
	dockerImage           = "confluentinc/confluent-local:7.8.3"
	kafkaBootstrapServers string
	configMap             *kafka.ConfigMap
)

func TestMain(m *testing.M) {
	ctx := context.Background()

	kafkaContainer, err := kafkacontainer.Run(ctx, dockerImage,
		kafkacontainer.WithClusterID("test-cluster"),
	)
	if err != nil {
		log.Fatal(err)
	}

	brokers, err := kafkaContainer.Brokers(ctx)
	if err != nil {
		log.Fatal(err)
	}
	kafkaBootstrapServers = brokers[0]

	configMap = &kafka.ConfigMap{
		"bootstrap.servers": kafkaBootstrapServers,
	}

	// run tests
	code := m.Run()

	if err = testcontainers.TerminateContainer(kafkaContainer); err != nil {
		log.Printf("failed to terminate container: %s", err)
	}

	os.Exit(code)
}

func TestClientAdapter_Implementation(t *testing.T) {
	t.Parallel()

	t.Run("GetLatestOffset functionality", func(t *testing.T) {
		var (
			ctx            = context.Background()
			topicSingleMsg = "confluentic-topic-single-message"
			topicMultiPart = "confluentic-topic-multi-partition"
		)

		adminClient, err := kafka.NewAdminClient(configMap)
		require.NoError(t, err)
		defer adminClient.Close()

		topics := []kafka.TopicSpecification{
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

		results, err := adminClient.CreateTopics(ctx, topics)
		require.NoError(t, err)
		for _, result := range results {
			if result.Error.Code() != kafka.ErrNoError {
				require.NoError(t, result.Error)
			}
		}

		producer, err := kafka.NewProducer(configMap)
		require.NoError(t, err)
		defer producer.Close()

		// produce 1 message to the single-message topic
		err = producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topicSingleMsg, Partition: 0},
			Value:          []byte("test message"),
		}, nil)
		require.NoError(t, err)

		// produce 5 messages to topicMultiPart partition 2
		for i := 0; i < 5; i++ {
			err = producer.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topicMultiPart, Partition: 2},
				Value:          []byte(fmt.Sprintf("message-%d", i)),
			}, nil)
			require.NoError(t, err)
		}
		producer.Flush(5000)

		adapter := confluenticadapter.NewClientAdapter(configMap)

		t.Run("success on partition with single message", func(t *testing.T) {
			latestOffset, err := adapter.GetLatestOffset(ctx, topicSingleMsg, 0)
			assert.NoError(t, err)
			assert.Equal(t, int64(0), latestOffset)
		})

		t.Run("success on partition with multiple messages", func(t *testing.T) {
			// after 5 messages (offsets 0-4), the latest offset should be 4
			latestOffset, err := adapter.GetLatestOffset(ctx, topicMultiPart, 2)
			assert.NoError(t, err)
			assert.Equal(t, int64(4), latestOffset)
		})

		t.Run("error on non-existent topic", func(t *testing.T) {
			_, err := adapter.GetLatestOffset(ctx, "non-existent-topic", 0)
			assert.Error(t, err)
		})

		t.Run("error on non-existent partition", func(t *testing.T) {
			// topic exists but partition 99 doesn't exist
			_, err := adapter.GetLatestOffset(ctx, topicSingleMsg, 99)
			assert.Error(t, err)
		})
	})

	t.Run("concurrent message tracking", func(t *testing.T) {
		ctx := context.Background()
		topic := "confluentic-concurrent-tracking-topic"
		numPartitions := 4
		numGoroutines := 10
		messagesPerGoroutine := 20

		hc, err := pulse.NewHealthChecker(
			pulse.Config{StuckTimeout: 200 * time.Millisecond},
			confluenticadapter.NewClientAdapter(configMap),
		)
		require.NoError(t, err)

		concurrentAdmin, err := kafka.NewAdminClient(configMap)
		require.NoError(t, err)
		defer concurrentAdmin.Close()

		topicSpec := kafka.TopicSpecification{
			Topic:             topic,
			NumPartitions:     numPartitions,
			ReplicationFactor: 1,
		}
		results, err := concurrentAdmin.CreateTopics(ctx, []kafka.TopicSpecification{topicSpec})
		require.NoError(t, err)
		if results[0].Error.Code() != kafka.ErrNoError {
			require.NoError(t, results[0].Error)
		}

		// produce messages to multiple partitions
		producer, err := kafka.NewProducer(configMap)
		require.NoError(t, err)
		defer producer.Close()

		for partition := 0; partition < numPartitions; partition++ {
			for i := 0; i < messagesPerGoroutine; i++ {
				err = producer.Produce(&kafka.Message{
					TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: int32(partition)},
					Value:          []byte(fmt.Sprintf("message-p%d-%d", partition, i)),
				}, nil)
				require.NoError(t, err)
			}
		}
		producer.Flush(2000)

		// start multiple goroutines to track messages concurrently
		var wg sync.WaitGroup
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(goroutineID int) {
				defer wg.Done()
				partition := goroutineID % numPartitions

				for msgIdx := 0; msgIdx < messagesPerGoroutine; msgIdx++ {
					hc.Track(ctx, confluenticadapter.NewMessage(&kafka.Message{
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

	t.Run("concurrent health checks", func(t *testing.T) {
		ctx := context.Background()
		topic := "confluentic-concurrent-health-topic"
		numHealthChecks := 50

		hc, err := pulse.NewHealthChecker(
			pulse.Config{StuckTimeout: 100 * time.Millisecond},
			confluenticadapter.NewClientAdapter(configMap),
		)
		require.NoError(t, err)

		healthAdmin, err := kafka.NewAdminClient(configMap)
		require.NoError(t, err)
		defer healthAdmin.Close()

		topicSpec := kafka.TopicSpecification{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: 1,
		}
		results, err := healthAdmin.CreateTopics(ctx, []kafka.TopicSpecification{topicSpec})
		require.NoError(t, err)
		if results[0].Error.Code() != kafka.ErrNoError {
			require.NoError(t, results[0].Error)
		}

		// produce and track a message
		producer, err := kafka.NewProducer(configMap)
		require.NoError(t, err)
		defer producer.Close()

		err = producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0},
			Value:          []byte("test message"),
		}, nil)
		require.NoError(t, err)
		producer.Flush(1000)

		hc.Track(ctx, confluenticadapter.NewMessage(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0, Offset: 0},
		}))

		// run multiple health checks concurrently
		var wg sync.WaitGroup
		healthResults := make(chan bool, numHealthChecks)
		errCh := make(chan error, numHealthChecks)

		for i := 0; i < numHealthChecks; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				healthy, err := hc.Healthy(ctx)
				healthResults <- healthy
				errCh <- err
			}()
		}

		wg.Wait()
		close(healthResults)
		close(errCh)

		// all health checks should return consistent results
		var healthyCount int
		for healthy := range healthResults {
			if healthy {
				healthyCount++
			}
		}

		// Check that no errors occurred
		for err = range errCh {
			assert.NoError(t, err)
		}

		assert.Equal(t, numHealthChecks, healthyCount)
	})

	t.Run("backpressure scenario - slow consumer should be detected as unhealthy", func(t *testing.T) {
		ctx := context.Background()
		topic := "confluentic-backpressure-topic"

		hc, err := pulse.NewHealthChecker(
			pulse.Config{StuckTimeout: 100 * time.Millisecond},
			confluenticadapter.NewClientAdapter(configMap),
		)
		require.NoError(t, err)

		backpressureAdmin, err := kafka.NewAdminClient(configMap)
		require.NoError(t, err)
		defer backpressureAdmin.Close()

		topicSpec := kafka.TopicSpecification{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: 1,
		}
		results, err := backpressureAdmin.CreateTopics(ctx, []kafka.TopicSpecification{topicSpec})
		require.NoError(t, err)
		if results[0].Error.Code() != kafka.ErrNoError {
			require.NoError(t, results[0].Error)
		}

		producer, err := kafka.NewProducer(configMap)
		require.NoError(t, err)
		defer producer.Close()

		messageCount := 100
		for i := 0; i < messageCount; i++ {
			err = producer.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0},
				Key:            []byte(fmt.Sprintf("key-%d", i)),
				Value:          []byte(fmt.Sprintf("message-%d", i)),
			}, nil)
			require.NoError(t, err)
		}
		producer.Flush(5000)

		// simulate slow consumer that only processes a few messages
		slowConsumerOffset := int64(5) // only processed 6 messages out of 100
		hc.Track(ctx, confluenticadapter.NewMessage(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0, Offset: kafka.Offset(slowConsumerOffset)},
		}))

		// consumer should be unhealthy due to significant lag after timestamp becomes stale
		assert.Eventually(t, func() bool {
			healthy, err := hc.Healthy(ctx)
			return err == nil && !healthy
		}, 1*time.Second, 50*time.Millisecond, "slow consumer should be unhealthy due to backpressure")

		// now simulate consumer catching up
		catchUpOffset := int64(messageCount - 1) // caught up to last message
		hc.Track(ctx, confluenticadapter.NewMessage(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0, Offset: kafka.Offset(catchUpOffset)},
		}))

		healthy, err := hc.Healthy(ctx)
		assert.NoError(t, err)
		assert.True(t, healthy, "consumer should be healthy after catching up")
	})

	t.Run("idle period followed by activity - should distinguish between stuck and idle", func(t *testing.T) {
		ctx := context.Background()
		topic := "confluentic-idle-activity-topic"

		hc, err := pulse.NewHealthChecker(
			pulse.Config{StuckTimeout: 100 * time.Millisecond},
			confluenticadapter.NewClientAdapter(configMap),
		)
		require.NoError(t, err)

		adminClient, err := kafka.NewAdminClient(configMap)
		require.NoError(t, err)
		defer adminClient.Close()

		topicSpec := kafka.TopicSpecification{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: 1,
		}
		results, err := adminClient.CreateTopics(ctx, []kafka.TopicSpecification{topicSpec})
		require.NoError(t, err)
		if results[0].Error.Code() != kafka.ErrNoError {
			require.NoError(t, results[0].Error)
		}

		producer, err := kafka.NewProducer(configMap)
		require.NoError(t, err)
		defer producer.Close()

		// phase 1: Initial activity
		err = producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0},
			Value:          []byte("initial message"),
		}, nil)
		require.NoError(t, err)
		producer.Flush(1000)

		hc.Track(ctx, confluenticadapter.NewMessage(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0, Offset: 0},
		}))

		healthy, err := hc.Healthy(ctx)
		assert.NoError(t, err)
		assert.True(t, healthy)

		// phase 2: Long idle period - no new messages
		// should still be healthy because consumer is caught up (idle, not stuck)
		assert.Eventually(t, func() bool {
			healthy, err := hc.Healthy(ctx)
			return err == nil && healthy
		}, 1*time.Second, 50*time.Millisecond, "consumer should be healthy during idle period")

		// Phase 3: Activity resumes
		err = producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0},
			Value:          []byte("new message"),
		}, nil)
		require.NoError(t, err)
		producer.Flush(1000)

		// consumer doesn't immediately process the new message
		// should be unhealthy because there's a new message but consumer is stuck
		assert.Eventually(t, func() bool {
			healthy, err := hc.Healthy(ctx)
			return err == nil && !healthy
		}, 1*time.Second, 50*time.Millisecond, "consumer should be unhealthy when stuck after idle period")

		// consumer processes the new message
		hc.Track(ctx, confluenticadapter.NewMessage(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0, Offset: 1},
		}))

		// should be healthy again
		healthy, err = hc.Healthy(ctx)
		assert.NoError(t, err)
		assert.True(t, healthy, "consumer should be healthy after processing new message")
	})
}

func TestHealthCheckerIntegration_WithClientAdapter(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("should be unhealthy when stale and lagging behind", func(t *testing.T) {
		topic := "confluentic-stuck-topic"
		brokerClient := confluenticadapter.NewClientAdapter(configMap)

		hc, err := pulse.NewHealthChecker(
			pulse.Config{StuckTimeout: 100 * time.Millisecond},
			brokerClient,
		)
		require.NoError(t, err, "failed to create health checker")

		adminClient, err := kafka.NewAdminClient(configMap)
		require.NoError(t, err)
		defer adminClient.Close()

		topicSpec := kafka.TopicSpecification{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: 1,
		}
		results, err := adminClient.CreateTopics(ctx, []kafka.TopicSpecification{topicSpec})
		require.NoError(t, err)
		if results[0].Error.Code() != kafka.ErrNoError {
			require.NoError(t, results[0].Error)
		}

		producer, err := kafka.NewProducer(configMap)
		require.NoError(t, err)
		defer producer.Close()

		err = producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0},
			Value:          []byte("initial message"),
		}, nil)
		require.NoError(t, err)
		producer.Flush(1000)

		hc.Track(ctx, confluenticadapter.NewMessage(&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topic,
				Partition: 0,
				Offset:    0,
			},
		}))

		// consumer should still be healthy before producing new message (no lag yet)
		assert.Eventually(t, func() bool {
			healthy, err := hc.Healthy(ctx)
			return err == nil && healthy
		}, 200*time.Millisecond, 20*time.Millisecond, "consumer should be healthy before new message")

		// produce a new message to make the consumer "lag behind"
		err = producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0},
			Value:          []byte("new message"),
		}, nil)
		require.NoError(t, err)
		producer.Flush(1000)

		// consumer should be unhealthy after broker updates watermark
		assert.Eventually(t, func() bool {
			healthy, err := hc.Healthy(ctx)
			return err == nil && !healthy
		}, 1*time.Second, 50*time.Millisecond)
	})

	t.Run("a running consumer should be unhealthy when it gets stuck", func(t *testing.T) {
		topic := fmt.Sprintf("live-consumer-topic-%d", time.Now().UnixNano())
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		hc, err := pulse.NewHealthChecker(
			pulse.Config{StuckTimeout: 100 * time.Millisecond},
			confluenticadapter.NewClientAdapter(configMap),
		)
		require.NoError(t, err)

		adminClient, err := kafka.NewAdminClient(configMap)
		require.NoError(t, err)
		defer adminClient.Close()

		topicSpec := kafka.TopicSpecification{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: 1,
		}
		results, err := adminClient.CreateTopics(ctx, []kafka.TopicSpecification{topicSpec})
		require.NoError(t, err)
		if results[0].Error.Code() != kafka.ErrNoError {
			require.NoError(t, results[0].Error)
		}

		producer, err := kafka.NewProducer(configMap)
		require.NoError(t, err)
		defer producer.Close()

		consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
			"bootstrap.servers": kafkaBootstrapServers,
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
			hc.Track(ctx, confluenticadapter.NewMessage(msg))

			messageProcessed <- struct{}{}

			// consumer now stops reading, simulating being stuck
			<-ctx.Done()
		}()

		// produce the first message to process
		err = producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0},
			Value:          []byte("first message"),
		}, nil)
		require.NoError(t, err)
		producer.Flush(1000)

		// wait until the consumer has processed and tracked the first message
		select {
		case <-messageProcessed: // success
		case <-time.After(5 * time.Second):
			t.Fatal("timeout: consumer never processed the first message")
		}

		// At this point, the consumer should still be healthy even after StuckTimeout (no new messages yet)
		assert.Eventually(t, func() bool {
			healthy, err := hc.Healthy(ctx)
			return err == nil && healthy
		}, 200*time.Millisecond, 20*time.Millisecond, "consumer should still be healthy when no new messages")

		// produce a new message
		// consumer is stuck and will NOT process this makes the consumer "lagging behind"
		err = producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0},
			Value:          []byte("second message"),
		}, nil)
		require.NoError(t, err)
		producer.Flush(1000)

		// stuck consumer should be reported as unhealthy
		assert.Eventually(t, func() bool {
			healthy, err := hc.Healthy(ctx)
			return err == nil && !healthy
		}, 1*time.Second, 50*time.Millisecond, "the stuck consumer should be reported as unhealthy")
	})

	t.Run("should be healthy when idle but caught up", func(t *testing.T) {
		topic := "confluentic-idle-topic"
		hc, _ := pulse.NewHealthChecker(pulse.Config{StuckTimeout: 100 * time.Millisecond}, confluenticadapter.NewClientAdapter(configMap))

		adminClient, err := kafka.NewAdminClient(configMap)
		require.NoError(t, err)
		defer adminClient.Close()

		topicSpec := kafka.TopicSpecification{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: 1,
		}
		results, err := adminClient.CreateTopics(ctx, []kafka.TopicSpecification{topicSpec})
		require.NoError(t, err)
		if results[0].Error.Code() != kafka.ErrNoError {
			require.NoError(t, results[0].Error)
		}

		producer, err := kafka.NewProducer(configMap)
		require.NoError(t, err)
		defer producer.Close()

		err = producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0},
			Value:          []byte("message"),
		}, nil)
		require.NoError(t, err)
		producer.Flush(1000)

		hc.Track(ctx, confluenticadapter.NewMessage(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0, Offset: 0},
		}))

		// still healthy because it's idle but not lagging (even after StuckTimeout)
		assert.Eventually(t, func() bool {
			healthy, err := hc.Healthy(ctx)
			return err == nil && healthy
		}, 1*time.Second, 50*time.Millisecond, "should be healthy when idle but caught up")
	})

	t.Run("multi-partition tracking - should be healthy when all partitions are caught up", func(t *testing.T) {
		topic := "confluentic-multi-partition-healthy-topic"
		numPartitions := 3
		hc, err := pulse.NewHealthChecker(
			pulse.Config{StuckTimeout: 100 * time.Millisecond},
			confluenticadapter.NewClientAdapter(configMap),
		)
		require.NoError(t, err)

		adminClient, err := kafka.NewAdminClient(configMap)
		require.NoError(t, err)
		defer adminClient.Close()

		topicSpec := kafka.TopicSpecification{
			Topic:             topic,
			NumPartitions:     numPartitions,
			ReplicationFactor: 1,
		}
		results, err := adminClient.CreateTopics(ctx, []kafka.TopicSpecification{topicSpec})
		require.NoError(t, err)
		if results[0].Error.Code() != kafka.ErrNoError {
			require.NoError(t, results[0].Error)
		}

		producer, err := kafka.NewProducer(configMap)
		require.NoError(t, err)
		defer producer.Close()

		// produce messages to all partitions
		for partition := 0; partition < numPartitions; partition++ {
			err = producer.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: int32(partition)},
				Value:          []byte(fmt.Sprintf("message-p%d", partition)),
			}, nil)
			require.NoError(t, err)
		}
		producer.Flush(1000)

		// track messages from all partitions
		for partition := 0; partition < numPartitions; partition++ {
			hc.Track(ctx, confluenticadapter.NewMessage(&kafka.Message{
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
		topic := "confluentic-multi-partition-stuck-topic"
		numPartitions := 3
		hc, err := pulse.NewHealthChecker(
			pulse.Config{StuckTimeout: 100 * time.Millisecond},
			confluenticadapter.NewClientAdapter(configMap),
		)
		require.NoError(t, err)

		adminClient, err := kafka.NewAdminClient(configMap)
		require.NoError(t, err)
		defer adminClient.Close()

		topicSpec := kafka.TopicSpecification{
			Topic:             topic,
			NumPartitions:     numPartitions,
			ReplicationFactor: 1,
		}
		results, err := adminClient.CreateTopics(ctx, []kafka.TopicSpecification{topicSpec})
		require.NoError(t, err)
		if results[0].Error.Code() != kafka.ErrNoError {
			require.NoError(t, results[0].Error)
		}

		producer, err := kafka.NewProducer(configMap)
		require.NoError(t, err)
		defer producer.Close()

		// produce initial messages to all partitions
		for partition := 0; partition < numPartitions; partition++ {
			err = producer.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: int32(partition)},
				Value:          []byte(fmt.Sprintf("message-p%d", partition)),
			}, nil)
			require.NoError(t, err)
		}
		producer.Flush(1000)

		// track messages from all partitions
		for partition := 0; partition < numPartitions; partition++ {
			hc.Track(ctx, confluenticadapter.NewMessage(&kafka.Message{
				TopicPartition: kafka.TopicPartition{
					Topic:     &topic,
					Partition: int32(partition),
					Offset:    0,
				},
			}))
		}

		// all partitions should still be healthy before new message
		assert.Eventually(t, func() bool {
			healthy, err := hc.Healthy(ctx)
			return err == nil && healthy
		}, 200*time.Millisecond, 20*time.Millisecond, "all partitions should be healthy before new message")

		// produce a new message to partition 1 only (making it lag behind)
		err = producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 1},
			Value:          []byte("new message"),
		}, nil)
		require.NoError(t, err)
		producer.Flush(1000)

		// should be unhealthy because partition 1 is lagging
		assert.Eventually(t, func() bool {
			healthy, err := hc.Healthy(ctx)
			return err == nil && !healthy
		}, 1*time.Second, 50*time.Millisecond, "should be unhealthy when one partition is stuck")
	})

	t.Run("multi-partition tracking - should handle mixed partition states correctly", func(t *testing.T) {
		topic := "confluentic-multi-partition-mixed-topic"
		hc, err := pulse.NewHealthChecker(
			pulse.Config{StuckTimeout: 100 * time.Millisecond},
			confluenticadapter.NewClientAdapter(configMap),
		)
		require.NoError(t, err)

		adminClient, err := kafka.NewAdminClient(configMap)
		require.NoError(t, err)
		defer adminClient.Close()

		topicSpec := kafka.TopicSpecification{
			Topic:             topic,
			NumPartitions:     3,
			ReplicationFactor: 1,
		}
		results, err := adminClient.CreateTopics(ctx, []kafka.TopicSpecification{topicSpec})
		require.NoError(t, err)
		if results[0].Error.Code() != kafka.ErrNoError {
			require.NoError(t, results[0].Error)
		}

		producer, err := kafka.NewProducer(configMap)
		require.NoError(t, err)
		defer producer.Close()

		// produce different numbers of messages to different partitions
		// partition 0: 1 message
		err = producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0},
			Value:          []byte("message-p0"),
		}, nil)
		require.NoError(t, err)

		// partition 1: 3 messages
		for i := 0; i < 3; i++ {
			err = producer.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 1},
				Value:          []byte(fmt.Sprintf("message-p1-%d", i)),
			}, nil)
			require.NoError(t, err)
		}

		// partition 2: 2 messages
		for i := 0; i < 2; i++ {
			err = producer.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 2},
				Value:          []byte(fmt.Sprintf("message-p2-%d", i)),
			}, nil)
			require.NoError(t, err)
		}
		producer.Flush(1000)

		// track latest messages from all partitions (all caught up)
		hc.Track(ctx, confluenticadapter.NewMessage(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0, Offset: 0},
		}))
		hc.Track(ctx, confluenticadapter.NewMessage(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 1, Offset: 2},
		}))
		hc.Track(ctx, confluenticadapter.NewMessage(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 2, Offset: 1},
		}))

		// should be healthy - all partitions are caught up
		healthy, err := hc.Healthy(ctx)
		assert.NoError(t, err)
		assert.True(t, healthy)

		// now track an older message from partition 1, making it lag
		hc.Track(ctx, confluenticadapter.NewMessage(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 1, Offset: 0},
		}))

		// should be unhealthy because partition 1 is now lagging after timestamp becomes stale
		// wait for the StuckTimeout (100ms) to pass before checking, then verify unhealthy state
		assert.Eventually(t, func() bool {
			healthy, err := hc.Healthy(ctx)
			return err == nil && !healthy
		}, 3*time.Second, 100*time.Millisecond, "should be unhealthy when partition 1 is lagging")
	})
}

func TestHealthCheckerIntegration_ConsumerGroup_WithConfluentAdapter(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("consumer group processing messages with health monitoring", func(t *testing.T) {
		topic := "confluentic-consumer-group-topic"
		groupID := fmt.Sprintf("test-group-%d", time.Now().UnixNano())

		hc, err := pulse.NewHealthChecker(
			pulse.Config{StuckTimeout: 500 * time.Millisecond},
			confluenticadapter.NewClientAdapter(configMap),
		)
		require.NoError(t, err)

		adminClient, err := kafka.NewAdminClient(configMap)
		require.NoError(t, err)
		defer adminClient.Close()

		topicSpec := kafka.TopicSpecification{
			Topic:             topic,
			NumPartitions:     2,
			ReplicationFactor: 1,
		}
		results, err := adminClient.CreateTopics(ctx, []kafka.TopicSpecification{topicSpec})
		require.NoError(t, err)
		if results[0].Error.Code() != kafka.ErrNoError {
			require.NoError(t, results[0].Error)
		}

		// Produce messages to consume
		producer, err := kafka.NewProducer(configMap)
		require.NoError(t, err)
		defer producer.Close()

		messageCount := 10
		for i := 0; i < messageCount; i++ {
			err = producer.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
				Key:            []byte(fmt.Sprintf("key-%d", i)),
				Value:          []byte(fmt.Sprintf("message-%d", i)),
			}, nil)
			require.NoError(t, err)
		}
		producer.Flush(5000)

		consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
			"bootstrap.servers":  kafkaBootstrapServers,
			"group.id":           groupID,
			"auto.offset.reset":  "earliest",
			"enable.auto.commit": false, // manual commit for testing
		})
		require.NoError(t, err)
		defer func(consumer *kafka.Consumer) {
			_ = consumer.Close()
		}(consumer)

		err = consumer.SubscribeTopics([]string{topic}, nil)
		require.NoError(t, err)

		// consumer loop with health monitoring
		messagesProcessed := 0
		consumerDone := make(chan bool)
		consumerCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		go func() {
			defer close(consumerDone)

			for {
				select {
				case <-consumerCtx.Done():
					return
				default:
					msg, err := consumer.ReadMessage(1000 * time.Millisecond)
					if err != nil {
						continue // timeout or other error, keep trying
					}

					// track message with health checker\
					hc.Track(ctx, confluenticadapter.NewMessage(msg))

					_, err = consumer.CommitMessage(msg)
					require.NoError(t, err, "failed to commit message")

					messagesProcessed++
					if messagesProcessed >= messageCount {
						return // processed all messages
					}
				}
			}
		}()

		// wait for consumer to process all messages
		select {
		case <-consumerDone: // success
		case <-time.After(10 * time.Second):
			t.Fatal("consumer did not process all messages within timeout")
		}

		healthy, err := hc.Healthy(ctx)
		assert.NoError(t, err)
		assert.True(t, healthy, "consumer should be healthy after processing all messages")
		assert.Equal(t, messageCount, messagesProcessed, "should have processed all messages")
	})

	t.Run("consumer group with stuck consumer detection", func(t *testing.T) {
		topic := "confluentic-stuck-consumer-topic"
		groupID := fmt.Sprintf("stuck-test-group-%d", time.Now().UnixNano())

		hc, err := pulse.NewHealthChecker(
			pulse.Config{StuckTimeout: 200 * time.Millisecond},
			confluenticadapter.NewClientAdapter(configMap),
		)
		require.NoError(t, err)

		adminClient, err := kafka.NewAdminClient(configMap)
		require.NoError(t, err)
		defer adminClient.Close()

		topicSpec := kafka.TopicSpecification{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: 1,
		}
		results, err := adminClient.CreateTopics(ctx, []kafka.TopicSpecification{topicSpec})
		require.NoError(t, err)
		if results[0].Error.Code() != kafka.ErrNoError {
			require.NoError(t, results[0].Error)
		}

		consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
			"bootstrap.servers": kafkaBootstrapServers,
			"group.id":          groupID,
			"auto.offset.reset": "earliest",
		})
		require.NoError(t, err)
		defer func(consumer *kafka.Consumer) {
			_ = consumer.Close()
		}(consumer)

		err = consumer.SubscribeTopics([]string{topic}, nil)
		require.NoError(t, err)

		// produce first message
		producer, err := kafka.NewProducer(configMap)
		require.NoError(t, err)
		defer producer.Close()

		err = producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0},
			Value:          []byte("first message"),
		}, nil)
		require.NoError(t, err)
		producer.Flush(1000)

		// consumer processes first message then gets "stuck"
		firstMessageProcessed := make(chan bool)
		consumerCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		go func() {
			msg, err := consumer.ReadMessage(5000 * time.Millisecond)
			require.NoError(t, err, "failed to read first message")

			hc.Track(ctx, confluenticadapter.NewMessage(msg))
			close(firstMessageProcessed)

			// consumer now stops processing
			<-consumerCtx.Done()
		}()

		select {
		case <-firstMessageProcessed:
		case <-time.After(10 * time.Second):
			t.Fatal("Consumer did not process first message")
		}

		// consumer should be healthy after processing first message
		assert.Eventually(t, func() bool {
			healthy, err := hc.Healthy(ctx)
			return err == nil && healthy
		}, 1*time.Second, 50*time.Millisecond, "Consumer should be healthy after first message")

		// produce second message while consumer is stuck
		err = producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0},
			Value:          []byte("second message"),
		}, nil)
		require.NoError(t, err)
		producer.Flush(1000)

		// consumer should become unhealthy
		assert.Eventually(t, func() bool {
			healthy, err := hc.Healthy(ctx)
			return err == nil && !healthy
		}, 2*time.Second, 100*time.Millisecond, "Consumer should be unhealthy when stuck and lagging")
	})

	t.Run("consumer should track multiple partitions independently", func(t *testing.T) {
		topic := "confluentic-multi-partition-consumer-topic"

		hc, err := pulse.NewHealthChecker(
			pulse.Config{StuckTimeout: 100 * time.Millisecond},
			confluenticadapter.NewClientAdapter(configMap),
		)
		require.NoError(t, err)

		adminClient, err := kafka.NewAdminClient(configMap)
		require.NoError(t, err)
		defer adminClient.Close()

		topicSpec := kafka.TopicSpecification{
			Topic:             topic,
			NumPartitions:     2,
			ReplicationFactor: 1,
		}
		results, err := adminClient.CreateTopics(ctx, []kafka.TopicSpecification{topicSpec})
		require.NoError(t, err)
		if results[0].Error.Code() != kafka.ErrNoError {
			require.NoError(t, results[0].Error)
		}

		producer, err := kafka.NewProducer(configMap)
		require.NoError(t, err)
		defer producer.Close()

		for partition := 0; partition < 2; partition++ {
			for i := 0; i < 3; i++ {
				err = producer.Produce(&kafka.Message{
					TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: int32(partition)},
					Value:          []byte(fmt.Sprintf("message-p%d-%d", partition, i)),
				}, nil)
				require.NoError(t, err)
			}
		}
		producer.Flush(1000)

		// consumer tracks messages from both partitions
		hc.Track(ctx, confluenticadapter.NewMessage(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0, Offset: 2},
		}))

		hc.Track(ctx, confluenticadapter.NewMessage(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 1, Offset: 2},
		}))

		// consumer should be healthy when caught up on all assigned partitions
		healthy, err := hc.Healthy(ctx)
		assert.NoError(t, err)
		assert.True(t, healthy, "consumer should be healthy when caught up on all partitions")
	})

	t.Run("consumer group rebalancing scenario - should handle partition reassignment", func(t *testing.T) {
		topic := "confluentic-rebalance-topic"

		hc, err := pulse.NewHealthChecker(
			pulse.Config{StuckTimeout: 100 * time.Millisecond},
			confluenticadapter.NewClientAdapter(configMap),
		)
		require.NoError(t, err)

		// create topic
		adminClient, err := kafka.NewAdminClient(configMap)
		require.NoError(t, err)
		defer adminClient.Close()

		topicSpec := kafka.TopicSpecification{
			Topic:             topic,
			NumPartitions:     3,
			ReplicationFactor: 1,
		}
		results, err := adminClient.CreateTopics(ctx, []kafka.TopicSpecification{topicSpec})
		require.NoError(t, err)
		if results[0].Error.Code() != kafka.ErrNoError {
			require.NoError(t, results[0].Error)
		}

		// produce messages to multiple partitions
		producer, err := kafka.NewProducer(configMap)
		require.NoError(t, err)
		defer producer.Close()

		for partition := 0; partition < 3; partition++ {
			for i := 0; i < 2; i++ {
				err = producer.Produce(&kafka.Message{
					TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: int32(partition)},
					Value:          []byte(fmt.Sprintf("message-p%d-%d", partition, i)),
				}, nil)
				require.NoError(t, err)
			}
		}
		producer.Flush(1000)

		// simulate consumer processing messages from multiple partitions (before rebalance)
		hc.Track(ctx, confluenticadapter.NewMessage(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0, Offset: 1},
		}))
		hc.Track(ctx, confluenticadapter.NewMessage(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 1, Offset: 1},
		}))
		hc.Track(ctx, confluenticadapter.NewMessage(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 2, Offset: 1},
		}))

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
			err = producer.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 1},
				Value:          []byte(fmt.Sprintf("new message %d", i)),
			}, nil)
			require.NoError(t, err)
		}
		producer.Flush(1000)

		healthy, err = hc.Healthy(ctx)
		assert.NoError(t, err)
		assert.True(t, healthy)
	})
}
