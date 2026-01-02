//go:build integration

package franz

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	kafkacontainer "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/vmyroslav/kafka-pulse-go/pulse"
)

var (
	dockerImage  = "confluentinc/confluent-local:7.8.3"
	globalClient *kgo.Client
	globalKadm   *kadm.Client
	brokers      []string
)

func TestMain(m *testing.M) {
	ctx := context.Background()

	// start Kafka
	container, err := kafkacontainer.Run(ctx, dockerImage,
		kafkacontainer.WithClusterID("pulse-cluster"),
	)
	if err != nil {
		log.Fatal(err)
	}

	brokers, err = container.Brokers(ctx)
	if err != nil {
		log.Fatal(err)
	}

	// force 127.0.0.1 for stability on macOS
	for i, b := range brokers {
		brokers[i] = strings.Replace(b, "localhost", "127.0.0.1", 1)
	}

	globalClient, err = kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelError, nil)),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	if err != nil {
		log.Fatal(err)
	}

	// wait for connection
	for i := 0; i < 15; i++ {
		if err = globalClient.Ping(ctx); err == nil {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if err != nil {
		log.Fatalf("failed to ping: %v", err)
	}

	// ait for cluster stabilization (KRaft controller election)
	globalKadm = kadm.NewClient(globalClient)
	var ready bool
	for i := 0; i < 20; i++ {
		md, err := globalKadm.Metadata(ctx)
		if err == nil && md.Controller >= 0 {
			ready = true
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if !ready {
		log.Fatal("failed to wait for Kafka controller election")
	}

	code := m.Run()

	if globalKadm != nil {
		globalKadm.Close()
	}
	if globalClient != nil {
		globalClient.Close()
	}

	_ = testcontainers.TerminateContainer(container)
	os.Exit(code)
}

func TestClientAdapterIntegration_Implementation(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	topic := fmt.Sprintf("topic-impl-%d", time.Now().UnixNano())
	createTopic(t, topic, 1)

	// produce first message
	res := globalClient.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte("test")})
	require.NoError(t, res.FirstErr())

	adapter, err := NewClientAdapter(globalClient)
	require.NoError(t, err)
	defer adapter.Close()

	assert.Eventually(t, func() bool {
		offset, err := adapter.GetLatestOffset(ctx, topic, 0)
		return err == nil && offset == 0
	}, 2*time.Second, 200*time.Millisecond)

	// produce second message
	res = globalClient.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte("test-2")})
	require.NoError(t, res.FirstErr())

	assert.Eventually(t, func() bool {
		offset, err := adapter.GetLatestOffset(ctx, topic, 0)
		return err == nil && offset == 1
	}, 2*time.Second, 200*time.Millisecond)
}

func TestHealthCheckerIntegration_WithClientAdapter(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	t.Run("should be unhealthy when lagging", func(t *testing.T) {
		t.Parallel()

		topic := fmt.Sprintf("topic-lag-%d", time.Now().UnixNano())
		createTopic(t, topic, 1)

		for i := 0; i < 5; i++ {
			res := globalClient.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte("msg")})
			require.NoError(t, res.FirstErr())
		}

		adapter, _ := NewClientAdapter(globalClient)
		defer adapter.Close()
		hc, _ := pulse.NewHealthChecker(pulse.Config{StuckTimeout: 100 * time.Millisecond}, adapter)

		hc.Track(ctx, NewMessage(&kgo.Record{Topic: topic, Partition: 0, Offset: 0}))

		assert.Eventually(t, func() bool {
			healthy, err := hc.Healthy(ctx)
			return err == nil && !healthy
		}, 2*time.Second, 200*time.Millisecond)
	})

	t.Run("should be healthy when caught up", func(t *testing.T) {
		t.Parallel()

		topic := fmt.Sprintf("topic-healthy-%d", time.Now().UnixNano())
		createTopic(t, topic, 1)

		res := globalClient.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte("msg")})
		require.NoError(t, res.FirstErr())

		adapter, _ := NewClientAdapter(globalClient)
		defer adapter.Close()
		hc, _ := pulse.NewHealthChecker(pulse.Config{StuckTimeout: 100 * time.Millisecond}, adapter)

		hc.Track(ctx, NewMessage(&kgo.Record{Topic: topic, Partition: 0, Offset: 0}))

		healthy, err := hc.Healthy(ctx)
		assert.NoError(t, err)
		assert.True(t, healthy)
	})
}

func TestClientAdapter_GetLatestOffset_Scenarios(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	adapter, err := NewClientAdapter(globalClient)
	require.NoError(t, err)
	defer adapter.Close()

	t.Run("empty topic returns -1", func(t *testing.T) {
		t.Parallel()
		topic := fmt.Sprintf("franz-empty-topic-%d", time.Now().UnixNano())
		createTopic(t, topic, 1)

		offset, err := adapter.GetLatestOffset(ctx, topic, 0)
		assert.NoError(t, err)
		assert.Equal(t, int64(-1), offset, "empty partition should return -1")
	})

	t.Run("single message returns 0", func(t *testing.T) {
		t.Parallel()

		topic := fmt.Sprintf("franz-single-message-%d", time.Now().UnixNano())
		createTopic(t, topic, 1)

		// produce 1 message
		produceMessages(t, topic, 0, 1)

		offset, err := adapter.GetLatestOffset(ctx, topic, 0)
		assert.NoError(t, err)
		assert.Equal(t, int64(0), offset, "partition with 1 message should return offset 0")
	})

	t.Run("multiple messages returns correct last offset", func(t *testing.T) {
		t.Parallel()

		topic := fmt.Sprintf("franz-multiple-messages-%d", time.Now().UnixNano())
		createTopic(t, topic, 1)

		// produce 5 messages (offsets 0-4)
		lastOffset := produceMessages(t, topic, 0, 5)

		offset, err := adapter.GetLatestOffset(ctx, topic, 0)
		assert.NoError(t, err)
		assert.Equal(t, lastOffset, offset, "should return last produced offset")
		assert.Equal(t, int64(4), offset, "5 messages should have last offset 4")
	})

	t.Run("multi-partition topic with different offsets", func(t *testing.T) {
		t.Parallel()

		topic := fmt.Sprintf("franz-multi-part-offsets-%d", time.Now().UnixNano())
		createTopic(t, topic, 3)

		// produce different number of messages to each partition
		lastOffset0 := produceMessages(t, topic, 0, 2)
		lastOffset1 := produceMessages(t, topic, 1, 5)
		lastOffset2 := produceMessages(t, topic, 2, 10)

		offset0, err := adapter.GetLatestOffset(ctx, topic, 0)
		assert.NoError(t, err)
		assert.Equal(t, lastOffset0, offset0, "partition 0 should return last produced offset")

		offset1, err := adapter.GetLatestOffset(ctx, topic, 1)
		assert.NoError(t, err)
		assert.Equal(t, lastOffset1, offset1, "partition 1 should return last produced offset")

		offset2, err := adapter.GetLatestOffset(ctx, topic, 2)
		assert.NoError(t, err)
		assert.Equal(t, lastOffset2, offset2, "partition 2 should return last produced offset")
	})

	t.Run("non-existent topic returns error", func(t *testing.T) {
		t.Parallel()

		_, err := adapter.GetLatestOffset(ctx, "franz-non-existent-topic", 0)
		assert.Error(t, err, "should return error for non-existent topic")
	})

	t.Run("non-existent partition returns error", func(t *testing.T) {
		t.Parallel()

		topic := fmt.Sprintf("franz-partition-error-%d", time.Now().UnixNano())
		createTopic(t, topic, 1)

		// topic exists with 1 partition (partition 0), but partition 99 doesn't exist
		_, err := adapter.GetLatestOffset(ctx, topic, 99)
		assert.Error(t, err, "should return error for non-existent partition")
	})

	t.Run("verify consumer can read up to latest offset", func(t *testing.T) {
		t.Parallel()

		topic := fmt.Sprintf("franz-consumer-verify-%d", time.Now().UnixNano())
		createTopic(t, topic, 1)

		// produce 5 messages
		produceMessages(t, topic, 0, 5)

		latestOffset, err := adapter.GetLatestOffset(ctx, topic, 0)
		require.NoError(t, err)
		require.Equal(t, int64(4), latestOffset)

		// create consumer to verify we can actually read up to this offset
		consumer, err := kgo.NewClient(
			kgo.SeedBrokers(brokers...),
			kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
				topic: {0: kgo.NewOffset().At(0)},
			}),
		)
		require.NoError(t, err)
		defer consumer.Close()

		// read all messages
		var consumedOffsets []int64
		fetchCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		for len(consumedOffsets) < 5 {
			fetches := consumer.PollFetches(fetchCtx)
			if fetches.IsClientClosed() {
				break
			}
			fetches.EachRecord(func(r *kgo.Record) {
				consumedOffsets = append(consumedOffsets, r.Offset)
			})
		}

		assert.Len(t, consumedOffsets, 5, "should consume 5 messages")
		assert.Equal(t, int64(0), consumedOffsets[0], "first message should be offset 0")
		assert.Equal(t, int64(4), consumedOffsets[4], "last message should be offset 4")
		assert.Equal(t, latestOffset, consumedOffsets[4], "latest offset should match last consumed offset")
	})
}

func TestHealthCheckerIntegration_MultiPartition(t *testing.T) {
	ctx := t.Context()

	t.Run("all partitions healthy", func(t *testing.T) {
		topic := fmt.Sprintf("franz-multi-all-healthy-%d", time.Now().UnixNano())
		createTopic(t, topic, 3)

		adapter, err := NewClientAdapter(globalClient)
		require.NoError(t, err)
		defer adapter.Close()

		hc, err := pulse.NewHealthChecker(
			pulse.Config{StuckTimeout: 100 * time.Millisecond},
			adapter,
		)
		require.NoError(t, err)

		// produce 5 messages to each partition and get last offsets
		var lastOffsets [3]int64
		for p := int32(0); p < 3; p++ {
			lastOffsets[p] = produceMessages(t, topic, p, 5)
		}

		// track all partitions at their latest offsets
		for p := int32(0); p < 3; p++ {
			hc.Track(ctx, NewMessage(&kgo.Record{
				Topic:     topic,
				Partition: p,
				Offset:    lastOffsets[p],
			}))
		}

		// should be healthy - all partitions caught up
		healthy, err := hc.Healthy(ctx)
		assert.NoError(t, err)
		assert.True(t, healthy, "should be healthy when all partitions are caught up")
	})

	t.Run("one partition stuck", func(t *testing.T) {
		topic := fmt.Sprintf("franz-multi-one-stuck-%d", time.Now().UnixNano())
		createTopic(t, topic, 3)

		adapter, err := NewClientAdapter(globalClient)
		require.NoError(t, err)
		defer adapter.Close()

		hc, err := pulse.NewHealthChecker(
			pulse.Config{StuckTimeout: 100 * time.Millisecond},
			adapter,
		)
		require.NoError(t, err)

		// produce messages to all partitions and get last offsets
		var lastOffsets [3]int64
		for p := int32(0); p < 3; p++ {
			lastOffsets[p] = produceMessages(t, topic, p, 5)
		}

		// ensure we have a valid offset to be stuck at (behind the last offset)
		stuckOffset := int64(0)
		if lastOffsets[1] > 2 {
			stuckOffset = lastOffsets[1] - 2 // stuck 2 messages behind
		}

		// track partitions 0 and 2 at latest offset, but partition 1 at old offset
		hc.Track(ctx, NewMessage(&kgo.Record{
			Topic:     topic,
			Partition: 0,
			Offset:    lastOffsets[0],
		}))
		hc.Track(ctx, NewMessage(&kgo.Record{
			Topic:     topic,
			Partition: 1,
			Offset:    stuckOffset, // stuck behind last offset
		}))
		hc.Track(ctx, NewMessage(&kgo.Record{
			Topic:     topic,
			Partition: 2,
			Offset:    lastOffsets[2],
		}))

		// should become unhealthy after stuck timeout
		eventuallyUnhealthy(t, hc, 3*time.Second, "should be unhealthy when one partition is stuck")
	})

	t.Run("mixed partition states with different message counts", func(t *testing.T) {
		topic := fmt.Sprintf("franz-multi-mixed-%d", time.Now().UnixNano())
		createTopic(t, topic, 3)

		adapter, err := NewClientAdapter(globalClient)
		require.NoError(t, err)
		defer adapter.Close()

		hc, err := pulse.NewHealthChecker(
			pulse.Config{StuckTimeout: 100 * time.Millisecond},
			adapter,
		)
		require.NoError(t, err)

		// produce different numbers of messages to each partition
		lastOffset0 := produceMessages(t, topic, 0, 2)
		lastOffset1 := produceMessages(t, topic, 1, 10)
		lastOffset2 := produceMessages(t, topic, 2, 3)

		// track all at their respective latest offsets
		hc.Track(ctx, NewMessage(&kgo.Record{
			Topic:     topic,
			Partition: 0,
			Offset:    lastOffset0,
		}))
		hc.Track(ctx, NewMessage(&kgo.Record{
			Topic:     topic,
			Partition: 1,
			Offset:    lastOffset1,
		}))
		hc.Track(ctx, NewMessage(&kgo.Record{
			Topic:     topic,
			Partition: 2,
			Offset:    lastOffset2,
		}))

		// should be healthy - all caught up despite different message counts
		healthy, err := hc.Healthy(ctx)
		assert.NoError(t, err)
		assert.True(t, healthy, "should be healthy when all partitions caught up")

		// now produce more messages to partition 1 only
		newLastOffset1 := produceMessages(t, topic, 1, 5)

		// wait for StuckTimeout to elapse
		time.Sleep(150 * time.Millisecond)

		// should become unhealthy - partition 1 is now behind
		eventuallyUnhealthy(t, hc, 3*time.Second, "should become unhealthy when partition 1 falls behind")

		// catch up partition 1
		hc.Track(ctx, NewMessage(&kgo.Record{
			Topic:     topic,
			Partition: 1,
			Offset:    newLastOffset1,
		}))

		// should become healthy immediately after tracking latest offset
		healthy, err = hc.Healthy(ctx)
		assert.NoError(t, err)
		assert.True(t, healthy, "should be healthy after catching up")
	})

	t.Run("empty partitions mixed with non-empty", func(t *testing.T) {
		topic := fmt.Sprintf("franz-multi-empty-mixed-%d", time.Now().UnixNano())
		createTopic(t, topic, 4)

		adapter, err := NewClientAdapter(globalClient)
		require.NoError(t, err)
		defer adapter.Close()

		hc, err := pulse.NewHealthChecker(
			pulse.Config{StuckTimeout: 100 * time.Millisecond},
			adapter,
		)
		require.NoError(t, err)

		// produce messages to partitions 0 and 2 only (1 and 3 remain empty)
		lastOffset0 := produceMessages(t, topic, 0, 3)
		lastOffset2 := produceMessages(t, topic, 2, 5)
		// partitions 1 and 3 are empty (no messages produced)

		// track non-empty partitions at latest offsets
		hc.Track(ctx, NewMessage(&kgo.Record{
			Topic:     topic,
			Partition: 0,
			Offset:    lastOffset0,
		}))
		hc.Track(ctx, NewMessage(&kgo.Record{
			Topic:     topic,
			Partition: 2,
			Offset:    lastOffset2,
		}))

		// should be healthy - only tracking partitions with messages, and they're caught up
		healthy, err := hc.Healthy(ctx)
		assert.NoError(t, err)
		assert.True(t, healthy, "should be healthy when tracked partitions are caught up")
	})
}

func TestHealthCheckerIntegration_ConsumerGroup(t *testing.T) {
	ctx := t.Context()

	t.Run("real consumer group processing with health monitoring", func(t *testing.T) {
		topic := fmt.Sprintf("franz-consumer-group-%d", time.Now().UnixNano())
		createTopic(t, topic, 2)

		// produce 10 messages
		for i := 0; i < 10; i++ {
			res := globalClient.ProduceSync(ctx, &kgo.Record{
				Topic: topic,
				Value: []byte(fmt.Sprintf("msg-%d", i)),
			})
			require.NoError(t, res.FirstErr())
		}

		consumer, err := kgo.NewClient(
			kgo.SeedBrokers(brokers...),
			kgo.ConsumerGroup("franz-test-group"),
			kgo.ConsumeTopics(topic),
		)
		require.NoError(t, err)
		defer consumer.Close()

		adapter, err := NewClientAdapter(globalClient)
		require.NoError(t, err)
		defer adapter.Close()

		hc, err := pulse.NewHealthChecker(
			pulse.Config{StuckTimeout: 100 * time.Millisecond},
			adapter,
		)
		require.NoError(t, err)

		// consume and track messages
		var consumed int
		pollCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()

		for consumed < 10 {
			fetches := consumer.PollFetches(pollCtx)
			if fetches.IsClientClosed() {
				break
			}
			fetches.EachRecord(func(r *kgo.Record) {
				hc.Track(ctx, NewMessage(r))
				consumed++
			})
		}

		assert.Equal(t, 10, consumed, "should consume all 10 messages")

		// should be healthy after processing all messages
		healthy, err := hc.Healthy(ctx)
		assert.NoError(t, err)
		assert.True(t, healthy, "should be healthy after consuming all messages")
	})

	t.Run("partition rebalancing simulation", func(t *testing.T) {
		topic := fmt.Sprintf("franz-rebalancing-test-%d", time.Now().UnixNano())
		createTopic(t, topic, 3)

		adapter, err := NewClientAdapter(globalClient)
		require.NoError(t, err)
		defer adapter.Close()

		hc, err := pulse.NewHealthChecker(
			pulse.Config{StuckTimeout: 100 * time.Millisecond},
			adapter,
		)
		require.NoError(t, err)

		// produce messages to all 3 partitions
		var lastOffsets [3]int64
		for p := int32(0); p < 3; p++ {
			lastOffsets[p] = produceMessages(t, topic, p, 5)
		}

		// simulate consumer owning all 3 partitions
		for p := int32(0); p < 3; p++ {
			hc.Track(ctx, NewMessage(&kgo.Record{
				Topic:     topic,
				Partition: p,
				Offset:    lastOffsets[p],
			}))
		}

		healthy, err := hc.Healthy(ctx)
		assert.NoError(t, err)
		assert.True(t, healthy, "should be healthy before rebalance")

		// simulate rebalance: Release partitions 1 and 2
		hc.Release(ctx, topic, 1)
		hc.Release(ctx, topic, 2)

		// produce new messages to released partitions
		produceMessages(t, topic, 1, 3)
		produceMessages(t, topic, 2, 3)

		// should still be healthy - not tracking released partitions
		healthy, err = hc.Healthy(ctx)
		assert.NoError(t, err)
		assert.True(t, healthy, "should be healthy - released partitions not tracked")

		// produce to partition 0 (still owned)
		produceMessages(t, topic, 0, 2)

		// wait for StuckTimeout to elapse
		time.Sleep(150 * time.Millisecond)

		// should become unhealthy - partition 0 is behind
		eventuallyUnhealthy(t, hc, 3*time.Second, "should become unhealthy - partition 0 behind")
	})

	t.Run("partition reassignment", func(t *testing.T) {
		topic := fmt.Sprintf("franz-reassignment-test-%d", time.Now().UnixNano())
		createTopic(t, topic, 2)

		adapter, err := NewClientAdapter(globalClient)
		require.NoError(t, err)
		defer adapter.Close()

		hc, err := pulse.NewHealthChecker(
			pulse.Config{StuckTimeout: 100 * time.Millisecond},
			adapter,
		)
		require.NoError(t, err)

		// produce initial messages
		lastOffset0 := produceMessages(t, topic, 0, 5)
		lastOffset1 := produceMessages(t, topic, 1, 5)

		// track partition 0
		hc.Track(ctx, NewMessage(&kgo.Record{
			Topic:     topic,
			Partition: 0,
			Offset:    lastOffset0,
		}))

		healthy, err := hc.Healthy(ctx)
		assert.NoError(t, err)
		assert.True(t, healthy, "should be healthy tracking partition 0")

		// release partition 0
		hc.Release(ctx, topic, 0)

		// acquire partition 1 (simulate reassignment)
		hc.Track(ctx, NewMessage(&kgo.Record{
			Topic:     topic,
			Partition: 1,
			Offset:    lastOffset1,
		}))

		// should still be healthy
		healthy, err = hc.Healthy(ctx)
		assert.NoError(t, err)
		assert.True(t, healthy, "should be healthy after reassignment")

		// produce to partition 1
		newLastOffset1 := produceMessages(t, topic, 1, 3)

		// wait for StuckTimeout to elapse
		time.Sleep(150 * time.Millisecond)

		// should become unhealthy - partition 1 behind
		eventuallyUnhealthy(t, hc, 3*time.Second, "should become unhealthy - partition 1 behind")

		// catch up on partition 1
		hc.Track(ctx, NewMessage(&kgo.Record{
			Topic:     topic,
			Partition: 1,
			Offset:    newLastOffset1,
		}))

		// should become healthy immediately after tracking latest offset
		healthy, err = hc.Healthy(ctx)
		assert.NoError(t, err)
		assert.True(t, healthy, "should be healthy after catching up")
	})
}

func TestHealthCheckerIntegration_Concurrent(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	t.Run("concurrent message tracking", func(t *testing.T) {
		t.Parallel()

		topic := fmt.Sprintf("franz-concurrent-tracking-%d", time.Now().UnixNano())
		numPartitions := 4
		numGoroutines := 10
		messagesPerGoroutine := 20

		createTopic(t, topic, numPartitions)

		adapter, err := NewClientAdapter(globalClient)
		require.NoError(t, err)
		defer adapter.Close()

		hc, err := pulse.NewHealthChecker(
			pulse.Config{StuckTimeout: 200 * time.Millisecond},
			adapter,
		)
		require.NoError(t, err)

		// produce messages to all partitions
		for p := int32(0); p < int32(numPartitions); p++ {
			produceMessages(t, topic, p, messagesPerGoroutine)
		}

		// track messages from multiple goroutines
		var wg sync.WaitGroup
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(goroutineID int) {
				defer wg.Done()
				partition := int32(goroutineID % numPartitions)

				for msgIdx := 0; msgIdx < messagesPerGoroutine; msgIdx++ {
					hc.Track(ctx, NewMessage(&kgo.Record{
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

		healthy, err := hc.Healthy(ctx)
		assert.NoError(t, err)
		assert.True(t, healthy, "should be healthy after concurrent tracking")
	})

	t.Run("concurrent health checks", func(t *testing.T) {
		t.Parallel()

		topic := fmt.Sprintf("franz-concurrent-health-%d", time.Now().UnixNano())
		numHealthChecks := 50

		createTopic(t, topic, 1)

		adapter, err := NewClientAdapter(globalClient)
		require.NoError(t, err)
		defer adapter.Close()

		hc, err := pulse.NewHealthChecker(
			pulse.Config{StuckTimeout: 200 * time.Millisecond},
			adapter,
		)
		require.NoError(t, err)

		// produce and track a message
		produceMessages(t, topic, 0, 1)
		hc.Track(ctx, NewMessage(&kgo.Record{
			Topic:     topic,
			Partition: 0,
			Offset:    0,
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

		// check that no errors occurred
		for err := range errCh {
			assert.NoError(t, err)
		}

		assert.Equal(t, numHealthChecks, healthyCount, "all health checks should return healthy")
	})

	t.Run("concurrent partition release", func(t *testing.T) {
		t.Parallel()

		topic := fmt.Sprintf("franz-concurrent-release-%d", time.Now().UnixNano())
		numPartitions := 10

		createTopic(t, topic, numPartitions)

		adapter, err := NewClientAdapter(globalClient)
		require.NoError(t, err)
		defer adapter.Close()

		hc, err := pulse.NewHealthChecker(
			pulse.Config{StuckTimeout: 200 * time.Millisecond},
			adapter,
		)
		require.NoError(t, err)

		// produce and track messages for all partitions
		for p := int32(0); p < int32(numPartitions); p++ {
			lastOffset := produceMessages(t, topic, p, 5)
			hc.Track(ctx, NewMessage(&kgo.Record{
				Topic:     topic,
				Partition: p,
				Offset:    lastOffset,
			}))
		}

		// wait a moment for all tracking to complete
		time.Sleep(50 * time.Millisecond)

		// verify healthy
		healthy, err := hc.Healthy(ctx)
		assert.NoError(t, err)
		assert.True(t, healthy, "should be healthy before releases")

		// release all partitions
		var wg sync.WaitGroup
		for p := int32(0); p < int32(numPartitions); p++ {
			wg.Add(1)
			go func(partition int32) {
				defer wg.Done()
				hc.Release(ctx, topic, partition)
			}(p)
		}

		wg.Wait()

		// should still be healthy (no tracked partitions)
		healthy, err = hc.Healthy(ctx)
		assert.NoError(t, err)
		assert.True(t, healthy, "should be healthy after releasing all partitions")
	})
}

// createTopic creates a topic on-demand for test isolation
func createTopic(t *testing.T, name string, partitions int) {
	t.Helper()

	ctx := context.Background()
	_, err := globalKadm.CreateTopics(ctx, int32(partitions), 1, nil, name)
	if err != nil && !strings.Contains(err.Error(), "TOPIC_ALREADY_EXISTS") {
		require.NoError(t, err)
	}

	// wait for topic to be ready and metadata to propagate
	// poll until we can confirm the topic exists with correct partition count
	require.Eventually(t, func() bool {
		md, err := globalKadm.Metadata(ctx, name)
		if err != nil {
			return false
		}

		topicMd, exists := md.Topics[name]
		if !exists {
			return false
		}

		// verify partition count matches
		if len(topicMd.Partitions) != partitions {
			return false
		}

		// verify all partitions have a leader assigned
		for _, partition := range topicMd.Partitions {
			if partition.Leader < 0 {
				return false // no leader assigned yet
			}
		}

		return true
	}, 5*time.Second, 100*time.Millisecond, "topic %s should be ready with %d partitions", name, partitions)
}

// produceMessages produces N messages to a specific partition and returns the last offset
func produceMessages(t *testing.T, topic string, partition int32, count int) int64 {
	t.Helper()
	ctx := t.Context()

	var lastOffset int64
	for i := 0; i < count; i++ {
		res := globalClient.ProduceSync(ctx, &kgo.Record{
			Topic:     topic,
			Partition: partition,
			Value:     []byte(fmt.Sprintf("msg-%d", i)),
		})
		require.NoError(t, res.FirstErr())
		record, err := res.First()
		require.NoError(t, err)
		lastOffset = record.Offset
	}

	return lastOffset
}

// eventuallyUnhealthy asserts health check becomes unhealthy within timeout
func eventuallyUnhealthy(t *testing.T, hc *pulse.HealthChecker, timeout time.Duration, description string) {
	t.Helper()

	ctx := t.Context()

	assert.Eventually(t, func() bool {
		healthy, err := hc.Healthy(ctx)
		return err == nil && !healthy
	}, timeout, 100*time.Millisecond, description)
}
