package main

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/IBM/sarama"
	adapter "github.com/vmyroslav/kafka-pulse-go/adapter/sarama"
	"github.com/vmyroslav/kafka-pulse-go/pulse"
)

type Consumer struct {
	client        sarama.Client
	consumerGroup sarama.ConsumerGroup
	groupID       string
	monitor       pulse.Monitor
	logger        *slog.Logger
	ready         chan bool
	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup

	// control fields
	paused          atomic.Bool
	processingDelay atomic.Int64 // nanoseconds
	defaultDelay    time.Duration
}

func NewConsumer(brokers []string, groupID, topic string, monitor pulse.Monitor, logger *slog.Logger) (*Consumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Return.Errors = true
	config.Consumer.Group.Session.Timeout = 10 * time.Second
	config.Consumer.Group.Heartbeat.Interval = 3 * time.Second

	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		return nil, err
	}

	consGroup, err := sarama.NewConsumerGroupFromClient(groupID, client)
	if err != nil {
		_ = client.Close()

		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	defaultDelay := 100 * time.Millisecond
	consumer := &Consumer{
		client:        client,
		consumerGroup: consGroup,
		groupID:       groupID,
		monitor:       monitor,
		logger:        logger,
		ready:         make(chan bool),
		ctx:           ctx,
		cancel:        cancel,
		defaultDelay:  defaultDelay,
	}

	// set default processing delay to 100ms
	consumer.processingDelay.Store(int64(defaultDelay))

	return consumer, nil
}

// Pause stops message processing (simulates stuck consumer)
func (c *Consumer) Pause() {
	c.paused.Store(true)
	c.logger.Info("consumer paused - simulating stuck consumer")
}

// Resume starts message processing again and resets delay to default
func (c *Consumer) Resume() {
	c.paused.Store(false)
	c.processingDelay.Store(int64(c.defaultDelay))
	c.logger.Info("consumer resumed - recovering from stuck state", "delay_reset_to", c.defaultDelay)
}

// SetProcessingDelay sets the delay between message processing
func (c *Consumer) SetProcessingDelay(delay time.Duration) {
	c.processingDelay.Store(int64(delay))
	c.logger.Info("processing delay updated", "delay", delay)
}

// IsPaused returns current pause state
func (c *Consumer) IsPaused() bool {
	return c.paused.Load()
}

// GetProcessingDelay returns current processing delay
func (c *Consumer) GetProcessingDelay() time.Duration {
	return time.Duration(c.processingDelay.Load())
}

// TriggerRebalance forces a consumer group rebalance by leaving and rejoining the group
func (c *Consumer) TriggerRebalance() error {
	c.logger.Info("Triggering consumer group rebalance...")

	// Close current consumer group
	if err := c.consumerGroup.Close(); err != nil {
		c.logger.Error("Failed to close consumer group during rebalance", "error", err)
		return err
	}

	// Create new consumer group with same configuration
	consGroup, err := sarama.NewConsumerGroupFromClient(c.groupID, c.client)
	if err != nil {
		c.logger.Error("Failed to create new consumer group during rebalance", "error", err)
		return err
	}

	c.consumerGroup = consGroup
	c.logger.Info("Consumer group rebalance triggered successfully")

	return nil
}

func (c *Consumer) Start(topic string) error {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for {
			if err := c.consumerGroup.Consume(c.ctx, []string{topic}, &consumerGroupHandler{
				consumer: c,
				ready:    c.ready,
			}); err != nil {
				c.logger.Error("Error from consumer", "error", err)
				return
			}

			if c.ctx.Err() != nil {
				return
			}
		}
	}()

	<-c.ready
	c.logger.Info("Consumer started and ready")
	return nil
}

func (c *Consumer) Stop() {
	c.cancel()
	c.wg.Wait()
	_ = c.consumerGroup.Close()
	_ = c.client.Close()
}

type consumerGroupHandler struct {
	consumer *Consumer
	ready    chan bool
}

func (h *consumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	close(h.ready)
	return nil
}

func (h *consumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *consumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	ctx := session.Context()

	for {
		select {
		case <-ctx.Done():
			h.consumer.logger.Debug(
				"Session closed",
				"topic",
				claim.Topic(),
				"partition",
				claim.Partition(),
				"error",
				ctx.Err(),
			)

			// --- IMPORTANT: Handling Rebalancing ---
			// The session's context is canceled by Sarama when a consumer group rebalance
			// starts or the session is otherwise closed. This signals that this consumer
			// instance is losing its claim to this partition.
			//
			// You MUST call `monitor.Release()` to purge this partition's tracking data
			// from this specific consumer's health monitor.
			//
			// Failure to do this would cause this instance to continue reporting stale
			// health information for a partition it no longer owns, leading to false
			// "stuck" alerts.
			h.consumer.monitor.Release(ctx, claim.Topic(), claim.Partition())

			return nil
		case message := <-claim.Messages():
			if message == nil {
				return nil
			}

			if h.consumer.paused.Load() {
				h.consumer.logger.Debug("Consumer paused, skipping message processing",
					"topic", message.Topic,
					"partition", message.Partition,
					"offset", message.Offset)
				continue // Skip processing but don't mark message as consumed
			}

			h.consumer.logger.Info("Received message",
				"topic", message.Topic,
				"partition", message.Partition,
				"offset", message.Offset,
				"value", string(message.Value))

			wrappedMessage := adapter.NewMessage(message)
			h.consumer.monitor.Track(session.Context(), wrappedMessage)

			// use configurable processing delay
			delay := time.Duration(h.consumer.processingDelay.Load())
			time.Sleep(delay)

			session.MarkMessage(message, "")
		}
	}
}
