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
	monitor       pulse.Monitor
	logger        *slog.Logger
	ready         chan bool
	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup

	// Dynamic control fields
	paused          atomic.Bool
	processingDelay atomic.Int64 // nanoseconds
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

	consumer := &Consumer{
		client:        client,
		consumerGroup: consGroup,
		monitor:       monitor,
		logger:        logger,
		ready:         make(chan bool),
		ctx:           ctx,
		cancel:        cancel,
	}

	// Set default processing delay to 100ms
	consumer.processingDelay.Store(int64(100 * time.Millisecond))

	return consumer, nil
}

// Pause stops message processing (simulates stuck consumer)
func (c *Consumer) Pause() {
	c.paused.Store(true)
	c.logger.Info("Consumer paused - simulating stuck consumer")
}

// Resume starts message processing again
func (c *Consumer) Resume() {
	c.paused.Store(false)
	c.logger.Info("Consumer resumed - recovering from stuck state")
}

// SetProcessingDelay sets the delay between message processing
func (c *Consumer) SetProcessingDelay(delay time.Duration) {
	c.processingDelay.Store(int64(delay))
	c.logger.Info("Processing delay updated", "delay", delay)
}

// IsPaused returns current pause state
func (c *Consumer) IsPaused() bool {
	return c.paused.Load()
}

// GetProcessingDelay returns current processing delay
func (c *Consumer) GetProcessingDelay() time.Duration {
	return time.Duration(c.processingDelay.Load())
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
	c.consumerGroup.Close()
	c.client.Close()
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

			// release the monitor for the topic and partition
			h.consumer.monitor.Release(ctx, claim.Topic(), claim.Partition())

			return nil
		case message := <-claim.Messages():
			if message == nil {
				return nil
			}

			// Check if consumer is paused
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

			// Use configurable processing delay
			delay := time.Duration(h.consumer.processingDelay.Load())
			time.Sleep(delay)

			session.MarkMessage(message, "")
		}
	}
}
