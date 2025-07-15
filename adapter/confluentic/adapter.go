package confluentic

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/vmyroslav/kafka-pulse-go/pulse"
	"time"
)

var _ pulse.TrackableMessage = (*Message)(nil)

// Message wraps a kafka.Message to implement the pulse.TrackableMessage interface
type Message struct {
	*kafka.Message
}

// NewMessage creates a new pulse.TrackableMessage from a confluent-kafka-go message
func NewMessage(msg *kafka.Message) pulse.TrackableMessage {
	return &Message{Message: msg}
}

// Topic returns the message topic
func (m *Message) Topic() string {
	if m.Message.TopicPartition.Topic == nil {
		return ""
	}
	return *m.Message.TopicPartition.Topic
}

// Partition returns the message partition
func (m *Message) Partition() int32 {
	return m.Message.TopicPartition.Partition
}

// Offset returns the message offset
func (m *Message) Offset() int64 {
	return int64(m.Message.TopicPartition.Offset)
}

// clientAdapter implements the pulse.BrokerClient. It holds the Kafka
// configuration needed to create a temporary client for metadata queries.
type clientAdapter struct {
	config *kafka.ConfigMap
}

// NewClientAdapter creates a new BrokerClient adapter.
// It requires a kafka.ConfigMap containing at least the "bootstrap.servers".
func NewClientAdapter(config *kafka.ConfigMap) pulse.BrokerClient {
	return &clientAdapter{config: config}
}

func (c *clientAdapter) GetLatestOffset(ctx context.Context, topic string, partition int32) (int64, error) {
	// Create a temporary producer for the sole purpose of this query.
	p, err := kafka.NewProducer(c.config)
	if err != nil {
		return 0, fmt.Errorf("failed to create temporary producer for watermark query: %w", err)
	}
	defer p.Close()

	// Use a timeout from the context.
	timeout, err := contextToTimeout(ctx)
	if err != nil {
		return 0, err
	}

	// QueryWatermarkOffsets is the correct method on a producer.
	_, high, err := p.QueryWatermarkOffsets(topic, partition, timeout)
	if err != nil {
		return 0, err
	}

	// High watermark - 1 is the offset of the last existing message.
	return high - 1, nil
}

// contextToTimeout is a helper to convert a context deadline to a timeout in milliseconds.
func contextToTimeout(ctx context.Context) (int, error) {
	deadline, ok := ctx.Deadline()
	if !ok {
		return 10000, nil // Default to 10 seconds
	}

	timeout := time.Until(deadline)
	if timeout < 0 {
		return 0, context.DeadlineExceeded
	}

	return int(timeout.Milliseconds()), nil
}

// consumerClientAdapter is the internal struct for the Consumer implementation.
type consumerClientAdapter struct {
	consumer *kafka.Consumer
}

// NewFromConsumer creates the adapter using a kafka.Consumer.
func NewFromConsumer(consumer *kafka.Consumer) pulse.BrokerClient {
	return &consumerClientAdapter{consumer: consumer}
}

// GetLatestOffset fetches the watermark using the Consumer.
func (c *consumerClientAdapter) GetLatestOffset(ctx context.Context, topic string, partition int32) (int64, error) {
	// The consumer's GetWatermarkOffsets does not accept a context.
	_, high, err := c.consumer.GetWatermarkOffsets(topic, partition)
	if err != nil {
		return 0, err
	}

	return high - 1, nil
}
