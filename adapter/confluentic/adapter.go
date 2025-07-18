package confluentic

import (
	"context"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/vmyroslav/kafka-pulse-go/pulse"
)

var (
	_ pulse.TrackableMessage = (*Message)(nil)
	_ pulse.BrokerClient     = (*clientAdapter)(nil)
)

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

// clientAdapter implements the pulse.BrokerClient interface.
// It holds the Kafka configuration needed to create a temporary producer for querying partition watermarks.
type clientAdapter struct {
	config *kafka.ConfigMap
}

// NewClientAdapter creates a new BrokerClient adapter.
// It requires a kafka.ConfigMap containing at least the "bootstrap.servers".
func NewClientAdapter(config *kafka.ConfigMap) pulse.BrokerClient {
	return &clientAdapter{config: config}
}

func (c *clientAdapter) GetLatestOffset(ctx context.Context, topic string, partition int32) (int64, error) {
	// create a temporary producer
	p, err := kafka.NewProducer(c.config)
	if err != nil {
		return 0, fmt.Errorf("failed to create temporary producer for watermark query: %w", err)
	}
	defer p.Close()

	// use a timeout from the context
	timeout, err := contextToTimeout(ctx)
	if err != nil {
		return 0, err
	}

	_, high, err := p.QueryWatermarkOffsets(topic, partition, timeout)
	if err != nil {
		return 0, err
	}

	// high watermark - 1 is the offset of the last existing message
	return high - 1, nil
}

// contextToTimeout is a helper to convert a context deadline to a timeout in milliseconds.
func contextToTimeout(ctx context.Context) (int, error) {
	deadline, ok := ctx.Deadline()
	if !ok {
		return 5000, nil // default to 5 seconds
	}

	timeout := time.Until(deadline)
	if timeout < 0 {
		return 0, context.DeadlineExceeded
	}

	return int(timeout.Milliseconds()), nil
}
