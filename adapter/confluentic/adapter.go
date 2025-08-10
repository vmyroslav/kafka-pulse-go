package confluentic

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/vmyroslav/kafka-pulse-go/pulse"
)

var (
	_ pulse.TrackableMessage = (*Message)(nil)
	_ pulse.BrokerClient     = (*ClientAdapter)(nil)
	_ io.Closer              = (*ClientAdapter)(nil)
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

// ClientAdapter implements the pulse.BrokerClient interface.
// It holds a reusable producer for efficiently querying partition watermarks.
type ClientAdapter struct {
	producer    *kafka.Producer
	ownProducer bool // indicates if the adapter owns the producer and should close it
	mu          sync.RWMutex
}

// NewClientAdapter creates a new ClientAdapter from a configuration.
// It requires a kafka.ConfigMap containing at least the "bootstrap.servers".
// The adapter creates and manages its own producer internally.
// The returned adapter must be closed by the caller to prevent resource leaks.
func NewClientAdapter(config *kafka.ConfigMap) (*ClientAdapter, error) {
	if config == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}

	producer, err := kafka.NewProducer(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}

	return &ClientAdapter{producer: producer, ownProducer: true}, nil
}

// NewClientAdapterWithProducer creates a new adapter from an existing producer.
// The adapter will NOT take ownership; the original creator is responsible for closing the producer.
func NewClientAdapterWithProducer(producer *kafka.Producer) *ClientAdapter {
	return &ClientAdapter{producer: producer, ownProducer: false}
}

// Close closes the underlying producer and releases resources.
// It's safe to call Close multiple times.
// Only closes the producer if this adapter owns it.
func (c *ClientAdapter) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// only close the producer if this adapter owns it
	if c.ownProducer && c.producer != nil {
		c.producer.Close()
		c.producer = nil
	}

	return nil
}

func (c *ClientAdapter) GetLatestOffset(ctx context.Context, topic string, partition int32) (int64, error) {
	c.mu.RLock()
	producer := c.producer
	c.mu.RUnlock()

	if producer == nil {
		return 0, fmt.Errorf("producer is closed")
	}

	// use a timeout from the context
	timeout, err := contextToTimeout(ctx)
	if err != nil {
		return 0, err
	}

	_, high, err := producer.QueryWatermarkOffsets(topic, partition, timeout)
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
