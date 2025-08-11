package segmentio

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/vmyroslav/kafka-pulse-go/pulse"
)

var (
	_ pulse.TrackableMessage = (*Message)(nil)
	_ pulse.BrokerClient     = (*ClientAdapter)(nil)
	_ io.Closer              = (*ClientAdapter)(nil)
)

// Message wraps a kafka.Message to implement the pulse.TrackableMessage interface
type Message struct {
	kafka.Message
}

// NewMessage creates a new pulse.TrackableMessage from a segmentio/kafka-go message
func NewMessage(msg kafka.Message) pulse.TrackableMessage {
	return &Message{Message: msg}
}

// Topic returns the message topic
func (m *Message) Topic() string {
	return m.Message.Topic
}

// Partition returns the message partition
func (m *Message) Partition() int32 {
	return int32(m.Message.Partition)
}

// Offset returns the message offset
func (m *Message) Offset() int64 {
	return m.Message.Offset
}

// ClientAdapter implements pulse.BrokerClient for the segmentio/kafka-go library
type ClientAdapter struct {
	dialer    *kafka.Dialer
	brokers   []string
	ownDialer bool
	mu        sync.RWMutex
}

// NewClientAdapter creates a new BrokerClient adapter with an owned dialer.
// It requires a list of bootstrap brokers to discover partition leaders.
// The returned adapter implements io.Closer and must be closed to free resources.
func NewClientAdapter(brokers []string) (*ClientAdapter, error) {
	if brokers == nil {
		return nil, errors.New("brokers cannot be nil")
	}

	return &ClientAdapter{
		dialer: &kafka.Dialer{
			Timeout:   10 * time.Second,
			DualStack: true,
		},
		brokers:   brokers,
		ownDialer: true,
	}, nil
}

// NewClientAdapterWithDialer creates a new BrokerClient adapter with a provided dialer.
// The caller retains ownership of the dialer and is responsible for its lifecycle.
// The returned adapter does NOT implement io.Closer since it doesn't own the dialer.
func NewClientAdapterWithDialer(dialer *kafka.Dialer, brokers []string) (*ClientAdapter, error) {
	if dialer == nil {
		return nil, errors.New("dialer cannot be nil")
	}

	if brokers == nil {
		return nil, errors.New("brokers cannot be nil")
	}

	return &ClientAdapter{
		dialer:    dialer,
		brokers:   brokers,
		ownDialer: false,
	}, nil
}

// GetLatestOffset fetches the last available offset for a topic-partition.
func (c *ClientAdapter) GetLatestOffset(ctx context.Context, topic string, partition int32) (int64, error) {
	c.mu.RLock()
	dialer := c.dialer
	brokers := c.brokers
	c.mu.RUnlock()

	if len(brokers) == 0 {
		return 0, fmt.Errorf("no brokers available")
	}

	conn, err := dialer.DialLeader(ctx, "tcp", brokers[0], topic, int(partition))
	if err != nil {
		return 0, err
	}
	defer func(conn *kafka.Conn) {
		_ = conn.Close()
	}(conn)

	lastOffset, err := conn.ReadLastOffset()
	if err != nil {
		return 0, err
	}

	// the high watermark is the offset of the next message to be written
	return lastOffset - 1, nil
}

// Close closes the dialer if it's owned by this adapter
func (c *ClientAdapter) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.ownDialer && c.dialer != nil {
		// kafka.Dialer doesn't have a Close method in segmentio/kafka-go
		// the dialer just holds configuration, no persistent connections to close
		c.dialer = nil
	}

	return nil
}
