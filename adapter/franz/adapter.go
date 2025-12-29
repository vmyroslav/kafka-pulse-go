package franz

import (
	"context"
	"fmt"
	"io"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/vmyroslav/kafka-pulse-go/pulse"
)

var (
	_ pulse.TrackableMessage = (*Message)(nil)
	_ pulse.BrokerClient     = (*ClientAdapter)(nil)
	_ io.Closer              = (*ClientAdapter)(nil)
)

// Message wraps a kgo.Record to implement the pulse.TrackableMessage interface
type Message struct {
	*kgo.Record
}

// NewMessage creates a new pulse.TrackableMessage from a franz-go record
func NewMessage(record *kgo.Record) pulse.TrackableMessage {
	return &Message{Record: record}
}

// Topic returns the message topic
func (m *Message) Topic() string {
	return m.Record.Topic
}

// Partition returns the message partition
func (m *Message) Partition() int32 {
	return m.Record.Partition
}

// Offset returns the message offset
func (m *Message) Offset() int64 {
	return m.Record.Offset
}

// ClientAdapter implements the pulse.BrokerClient interface.
// It uses kadm.Client to query partition offsets efficiently.
type ClientAdapter struct {
	client *kgo.Client
	kadm   *kadm.Client
}

// NewClientAdapter creates a new ClientAdapter from a kgo.Client.
func NewClientAdapter(client *kgo.Client) (*ClientAdapter, error) {
	if client == nil {
		return nil, fmt.Errorf("kgo client cannot be nil")
	}

	kadmClient := kadm.NewClient(client)

	return &ClientAdapter{
		client: client,
		kadm:   kadmClient,
	}, nil
}

// GetLatestOffset fetches the offset of the latest actual message for a topic partition.
// It returns the high watermark minus 1, representing the offset of the last committed message.
// For empty partitions, it returns -1.
func (c *ClientAdapter) GetLatestOffset(ctx context.Context, topic string, partition int32) (int64, error) {
	// returns high watermarks for partitions
	offsets, err := c.kadm.ListEndOffsets(ctx, topic)
	if err != nil {
		return 0, fmt.Errorf("failed to list end offsets: %w", err)
	}

	topicOffset, exists := offsets.Lookup(topic, partition)
	if !exists {
		return 0, fmt.Errorf("topic %s partition %d not found", topic, partition)
	}

	if topicOffset.Err != nil {
		return 0, fmt.Errorf("partition error: %w", topicOffset.Err)
	}

	highWatermark := topicOffset.Offset

	return highWatermark - 1, nil
}

// Close is a no-op for franz adapter.
// The underlying kgo.Client lifecycle is managed separately by the user.
func (c *ClientAdapter) Close() error {
	return nil // no-op: kadm.Client doesn't require explicit cleanup
}
