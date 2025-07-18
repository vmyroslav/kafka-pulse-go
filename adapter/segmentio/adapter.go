package segmentio

import (
	"context"

	"github.com/segmentio/kafka-go"
	"github.com/vmyroslav/kafka-pulse-go/pulse"
)

var (
	_ pulse.TrackableMessage = (*Message)(nil)
	_ pulse.BrokerClient     = (*clientAdapter)(nil)
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

// clientAdapter implements pulse.BrokerClient for the segmentio/kafka-go library
type clientAdapter struct {
	brokers []string
}

// NewClientAdapter creates a new BrokerClient adapter.
// It requires a list of bootstrap brokers to discover partition leaders.
func NewClientAdapter(brokers []string) pulse.BrokerClient {
	return &clientAdapter{
		brokers: brokers,
	}
}

// GetLatestOffset fetches the last available offset for a topic-partition.
func (c *clientAdapter) GetLatestOffset(ctx context.Context, topic string, partition int32) (int64, error) {
	// kafka.DialLeader establishes a connection to the leader broker for the given
	// partition, using the first broker as a seed to discover the cluster topology
	conn, err := kafka.DialLeader(ctx, "tcp", c.brokers[0], topic, int(partition))
	if err != nil {
		return 0, err
	}
	defer func(conn *kafka.Conn) {
		_ = conn.Close()
	}(conn)

	// ReadLastOffset returns the high watermark for the partition
	lastOffset, err := conn.ReadLastOffset()
	if err != nil {
		return 0, err
	}

	// the high watermark is the offset of the next message to be written
	return lastOffset - 1, nil
}
