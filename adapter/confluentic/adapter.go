package confluentic

import (
	"context"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/vmyroslav/kafka-pulse-go/pulse"
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

// clientAdapter wraps a Confluent Kafka AdminClient to implement pulse.BrokerClient
type clientAdapter struct {
	adminClient *kafka.AdminClient
}

// NewClientAdapter creates a new BrokerClient adapter for a confluent-kafka-go AdminClient
func NewClientAdapter(adminClient *kafka.AdminClient) pulse.BrokerClient {
	return &clientAdapter{adminClient: adminClient}
}

// GetLatestOffset fetches the newest available offset (the high watermark) for a topic-partition
func (c *clientAdapter) GetLatestOffset(ctx context.Context, topic string, partition int32) (int64, error) {
	// derive a timeout from the context for the GetWatermarkOffsets call
	timeout := 10 * time.Second
	if deadline, ok := ctx.Deadline(); ok {
		if newTimeout := time.Until(deadline); newTimeout > 0 {
			timeout = newTimeout
		} else {
			return 0, context.DeadlineExceeded
		}
	}

	// the high watermark is the offset of the next message to be produced
	_, high, err := c.adminClient.GetWatermarkOffsets(topic, partition, int(timeout.Milliseconds()))
	if err != nil {
		return 0, err
	}

	// to get the offset of the last actual message, subtract 1 from the high watermark
	return high - 1, nil
}
