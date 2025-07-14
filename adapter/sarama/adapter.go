package sarama

import (
	"context"
	"github.com/IBM/sarama"
	"github.com/vmyroslav/kafka-pulse-go/pulse"
)

// Message wraps a sarama.ConsumerMessage to implement pulse.TrackableMessage
type Message struct {
	*sarama.ConsumerMessage
}

func (m *Message) Topic() string {
	return m.ConsumerMessage.Topic
}

func (m *Message) Partition() int32 {
	return m.ConsumerMessage.Partition
}

func (m *Message) Offset() int64 {
	return m.ConsumerMessage.Offset
}

// clientAdapter wraps a sarama.Client to implement pulse.BrokerClient
type clientAdapter struct {
	client sarama.Client
}

// NewClientAdapter creates a new BrokerClient adapter for a sarama.Client
func NewClientAdapter(c sarama.Client) pulse.BrokerClient {
	return &clientAdapter{client: c}
}

// GetLatestOffset fetches the newest available offset using the Sarama client
func (c *clientAdapter) GetLatestOffset(_ context.Context, topic string, partition int32) (int64, error) {
	latestOffset, err := c.client.GetOffset(topic, partition, sarama.OffsetNewest)
	if err != nil {
		return 0, err
	}

	// return offset - 1 to get the offset of the last actual message
	return latestOffset - 1, nil
}
