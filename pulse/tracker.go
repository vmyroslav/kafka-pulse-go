package pulse

import (
	"sync"
	"time"
)

type OffsetTimestamp struct {
	Timestamp time.Time
	Offset    int64
}

type tracker struct {
	topicPartitionOffsets map[string]map[int32]OffsetTimestamp
	mu                    sync.RWMutex
}

func newTracker() *tracker {
	return &tracker{topicPartitionOffsets: make(map[string]map[int32]OffsetTimestamp)}
}

func (t *tracker) track(m TrackableMessage) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if _, ok := t.topicPartitionOffsets[m.Topic()]; !ok {
		t.topicPartitionOffsets[m.Topic()] = make(map[int32]OffsetTimestamp)
	}

	existing := t.topicPartitionOffsets[m.Topic()][m.Partition()]

	// only update timestamp if offset has changed to handle poison-pill messages
	if existing.Offset != m.Offset() {
		t.topicPartitionOffsets[m.Topic()][m.Partition()] = OffsetTimestamp{
			Offset:    m.Offset(),
			Timestamp: time.Now(),
		}
	}
}

func (t *tracker) currentOffsets() map[string]map[int32]OffsetTimestamp {
	t.mu.RLock()
	defer t.mu.RUnlock()

	// create a deep copy to avoid data races
	result := make(map[string]map[int32]OffsetTimestamp)
	for topic, partitions := range t.topicPartitionOffsets {
		result[topic] = make(map[int32]OffsetTimestamp)
		for partition, offsetTimestamp := range partitions {
			result[topic][partition] = offsetTimestamp
		}
	}

	return result
}

func (t *tracker) drop(topic string, partition int32) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if partitions, exists := t.topicPartitionOffsets[topic]; exists {
		delete(partitions, partition)
	}
}
