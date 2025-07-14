package pulse

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockMessage struct {
	topic     string
	partition int32
	offset    int64
}

func (m *mockMessage) Topic() string    { return m.topic }
func (m *mockMessage) Partition() int32 { return m.partition }
func (m *mockMessage) Offset() int64    { return m.offset }

func TestNewTracker(t *testing.T) {
	t.Parallel()

	tr := newTracker()
	require.NotNil(t, tr)
	assert.NotNil(t, tr.topicPartitionOffsets)
}

func TestTracker_Track(t *testing.T) {
	t.Parallel()

	tr := newTracker()

	msg := &mockMessage{
		topic:     "test-topic",
		partition: 0,
		offset:    100,
	}

	tr.track(msg)

	offsets := tr.currentOffsets()
	assert.Len(t, offsets, 1)

	topicOffsets, exists := offsets["test-topic"]
	assert.True(t, exists, "Topic not found in offsets")
	assert.Len(t, topicOffsets, 1)

	offsetTs, exists := topicOffsets[0]
	assert.True(t, exists, "Partition 0 not found")
	assert.Equal(t, int64(100), offsetTs.Offset)
}

func TestTracker_Track_SameOffset(t *testing.T) {
	t.Parallel()

	tr := newTracker()

	msg := &mockMessage{
		topic:     "test-topic",
		partition: 0,
		offset:    100,
	}

	// track the same message twice
	tr.track(msg)
	firstTime := tr.currentOffsets()["test-topic"][0].Timestamp

	time.Sleep(10 * time.Millisecond) // ensure time difference
	tr.track(msg)
	secondTime := tr.currentOffsets()["test-topic"][0].Timestamp

	// timestamp should not change for same offset
	assert.True(t, firstTime.Equal(secondTime), "Timestamp should not change when tracking same offset")
}

func TestTracker_Track_DifferentOffset(t *testing.T) {
	t.Parallel()

	tr := newTracker()

	msg1 := &mockMessage{
		topic:     "test-topic",
		partition: 0,
		offset:    100,
	}

	msg2 := &mockMessage{
		topic:     "test-topic",
		partition: 0,
		offset:    101,
	}

	tr.track(msg1)
	firstTime := tr.currentOffsets()["test-topic"][0].Timestamp

	time.Sleep(10 * time.Millisecond)
	tr.track(msg2)
	secondTime := tr.currentOffsets()["test-topic"][0].Timestamp

	// timestamp should change for different offset
	assert.True(t, secondTime.After(firstTime), "Timestamp should update when tracking different offset")
	assert.Equal(t, int64(101), tr.currentOffsets()["test-topic"][0].Offset)
}

func TestTracker_CurrentOffsets_Copy(t *testing.T) {
	t.Parallel()

	tr := newTracker()

	msg := &mockMessage{
		topic:     "test-topic",
		partition: 0,
		offset:    100,
	}

	tr.track(msg)
	offsets1 := tr.currentOffsets()
	offsets2 := tr.currentOffsets()

	// modify one copy
	offsets1["test-topic"][0] = OffsetTimestamp{
		Offset:    999,
		Timestamp: time.Now(),
	}

	// the other copy should be unchanged
	assert.NotEqual(t, int64(999), offsets2["test-topic"][0].Offset, "currentOffsets() should return a deep copy")
}

func TestTracker_Drop(t *testing.T) {
	t.Parallel()

	tr := newTracker()

	msg := &mockMessage{
		topic:     "test-topic",
		partition: 0,
		offset:    100,
	}

	tr.track(msg)

	// verify it exists
	offsets := tr.currentOffsets()
	_, exists := offsets["test-topic"][0]
	assert.True(t, exists, "Message should exist before drop")

	// drop the partition
	tr.drop("test-topic", 0)

	// verify it's gone
	offsets = tr.currentOffsets()
	_, exists = offsets["test-topic"][0]
	assert.False(t, exists, "Message should not exist after drop")
}

func TestTracker_Drop_NonexistentPartition(t *testing.T) {
	t.Parallel()

	tr := newTracker()

	// should not panic when dropping non-existent partition
	tr.drop("nonexistent-topic", 0)
	tr.drop("test-topic", 999)
}

func TestTracker_Concurrent(t *testing.T) {
	t.Parallel()

	tr := newTracker()

	var wg sync.WaitGroup

	numGoroutines := 100

	// concurrent tracking
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)

		go func(offset int64) {
			defer wg.Done()

			msg := &mockMessage{
				topic:     "test-topic",
				partition: 0,
				offset:    offset,
			}
			tr.track(msg)
		}(int64(i))
	}

	for i := 0; i < 10; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			_ = tr.currentOffsets()
		}()
	}

	wg.Wait()

	offsets := tr.currentOffsets()
	assert.Len(t, offsets, 1, "Expected 1 topic after concurrent operations")
}
