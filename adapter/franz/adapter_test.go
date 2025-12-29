package franz

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestMessage_Wrapper(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name              string
		record            *kgo.Record
		expectedTopic     string
		expectedPartition int32
		expectedOffset    int64
	}

	testCases := []testCase{
		{
			name: "valid message",
			record: &kgo.Record{
				Topic:     "test-topic",
				Partition: 1,
				Offset:    100,
				Key:       []byte("key"),
				Value:     []byte("value"),
			},
			expectedTopic:     "test-topic",
			expectedPartition: 1,
			expectedOffset:    100,
		},
		{
			name: "empty topic",
			record: &kgo.Record{
				Topic:     "",
				Partition: 2,
				Offset:    200,
			},
			expectedTopic:     "",
			expectedPartition: 2,
			expectedOffset:    200,
		},
		{
			name: "zero offset",
			record: &kgo.Record{
				Topic:     "zero-offset-topic",
				Partition: 0,
				Offset:    0,
			},
			expectedTopic:     "zero-offset-topic",
			expectedPartition: 0,
			expectedOffset:    0,
		},
		{
			name: "max partition value",
			record: &kgo.Record{
				Topic:     "max-partition-topic",
				Partition: 2147483647, // max int32
				Offset:    999,
			},
			expectedTopic:     "max-partition-topic",
			expectedPartition: 2147483647,
			expectedOffset:    999,
		},
		{
			name: "negative offset",
			record: &kgo.Record{
				Topic:     "negative-offset-topic",
				Partition: 1,
				Offset:    -1,
			},
			expectedTopic:     "negative-offset-topic",
			expectedPartition: 1,
			expectedOffset:    -1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			msg := NewMessage(tc.record)

			assert.Equal(t, tc.expectedTopic, msg.Topic())
			assert.Equal(t, tc.expectedPartition, msg.Partition())
			assert.Equal(t, tc.expectedOffset, msg.Offset())

			// verify that the underlying record is still accessible
			if tc.record.Key != nil {
				impl := msg.(*Message)
				assert.Equal(t, tc.record.Key, impl.Record.Key)
			}
		})
	}
}

func TestClientAdapter_Constructors(t *testing.T) {
	t.Parallel()

	t.Run("NewClientAdapter should handle nil client", func(t *testing.T) {
		t.Parallel()

		adapter, err := NewClientAdapter(nil)
		assert.Error(t, err)
		assert.Nil(t, adapter)
		assert.Contains(t, err.Error(), "kgo client cannot be nil")
	})
}

func TestClientAdapter_Close(t *testing.T) {
	t.Parallel()

	t.Run("Close should be idempotent", func(t *testing.T) {
		t.Parallel()

		client, err := kgo.NewClient(
			kgo.SeedBrokers("localhost:9092"),
		)
		require.NoError(t, err)
		defer client.Close()

		adapter, err := NewClientAdapter(client)
		require.NoError(t, err)
		require.NotNil(t, adapter)

		// first close
		err = adapter.Close()
		assert.NoError(t, err)

		// second close should also succeed
		err = adapter.Close()
		assert.NoError(t, err)
	})
}
