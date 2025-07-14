package pulse

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

type TrackableMessage interface {
	// Topic returns the message topic.
	Topic() string

	// Partition returns the message partition.
	Partition() int32

	// Offset returns the message offset.
	Offset() int64
}

type Monitor interface {
	// Track records that a message has been processed for a topic partition.
	Track(ctx context.Context, msg TrackableMessage)

	// Release removes a partition from tracking, typically during a rebalance.
	Release(ctx context.Context, topic string, partition int32)

	// Healthy returns true if the consumer is making progress,
	// and false if it appears to be stalled or lagging.
	Healthy(ctx context.Context) (bool, error)
}

type BrokerClient interface {
	// GetLatestOffset fetches the newest available offset for a topic partition.
	GetLatestOffset(ctx context.Context, topic string, partition int32) (int64, error)
}

type HealthChecker struct {
	client       BrokerClient
	tracker      *tracker
	logger       *slog.Logger
	stuckTimeout time.Duration
}

func NewHealthChecker(cfg Config, client BrokerClient) (*HealthChecker, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	if client == nil {
		return nil, errors.New("kafka client cannot be nil")
	}

	logger := slog.New(slog.DiscardHandler)
	if cfg.Logger != nil {
		logger = cfg.Logger
	}

	return &HealthChecker{
		client:       client,
		tracker:      newTracker(),
		stuckTimeout: cfg.StuckTimeout,
		logger:       logger,
	}, nil
}

func (h *HealthChecker) Healthy(ctx context.Context) (bool, error) {
	currentState := h.tracker.currentOffsets()
	now := time.Now()

	// check if any partition is stuck beyond timeout
	for topic := range currentState {
		for partition := range currentState[topic] {
			offsetTimestamp := currentState[topic][partition]
			if now.Sub(offsetTimestamp.Timestamp) > h.stuckTimeout {
				// Timeout exceeded - need to check if consumer is truly stuck
				// by comparing with latest broker offset
				latestOffset, err := h.client.GetLatestOffset(ctx, topic, partition)
				if err != nil {
					h.logger.Error("failed to get latest offset from broker",
						"topic", topic, "partition", partition, "error", err)
					// Don't fail the health check on a transient broker error.
					// A persistent error will eventually be caught by other monitoring.
					continue
				}

				// Convert to actual latest message offset (OffsetNewest returns next offset)
				latestMessageOffset := latestOffset - 1

				// Consumer is stuck if it's behind available messages
				if offsetTimestamp.Offset < latestMessageOffset {
					h.logger.Warn("consumer stuck behind available messages",
						"topic", topic,
						"partition", partition,
						"consumerOffset", offsetTimestamp.Offset,
						"latestOffset", latestMessageOffset,
						"lastUpdate", offsetTimestamp.Timestamp,
						"timeout", h.stuckTimeout,
					)

					return false, nil
				}
				// Consumer is caught up, just idle
				h.logger.Info("Consumer idle but caught up",
					"topic", topic,
					"partition", partition,
					"offset", offsetTimestamp.Offset,
					"lastUpdate", offsetTimestamp.Timestamp,
				)
			}
		}
	}

	return true, nil
}

func (h *HealthChecker) Track(_ context.Context, msg TrackableMessage) {
	h.tracker.track(msg)
}

func (h *HealthChecker) Release(_ context.Context, topic string, partition int32) {
	h.tracker.drop(topic, partition)
}
