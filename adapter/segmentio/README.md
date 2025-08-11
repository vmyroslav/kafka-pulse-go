# SegmentIO Adapter

[![Godoc](https://pkg.go.dev/badge/github.com/vmyroslav/kafka-pulse-go/adapter/segmentio)](https://pkg.go.dev/github.com/vmyroslav/kafka-pulse-go/adapter/segmentio)

Kafka health monitoring adapter for [segmentio/kafka-go](https://github.com/segmentio/kafka-go) Kafka client.

## Installation

```bash
go get github.com/vmyroslav/kafka-pulse-go/adapter/segmentio
```

## Quick Start

This example demonstrates how to integrate `kafka-pulse-go` into a standard segmentio/kafka-go consumer application, exposing the health status via an HTTP endpoint for use with systems like Kubernetes.

A key feature of the kafka-go reader is the OnRevoke callback, which we'll use to reliably inform the health checker when partitions are revoked during a consumer group rebalance. This prevents false alarms.

### Example Application

The following code sets up a consumer that processes messages and handles health checks in a single `main.go` file.

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/vmyroslav/kafka-pulse-go/pulse"
	pulseSegmentio "github.com/vmyroslav/kafka-pulse-go/adapter/segmentio"
)

const (
	kafkaTopic      = "healthcheck-topic"
	consumerGroup   = "pulse-checker-group-segmentio"
	healthCheckPort = "8080"
)

var kafkaBrokers = []string{"localhost:9092"}

func main() {
	// Setup logger
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	// 1. Initialize Pulse Health Checker
	// The adapter only needs the list of broker addresses.
	pulseAdapter, err := pulseSegmentio.NewClientAdapter(kafkaBrokers)
	if err != nil {
		logger.Error("Failed to create segmentio adapter", "error", err)
		os.Exit(1)
	}
	pulseConfig := pulse.Config{
		Logger:             logger,
		StuckTimeout:       30 * time.Second,
		IgnoreBrokerErrors: false,
	}
	healthChecker, err := pulse.NewHealthChecker(pulseConfig, pulseAdapter)
	if err != nil {
		logger.Error("Failed to create health checker", "error", err)
		os.Exit(1)
	}

	// 2. Setup HTTP Health Check Endpoint
	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		healthy, err := healthChecker.Healthy(r.Context())
		if err != nil {
			logger.ErrorContext(r.Context(), "Health check failed with an error", "error", err)
			http.Error(w, fmt.Sprintf("Health check failed: %s", err), http.StatusInternalServerError)
			return
		}
		if !healthy {
			logger.WarnContext(r.Context(), "Health check reported unhealthy status")
			http.Error(w, "Consumer is not healthy", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	go func() {
		logger.Info("Starting health check server", "port", healthCheckPort)
		if err := http.ListenAndServe(":"+healthCheckPort, nil); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("Health check server failed", "error", err)
		}
	}()

	// 3. Initialize the kafka-go Reader with the OnRevoke callback
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   kafkaBrokers,
		Topic:     kafkaTopic,
		GroupID:   consumerGroup,
		Partition: 0, // Must specify a partition if GroupID is not set. With GroupID, it's ignored.
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
		// The OnRevoke callback is the key to handling rebalances correctly.
		// It ensures we release revoked partitions from tracking.
		OnRevoke: func(ctx context.Context, partitions []kafka.Partition) error {
			logger.Info("Partitions revoked", "partitions", partitions)
			for _, p := range partitions {
				healthChecker.Release(ctx, p.Topic, int32(p.ID))
			}
			return nil
		},
		OnAssign: func(ctx context.Context, partitions []kafka.Partition) error {
			logger.Info("Partitions assigned", "partitions", partitions)
			return nil
		},
	})
	defer reader.Close()

	// 4. Start the Consumer Loop
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		for {
			select {
			case <-ctx.Done():
				logger.Info("Context cancelled, shutting down consumer loop.")
				return
			default:
				// Use FetchMessage to control when commits happen.
				msg, err := reader.FetchMessage(ctx)
				if err != nil {
					if errors.Is(err, context.Canceled) {
						return
					}
					logger.Error("Failed to fetch message", "error", err)
					continue
				}

				// 1. Process your message here
				logger.Info("Message processed", "partition", msg.Partition, "offset", msg.Offset)

				// 2. Track the processed message
				healthChecker.Track(ctx, pulseSegmentio.NewMessage(msg))

				// 3. Commit the message
				if err := reader.CommitMessages(ctx, msg); err != nil {
					logger.Error("Failed to commit message", "error", err)
				}
			}
		}
	}()

	logger.Info("Consumer group started. Press Ctrl+C to exit.")

	// Graceful Shutdown
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	<-sigterm
	logger.Info("Termination signal received, shutting down...")
	cancel()
}
```

-----

## Configuration

You can configure the `HealthChecker` using the `pulse.Config` struct.

| Field                | Type            | Description                                                                                                                                                             | Default                           |
| -------------------- | --------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------- |
| `Logger`             | `*slog.Logger`  | An instance of a structured logger (`slog`). If `nil`, logs are discarded.                                                                                              | `slog.New(slog.DiscardHandler)`   |
| `IgnoreBrokerErrors` | `bool`          | If `true`, the health check will pass even if the library fails to fetch the latest offset from Kafka. Useful for tolerating transient network issues.                   | `false`                           |
| `StuckTimeout`       | `time.Duration` | **Required.** The duration after which a non-progressing partition is checked against the broker to see if it's truly stuck. Must be greater than 0.                      | (none)                            |

See the [core library documentation](../../README.md) for `pulse.Config` and `pulse.HealthChecker` usage.