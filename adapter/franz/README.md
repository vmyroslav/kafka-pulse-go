# Franz-go Adapter

[![Godoc](https://pkg.go.dev/badge/github.com/vmyroslav/kafka-pulse-go/adapter/franz)](https://pkg.go.dev/github.com/vmyroslav/kafka-pulse-go/adapter/franz)

Kafka health monitoring adapter for [franz-go](https://github.com/twmb/franz-go) Kafka client.

## Installation

```bash
go get github.com/vmyroslav/kafka-pulse-go/adapter/franz
```

## Quick Start

This example demonstrates how to integrate `kafka-pulse-go` into a franz-go consumer application, exposing the health status via an HTTP endpoint for use with systems like Kubernetes.

### Example Application

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

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/vmyroslav/kafka-pulse-go/adapter/franz"
	"github.com/vmyroslav/kafka-pulse-go/pulse"
)

const (
	kafkaBrokers    = "localhost:9092"
	kafkaTopic      = "healthcheck-topic"
	consumerGroup   = "pulse-checker-group"
	healthCheckPort = "8080"
)

func main() {
	// Setup logger
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	// 1. Initialize franz-go Client
	client, err := kgo.NewClient(
		kgo.SeedBrokers(kafkaBrokers),
		kgo.ConsumerGroup(consumerGroup),
		kgo.ConsumeTopics(kafkaTopic),
		kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelError, nil)),
	)
	if err != nil {
		logger.Error("Failed to create franz-go client", "error", err)
		os.Exit(1)
	}
	defer client.Close()

	// 2. Initialize Pulse Health Checker
	pulseAdapter, err := franz.NewClientAdapter(client)
	if err != nil {
		logger.Error("Failed to create franz adapter", "error", err)
		os.Exit(1)
	}
	defer pulseAdapter.Close()

	pulseConfig := pulse.Config{
		Logger:             logger,
		StuckTimeout:       30 * time.Second, // consider a consumer stuck if no new messages are processed for 30s
		IgnoreBrokerErrors: false,            // fail health check if the broker can't be reached
	}
	healthChecker, err := pulse.NewHealthChecker(pulseConfig, pulseAdapter)
	if err != nil {
		logger.Error("Failed to create health checker", "error", err)
		os.Exit(1)
	}

	// 3. Setup HTTP Health Check Endpoint
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

	// 4. Start consuming messages
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		for {
			fetches := client.PollFetches(ctx)
			if fetches.IsClientClosed() {
				return
			}

			// Handle errors
			if errs := fetches.Errors(); len(errs) > 0 {
				for _, err := range errs {
					logger.Error("Fetch error", "error", err.Err)
				}
			}

			// Process records
			fetches.EachPartition(func(p kgo.FetchTopicPartition) {
				// When partition is revoked during rebalance, release it from tracking
				// to prevent false positives
				defer func() {
					if ctx.Err() != nil {
						healthChecker.Release(context.Background(), p.Topic, p.Partition)
					}
				}()

				for _, record := range p.Records {
					// 1. Process your message here
					logger.Info("Message processed",
						"topic", record.Topic,
						"partition", record.Partition,
						"offset", record.Offset,
					)

					// 2. Track the processed message
					healthChecker.Track(ctx, franz.NewMessage(record))
				}
			})
		}
	}()

	logger.Info("Consumer started. Press Ctrl+C to exit.")

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

| Field                | Type            | Description                                                                                                                                                             | Default                         |
| -------------------- | --------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------- |
| `Logger`             | `*slog.Logger`  | An instance of a structured logger (`slog`). If `nil`, logs are discarded.                                                                                              | `slog.New(slog.DiscardHandler)` |
| `IgnoreBrokerErrors` | `bool`          | If `true`, the health check will pass even if the library fails to fetch the latest offset from Kafka. Useful for tolerating transient network issues.                   | `false`                         |
| `StuckTimeout`       | `time.Duration` | **Required.** The duration after which a non-progressing partition is checked against the broker to see if it's truly stuck. Must be greater than 0.                      | (none)                          |

-----

## Franz-go Specific Notes

- The adapter uses `kadm.Client` (franz-go's admin client) for fetching partition offsets
- `Close()` is a no-op for the franz adapter since `kadm.Client` is lightweight and doesn't require explicit cleanup
- The underlying `kgo.Client` lifecycle should be managed by your application

-----

See the [core library documentation](../../README.md) for `pulse.Config` and `pulse.HealthChecker` usage.
