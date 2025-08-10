# Confluent Adapter

[![Godoc](https://pkg.go.dev/badge/github.com/vmyroslav/kafka-pulse-go/adapter/confluent)](https://pkg.go.dev/github.com/vmyroslav/kafka-pulse-go/adapter/confluent)

Kafka health monitoring adapter for [Confluent's Kafka Go client](https://github.com/confluentinc/confluent-kafka-go).

## Installation

```bash
go get github.com/vmyroslav/kafka-pulse-go/adapter/confluent
```

## Quick Start

This example demonstrates how to integrate `kafka-pulse-go` into a standard Confluent Kafka consumer application, exposing the health status via an HTTP endpoint for use with systems like Kubernetes. \
The confluent-kafka-go client uses a polling loop. We will call `healthChecker.Track()` when a message is processed and `healthChecker.Release()` when partitions are revoked during a rebalance.

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

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	pulseConfluent "github.com/vmyroslav/kafka-pulse-go/adapter/confluent"
	"github.com/vmyroslav/kafka-pulse-go/pulse"
)

const (
	kafkaBrokers    = "localhost:9092"
	kafkaTopic      = "healthcheck-topic"
	consumerGroup   = "pulse-checker-group-confluent"
	healthCheckPort = "8080"
)

func main() {
	// Setup logger
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	// 1. Initialize Confluent Kafka Config and Consumer
	kafkaConfig := &kafka.ConfigMap{
		"bootstrap.servers":  kafkaBrokers,
		"group.id":           consumerGroup,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": "false", // Recommended for at-least-once processing
	}
	consumer, err := kafka.NewConsumer(kafkaConfig)
	if err != nil {
		logger.Error("Failed to create Kafka consumer", "error", err)
		os.Exit(1)
	}
	defer consumer.Close()

	// 2. Initialize Pulse Health Checker
	// The adapter creates and manages its own producer for fetching offsets.
	pulseAdapter, err := pulseConfluent.NewClientAdapter(kafkaConfig)
	if err != nil {
		logger.Error("Failed to create pulse adapter", "error", err)
		os.Exit(1)
	}
	defer pulseAdapter.Close()
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

	// 4. Start the Consumer Poll Loop
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		// Subscribe to the topic
		err := consumer.SubscribeTopics([]string{kafkaTopic}, nil)
		if err != nil {
			logger.Error("Failed to subscribe to topics", "error", err)
			return
		}

		run := true
		for run {
			select {
			case <-ctx.Done():
				run = false
			default:
				// Poll for messages and errors
				ev := consumer.Poll(100)
				if ev == nil {
					continue
				}

				switch e := ev.(type) {
				case *kafka.Message:
					// 1. Process your message here
					logger.Info("Message processed", "topic", *e.TopicPartition.Topic, "offset", e.TopicPartition.Offset)

					// 2. Track the processed message
					healthChecker.Track(ctx, pulseConfluent.NewMessage(e))

					// 3. Mark the message as consumed (commit offset)
					if _, err := consumer.CommitMessage(e); err != nil {
						logger.Error("Failed to commit message", "error", err)
					}

				case kafka.Error:
					// Errors should be logged, and fatal errors may require exiting.
					logger.Error("Kafka consumer error", "code", e.Code(), "error", e.Error())
					if e.IsFatal() {
						run = false
					}

				default:
					// Other events (like PartitionEOF) can be ignored
					logger.Debug("Ignored event", "event", e)
				}
			}
		}
		logger.Info("Consumer loop shutting down")
	}()

	logger.Info("Consumer group started. Press Ctrl+C to exit.")

	// Graceful Shutdown
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	<-sigterm
	logger.Info("Termination signal received, shutting down...")
	cancel()

	// Give the consumer time to shut down cleanly
	time.Sleep(2 * time.Second)
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

-----

See the [core library documentation](../../README.md) for `pulse.Config` and `pulse.HealthChecker` usage.