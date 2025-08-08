# Sarama Adapter

[![Godoc](https://pkg.go.dev/badge/github.com/vmyroslav/kafka-pulse-go/adapter/sarama)](https://pkg.go.dev/github.com/vmyroslav/kafka-pulse-go/adapter/sarama)

Kafka health monitoring adapter for [IBM Sarama](https://github.com/IBM/sarama) Kafka client.

## Installation

```bash
go get github.com/vmyroslav/kafka-pulse-go/adapter/sarama
```

## Quick Start

This example demonstrates how to integrate `kafka-pulse-go` into a standard Sarama consumer group application, exposing the health status via an HTTP endpoint for use with systems like Kubernetes.

### Step 1: Define the Consumer Group Handler

Create a handler that embeds the `pulse.Monitor` and implements the `sarama.ConsumerGroupHandler` interface.

**`ConsumerHandler.go`**:

```go
package main

import (
	"context"
	"log/slog"

	"github.com/IBM/sarama"
	"github.com/vmyroslav/kafka-pulse-go/pulse"
	pulseSarama "github.com/vmyroslav/kafka-pulse-go/adapter/sarama"
)

// ConsumerHandler represents a Sarama consumer group handler
type ConsumerHandler struct {
	logger        *slog.Logger
	healthChecker pulse.Monitor
}

// NewConsumerHandler creates a new instance of ConsumerHandler
func NewConsumerHandler(logger *slog.Logger, healthChecker pulse.Monitor) *ConsumerHandler {
	return &ConsumerHandler{
		logger:        logger,
		healthChecker: healthChecker,
	}
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (h *ConsumerHandler) Setup(session sarama.ConsumerGroupSession) error {
	h.logger.Info("Consumer group session is set up", "memberID", session.MemberID())
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (h *ConsumerHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	h.logger.Info("Consumer group session is cleaned up", "memberID", session.MemberID())
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
// Once the Messages() channel is closed, the handler must finish its processing
// loop and exit.
func (h *ConsumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// When this function exits, the claim is released. We must release the partition
	// from tracking to prevent false positives after a rebalance.
	// A deferred call ensures this happens reliably.
	defer h.healthChecker.Release(context.Background(), claim.Topic(), claim.Partition())

	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				h.logger.Info("Message channel closed, ending claim loop",
					"topic", claim.Topic(), "partition", claim.Partition())
				return nil
			}

			// 1. Process your message here
			h.logger.Info("Message processed",
				"topic", message.Topic, "partition", message.Partition, "offset", message.Offset)

			// 2. Track the processed message
			h.healthChecker.Track(context.Background(), pulseSarama.NewMessage(message))

			// 3. Mark the message as consumed
			session.MarkMessage(message, "")

		case <-session.Context().Done():
			// The session context is canceled when the consumer group is closing.
			h.logger.Info("Session context done, ending claim loop",
				"topic", claim.Topic(), "partition", claim.Partition())
			return nil
		}
	}
}
```

### Step 2: Set Up the Main Application

In your `main.go`, initialize Sarama, the `HealthChecker`, and the HTTP server.

**`main.go`**:

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

	"github.com/IBM/sarama"
	"github.com/vmyroslav/kafka-pulse-go/pulse"
	pulseSarama "github.com/vmyroslav/kafka-pulse-go/adapter/sarama"
)

const (
	kafkaBrokers = "localhost:9092"
	kafkaTopic   = "healthcheck-topic"
	consumerGroup = "pulse-checker-group"
	healthCheckPort = "8080"
)

func main() {
	// Setup logger
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	// 1. Initialize Sarama Client
	saramaConfig := sarama.NewConfig()
	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	saramaConfig.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	saramaConfig.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
        sarama.NewBalanceStrategyRoundRobin(),
    }
	client, err := sarama.NewClient([]string{kafkaBrokers}, saramaConfig)
	if err != nil {
		logger.Error("Failed to create Sarama client", "error", err)
		os.Exit(1)
	}
	defer client.Close()

	// 2. Initialize Pulse Health Checker
	pulseAdapter := pulseSarama.NewClientAdapter(client)
	pulseConfig := pulse.Config{
		Logger:       logger,
		StuckTimeout: 30 * time.Second, // consider a consumer stuck if no new messages are processed for 30s
		IgnoreBrokerErrors: false,     // fail health check if the broker can't be reached
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

	// 4. Start the Consumer Group
	cGroup, err := sarama.NewConsumerGroupFromClient(consumerGroup, client)
	if err != nil {
		logger.Error("Failed to create consumer group", "error", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	handler := NewConsumerHandler(logger, healthChecker)

	go func() {
		defer cGroup.Close()
		for {
			// `Consume` should be called inside an infinite loop, when a
			// server-side rebalance happens, the consumer session will end
			if err = cGroup.Consume(ctx, []string{kafkaTopic}, handler); err != nil {
				if errors.Is(err, sarama.ErrClosedConsumerGroup) {
					return
				}
				logger.Error("Error from consumer", "error", err)
			}
			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				return
			}
		}
	}()

	logger.Info("Consumer group started. Press Ctrl+C to exit.")

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

-----

See the [core library documentation](../../README.md) for `pulse.Config` and `pulse.HealthChecker` usage.