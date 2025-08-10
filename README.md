[![Action Status](https://github.com/vmyroslav/kafka-pulse-go/actions/workflows/ci.yaml/badge.svg)](https://github.com/vmyroslav/kafka-pulse-go/actions/workflows/ci.yaml)
[![codecov](https://codecov.io/gh/vmyroslav/kafka-pulse-go/graph/badge.svg?token=I4VAB76KRW)](https://codecov.io/gh/vmyroslav/kafka-pulse-go)
[![Go Report Card](https://goreportcard.com/badge/github.com/vmyroslav/kafka-pulse-go)](https://goreportcard.com/report/github.com/vmyroslav/kafka-pulse-go)
[![Godoc](https://pkg.go.dev/badge/github.com/vmyroslav/kafka-pulse-go)](https://pkg.go.dev/github.com/vmyroslav/kafka-pulse-go)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

# kafka-pulse-go

`kafka-pulse-go` is a lightweight, dependency-free Go library designed to monitor the health of Kafka consumers. It helps you quickly detect if a consumer is stalled, stuck, or lagging significantly behind the topic log.

Based on techniques described in [PagerDuty's Kafka Health Checks](https://www.pagerduty.com/eng/kafka-health-checks/) article.

The core logic is decoupled from any specific Kafka client library, with ready-to-use adapters provided for popular clients.

## How It Works

The health checker operates on a simple principle:

1.  **Track:** Your consumer code calls `Track()` for every message it processes. The library records the message's offset and the current timestamp for that specific topic-partition.
2.  **Check:** Periodically (e.g., in an HTTP health check endpoint), you call `Healthy()`.
3.  **Verify:** To avoid false alarms for idle partitions (i.e., partitions with no new messages), the library performs a verification step. It queries the Kafka broker for the latest available offset on that topic-partition.
4.  **Diagnose:**
    * If the consumer's tracked offset is **less than** the broker's latest offset, the consumer is confirmed to be **stuck**. The health check fails.
    * If the consumer's tracked offset is **equal to or greater than** the broker's latest offset, the consumer is simply **idle and caught up**. The health check passes.

This mechanism reliably distinguishes between a healthy, idle consumer and a genuinely failing one.

-----

### Installation

```bash
go get github.com/vmyroslav/kafka-pulse-go
```

### Basic Usage

```go
import (
    "time"
    "github.com/vmyroslav/kafka-pulse-go/pulse"
)

// Create health checker with configuration
config := pulse.Config{
    StuckTimeout: 30 * time.Second,
    Logger:       logger, // optional
}
healthChecker := pulse.NewHealthChecker(config, brokerClient)

// Track messages during processing
healthChecker.TrackMessage(message)

// Check consumer health
isHealthy, err := healthChecker.IsHealthy(ctx)
```

## Kafka Client Adapters

Ready-to-use adapters for popular Kafka clients. Each adapter provides the required interfaces to enable health monitoring with your preferred Kafka library.

### Available Adapters

| Adapter | Package | Description |
|---------|---------|-------------|
| [Sarama](adapter/sarama/) | `github.com/vmyroslav/kafka-pulse-go/adapter/sarama` | IBM Sarama client adapter |
| [SegmentIO](adapter/segmentio/) | `github.com/vmyroslav/kafka-pulse-go/adapter/segmentio` | segmentio/kafka-go client adapter |
| [Confluent](adapter/confluent/) | `github.com/vmyroslav/kafka-pulse-go/adapter/confluent` | Confluent's kafka-go client adapter |

Each adapter directory contains detailed documentation and usage examples.