[![Action Status](https://github.com/vmyroslav/kafka-pulse-go/actions/workflows/ci.yaml/badge.svg)](https://github.com/vmyroslav/kafka-pulse-go/actions/workflows/ci.yaml)
[![Go Report Card](https://goreportcard.com/badge/github.com/vmyroslav/kafka-pulse-go)](https://goreportcard.com/report/github.com/vmyroslav/kafka-pulse-go)
[![Godoc](https://pkg.go.dev/badge/github.com/vmyroslav/kafka-pulse-go)](https://pkg.go.dev/github.com/vmyroslav/kafka-pulse-go)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

# kafka-pulse-go

A Go library for monitoring Kafka consumer health and detecting stuck consumers. Based on techniques described in [PagerDuty's Kafka Health Checks](https://www.pagerduty.com/eng/kafka-health-checks/) article.

## Core Library

The core library provides health monitoring functionality that distinguishes between stuck consumers (behind available messages) vs idle consumers (caught up with no new messages).

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
| [Confluent](adapter/confluentic/) | `github.com/vmyroslav/kafka-pulse-go/adapter/confluentic` | Confluent's kafka-go client adapter |

Each adapter directory contains detailed documentation and usage examples.