[![Action Status](https://github.com/vmyroslav/kafka-pulse-go/actions/workflows/ci.yaml/badge.svg)](https://github.com/vmyroslav/kafka-pulse-go/actions/workflows/ci.yaml)

# kafka-pulse-go

A Go library for monitoring Kafka consumer health and detecting stuck consumers. \
It provides lag detection and stuck consumer identification to help ensure your Kafka consumers are processing messages effectively.

## Features

- **Consumer Health Monitoring**: Track message processing and detect stuck consumers
- **Lag Detection**: Distinguish between truly stuck consumers vs idle consumers
- **Multiple Kafka Client Support**: Ready-to-use adapters for popular Kafka clients:
  - **Sarama Adapter**: IBM Sarama Kafka client
  - **Segmentio Adapter**: segmentio/kafka-go client  
  - **Confluent Adapter**: Confluent's confluent-kafka-go client

## Quick Start

### Installation

```bash
go get github.com/vmyroslav/kafka-pulse-go
```

### Basic Usage

TODO: Add a simple example of how to use the library

```go
```

## Supported Kafka Clients

kafka-pulse-go provides adapters for multiple popular Kafka clients. Each adapter implements the required interfaces to enable health monitoring with your preferred Kafka library.

### Sarama (IBM Sarama)

```bash
go get github.com/vmyroslav/kafka-pulse-go/adapter/sarama
```

**Usage:**
```go
import (
    "github.com/IBM/sarama"
    adapter "github.com/vmyroslav/kafka-pulse-go/adapter/sarama"
    "github.com/vmyroslav/kafka-pulse-go/pulse"
)

// Create Sarama client
client, err := sarama.NewClient(brokers, saramaConfig)
if err != nil {
    // handle error
}

// Create pulse adapter
brokerClient := adapter.NewClientAdapter(client)

// Wrap Sarama messages for tracking
message := adapter.NewMessage(saramaConsumerMessage)
```

### Segmentio (segmentio/kafka-go)

```bash
go get github.com/vmyroslav/kafka-pulse-go/adapter/segmentio
```

**Usage:**
```go
import (
    "github.com/segmentio/kafka-go"
    adapter "github.com/vmyroslav/kafka-pulse-go/adapter/segmentio"
    "github.com/vmyroslav/kafka-pulse-go/pulse"
)

// Create pulse adapter with broker list
brokerClient := adapter.NewClientAdapter([]string{"localhost:9092"})

// Wrap kafka-go messages for tracking
message := adapter.NewMessage(kafkaMessage)
```

### Confluent (confluent-kafka-go)

```bash
go get github.com/vmyroslav/kafka-pulse-go/adapter/confluentic
```

**Usage:**
```go
import (
    "github.com/confluentinc/confluent-kafka-go/v2/kafka"
    adapter "github.com/vmyroslav/kafka-pulse-go/adapter/confluentic"
    "github.com/vmyroslav/kafka-pulse-go/pulse"
)

// Create Confluent consumer
consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
    "bootstrap.servers": "localhost:9092",
    "group.id":          "myGroup",
})
if err != nil {
    // handle error
}

// Create pulse adapter
brokerClient := adapter.NewClientAdapter(consumer)

// Wrap Confluent messages for tracking
message := adapter.NewMessage(confluentMessage)
```

## Configuration

TODO


## Examples

TODO

## Support

- Check the [examples/](examples/) directory for usage examples