[![Action Status](https://github.com/vmyroslav/kafka-pulse-go/actions/workflows/ci.yaml/badge.svg)](https://github.com/vmyroslav/kafka-pulse-go/actions/workflows/ci.yaml)

# kafka-pulse-go

A Go library for monitoring Kafka consumer health and detecting stuck consumers. It provides lag detection and stuck consumer identification to help ensure your Kafka consumers are processing messages effectively.

## Features

- **Consumer Health Monitoring**: Track message processing and detect stuck consumers
- **Lag Detection**: Distinguish between truly stuck consumers vs idle consumers
- **Interface-Based Design**: Clean abstractions for different Kafka client implementations
- **Sarama Adapter**: Ready-to-use adapter for IBM Sarama Kafka client
- **Thread-Safe Operations**: Concurrent access support with proper synchronization
- **Comprehensive Testing**: 100% test coverage with integration tests
- **Configurable Error Handling**: Choose whether to fail on broker errors or treat them as transient

## Quick Start

### Installation

```bash
go get github.com/vmyroslav/kafka-pulse-go
```

### Basic Usage

```go
package main

import (
    "context"
    "log/slog"
    "time"

    "github.com/vmyroslav/kafka-pulse-go/pulse"
    "github.com/vmyroslav/kafka-pulse-go/adapter/sarama"
)

func main() {
    // Create Kafka client (using Sarama)
    client, err := sarama.NewClient([]string{"localhost:9092"}, sarama.NewConfig())
    if err != nil {
        panic(err)
    }

    // Create health checker
    hc, err := pulse.NewHealthChecker(pulse.Config{
        Logger:       slog.Default(),
        StuckTimeout: 30 * time.Second,
        // IgnoreBrokerErrors: true, // Set to true to ignore broker errors (default: false)
    }, sarama.NewClientAdapter(client))
    if err != nil {
        panic(err)
    }

    // Track a message
    ctx := context.Background()
    msg := &sarama.Message{
        ConsumerMessage: &sarama.ConsumerMessage{
            Topic:     "orders",
            Partition: 0,
            Offset:    12345,
        },
    }
    hc.Track(ctx, msg)

    // Check health
    healthy, err := hc.Healthy(ctx)
    if err != nil {
        log.Fatal("Health check failed:", err)
    }
    
    if healthy {
        log.Println("Consumer is healthy")
    } else {
        log.Println("Consumer is stuck or unhealthy")
    }
}
```

### Configuration Options

```go
// Default configuration (fails on broker errors)
config := pulse.Config{
    Logger:       slog.Default(),
    StuckTimeout: 30 * time.Second,
    // IgnoreBrokerErrors defaults to false (fail on errors)
}

// Ignore broker errors (legacy behavior)
configIgnoreErrors := pulse.Config{
    Logger:             slog.Default(),
    StuckTimeout:       30 * time.Second,
    IgnoreBrokerErrors: true, // Ignore broker errors
}

// Explicitly fail on broker errors (same as default)
configFailOnErrors := pulse.Config{
    Logger:             slog.Default(),
    StuckTimeout:       30 * time.Second,
    IgnoreBrokerErrors: false, // Fail on broker errors (default)
}
```

### HTTP Server Integration

```go
func healthHandler(hc *pulse.HealthChecker) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        healthy, err := hc.Healthy(r.Context())
        if err != nil {
            http.Error(w, err.Error(), http.StatusInternalServerError)
            return
        }
        
        if healthy {
            w.WriteHeader(http.StatusOK)
            fmt.Fprintf(w, `{"status": "healthy"}`)
        } else {
            w.WriteHeader(http.StatusServiceUnavailable)
            fmt.Fprintf(w, `{"status": "unhealthy"}`)
        }
    }
}
```

## Configuration

### FailOnBrokerErrors

The `FailOnBrokerErrors` configuration controls how the health checker behaves when it encounters broker connection errors:

- **`nil` (default)**: Fails the health check when broker errors occur (returns `false, error`)
- **`true`**: Explicitly fails the health check on broker errors
- **`false`**: Ignores broker errors and continues health checking (legacy behavior)

```go
// Default behavior - fail on broker errors
config := pulse.Config{
    Logger:       slog.Default(),
    StuckTimeout: 30 * time.Second,
}

// Legacy behavior - ignore broker errors
config := pulse.Config{
    Logger:             slog.Default(),
    StuckTimeout:       30 * time.Second,
    FailOnBrokerErrors: &[]bool{false}[0], // or use helper: boolPtr(false)
}
```

This setting is useful for different deployment scenarios:
- **Production**: Default (`true`) for immediate failure detection
- **Development/Testing**: Set to `false` for more resilient behavior during infrastructure issues

## Examples

This repository includes comprehensive examples demonstrating real-world usage:

### 1. Integration Test
- **Location**: `adapter/sarama/integration_test.go`
- **Features**: Sarama adapter integration with mock broker
- **Usage**: `cd adapter/sarama && go test -v -run TestSaramaIntegration`

### 2. HTTP Server
- **Location**: `examples/http-server/`
- **Features**: REST API for health checks and metrics
- **Usage**: `cd examples/http-server && go run main.go`
- **Endpoints**:
  - `GET /health` - Health check status
  - `GET /metrics` - Consumer metrics
  - `POST /consumer/track` - Track message
  - `DELETE /consumer/partition/{topic}/{partition}` - Release partition

### 3. Complex Consumer
- **Location**: `examples/complex-consumer/`
- **Features**: Multiple consumer scenarios simulation
- **Usage**: `cd examples/complex-consumer && go run main.go -scenario healthy`
- **Scenarios**: healthy, stuck, mixed, rebalancing, broker-issues

### 4. Docker Environment
- **Location**: `examples/docker-compose.yml`
- **Features**: Complete Kafka ecosystem with monitoring
- **Usage**: `cd examples && make up`
- **Services**: Kafka, Zookeeper, Kafka UI, Prometheus, Grafana

## Architecture

### Core Components

**pulse/ package** - Main monitoring library:
- `Config` - Configuration with logger, stuck timeout, and error handling settings
- `HealthChecker` - Main monitoring component that tracks consumer progress
- `tracker` - Thread-safe offset tracking with timestamps
- `Monitor` interface - Defines tracking, release, and health check operations
- `TrackableMessage` interface - Abstraction for trackable Kafka messages
- `BrokerClient` interface - Abstraction for fetching latest offsets from brokers

### Key Architecture Patterns

1. **Interface-based Design**: Core functionality uses interfaces (`Monitor`, `TrackableMessage`, `BrokerClient`) to enable different Kafka client implementations

2. **Adapter Pattern**: The `adapter/sarama/` directory provides Sarama-specific implementations:
   - `Message` wraps `sarama.ConsumerMessage` to implement `TrackableMessage`
   - `clientAdapter` wraps `sarama.Client` to implement `BrokerClient`

3. **Health Check Logic**: Distinguishes between truly stuck consumers (behind available messages) vs idle consumers (caught up but no new messages)

4. **Thread Safety**: The `tracker` component uses RWMutex for concurrent access and creates deep copies to prevent data races

5. **Configurable Error Handling**: Allows choosing between failing fast on broker errors vs treating them as transient

## Development Commands

All development commands use the Task automation tool (Taskfile.yml):

- `task` - Show all available tasks
- `task test` - Run tests with coverage (60s timeout, race detection)
- `task lint` - Run golangci-lint checks with .golangci.yml config
- `task fmt` - Format code with gofumpt and apply lint fixes
- `task clean` - Clean dependencies (go mod tidy) and format code
- `task update-deps` - Update all Go dependencies to latest versions
- `task ci:prepare-release VERSION=X.Y.Z` - Prepare a semantic version release

## Testing

The library includes comprehensive tests with 100% coverage:

```bash
# Run all tests
task test

# Run specific test suites
go test ./pulse/...
go test ./adapter/sarama/...

# Run integration tests
cd adapter/sarama && go test -v -run TestSaramaIntegration

# Run examples tests
cd examples && make test
```

## Module Structure

- Root module: `github.com/vmyroslav/kafka-pulse-go` (Go 1.24)
- Adapter submodule: `adapter/sarama/` has its own go.mod with IBM Sarama dependency

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests: `task test`
5. Run linter: `task lint`
6. Submit a pull request

## License

See [LICENSE](LICENSE) file for details.

## Support

- Check the [examples/](examples/) directory for comprehensive usage examples
- Review the [CLAUDE.md](CLAUDE.md) file for development guidelines
- Open an issue for bugs or feature requests