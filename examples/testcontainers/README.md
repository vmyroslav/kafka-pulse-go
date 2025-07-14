# Kafka Pulse Test Containers Example

This example demonstrates how to use the `kafka-pulse-go` library with real Kafka containers using testcontainers-go and the Sarama adapter.

## Features

- **Real Kafka Environment**: Uses testcontainers to bootstrap a real Kafka cluster
- **Sarama Integration**: Demonstrates usage with the IBM Sarama Kafka client
- **Health Monitoring**: Exposes consumer health via HTTP endpoints
- **Lag Detection**: Shows how the library detects stuck consumers vs idle consumers

## What This Example Does

1. **Starts Kafka Container**: Bootstraps a Confluent Kafka container using testcontainers
2. **Creates Topic**: Creates a test topic with 3 partitions
3. **Sets Up Consumer**: Creates a Sarama consumer group that tracks messages using kafka-pulse
4. **Exposes Health Endpoints**: Provides HTTP endpoints for health monitoring
5. **Produces Messages**: Generates test messages to demonstrate consumer tracking
6. **Monitors Health**: Periodically checks and logs consumer health status

## Running the Example

### Prerequisites

- Go 1.24+
- Docker (for testcontainers)

### Steps

1. **Install Dependencies**:
   ```bash
   cd examples/testcontainers
   go mod tidy
   ```

2. **Run the Example**:
   ```bash
   go run .
   ```

3. **Monitor Health Endpoints**:
   - **General Health**: http://localhost:8080/health
   - **Readiness**: http://localhost:8080/health/ready  
   - **Liveness**: http://localhost:8080/health/live

## Health Endpoints

### `/health`
Returns detailed health information including:
- Consumer lag status
- Stuck consumer detection
- Processing timestamps

Example response:
```json
{
  "status": "healthy",
  "timestamp": "2024-01-15T10:30:00Z",
  "details": {
    "test-topic": {
      "0": {"lag": 0, "stuck": false},
      "1": {"lag": 5, "stuck": false},
      "2": {"lag": 0, "stuck": false}
    }
  }
}
```

### `/health/ready`
Kubernetes-style readiness probe - returns 200 if consumer is healthy, 503 if not.

### `/health/live`  
Kubernetes-style liveness probe - always returns 200 (application is alive).

## Understanding the Output

The application will log:
- Kafka container startup
- Topic creation
- Consumer startup and message processing
- Periodic health check results
- Message production progress

Watch for log entries like:
```
Health check result healthy=true details=map[test-topic:map[0:map[lag:0 stuck:false]]]
```

## Consumer Behavior

The example consumer:
- Processes messages with a 100ms delay to simulate work
- Tracks each message with the pulse monitor
- Releases tracking after processing
- Commits offsets after successful processing

This demonstrates how kafka-pulse can distinguish between:
- **Healthy consumers**: Processing messages normally
- **Stuck consumers**: Behind on available messages
- **Idle consumers**: Caught up with no new messages

## Customization

You can modify:
- `stuckTimeout` in the pulse.Config (currently 30s)
- Message production rate and count
- Consumer processing delay
- Health check intervals

## Cleanup

The application handles graceful shutdown on SIGINT/SIGTERM and will:
- Stop the consumer
- Shutdown the health server  
- Terminate the Kafka container
- Clean up all resources