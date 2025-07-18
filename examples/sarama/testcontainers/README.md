# Kafka Consumer Stuck Detection Demo

This example demonstrates the kafka-pulse-go library's ability to detect stuck Kafka consumers using **dynamic control mechanisms** for interactive demonstration.

## Features

- **Dynamic Consumer Control**: Pause/resume consumer processing at runtime
- **Real Kafka Environment**: Uses testcontainers to bootstrap a real Kafka cluster
- **Sarama Integration**: Demonstrates usage with the IBM Sarama Kafka client
- **Real-time Health Monitoring**: Health checks every 5 seconds with visual indicators
- **Continuous Message Production**: Background message production to simulate real-world scenarios
- **HTTP Control API**: RESTful endpoints to control consumer behavior
- **Stuck Detection**: Detects when consumer stops processing messages for >10 seconds (configurable)

## Interactive Demo Flow

1. **Starts Kafka Container**: Bootstraps a Confluent Kafka container using testcontainers
2. **Creates Topic**: Creates a test topic with 3 partitions  
3. **Sets Up Consumer**: Creates a Sarama consumer group that tracks messages using kafka-pulse
4. **Exposes Control & Health APIs**: Provides HTTP endpoints for dynamic consumer control and health monitoring
5. **Continuous Production**: Generates messages every 3 seconds to simulate real-world scenarios
6. **Real-time Monitoring**: Health checks every 5 seconds with visual status indicators

## Running the Demo

### Prerequisites

- Go 1.24+
- Docker (for testcontainers)

### Steps

1. **Start the Application**:
   ```bash
   cd examples/testcontainers
   go run .
   ```

2. **Watch Initial Healthy State**:
   Look for health check logs showing:
   ```
   📊 Health Check Status icon=✅ healthy=true consumer_paused=false processing_delay=100ms
   ```

3. **Simulate Stuck Consumer**:
   ```bash
   curl -X POST http://localhost:8080/control/pause
   ```

4. **Watch Health Transition**:
   After 10 seconds, health checks will show:
   ```
   📊 Health Check Status icon=❌ healthy=false consumer_paused=true processing_delay=100ms
   ```

5. **Recover Consumer**:
   ```bash
   curl -X POST http://localhost:8080/control/resume
   ```

## Available Endpoints

### Health Endpoints
- **General Health**: http://localhost:8080/health
- **Readiness**: http://localhost:8080/health/ready  
- **Liveness**: http://localhost:8080/health/live

### Control Endpoints (NEW!)
- **Pause Consumer**: `POST /control/pause` - Simulates stuck consumer
- **Resume Consumer**: `POST /control/resume` - Recovers from stuck state
- **Set Processing Delay**: `POST /control/slow` - Simulates slow consumer
- **Get Status**: `GET /control/status` - Current consumer state

## API Examples

### Control Commands

**Pause Consumer (Simulate Stuck)**:
```bash
curl -X POST http://localhost:8080/control/pause
# Response: {"status":"success","paused":true,"processing_delay":"100ms","message":"Consumer paused - simulating stuck consumer"}
```

**Resume Consumer (Recover)**:
```bash
curl -X POST http://localhost:8080/control/resume  
# Response: {"status":"success","paused":false,"processing_delay":"100ms","message":"Consumer resumed - recovering from stuck state"}
```

**Set Slow Processing**:
```bash
curl -X POST http://localhost:8080/control/slow \
  -H 'Content-Type: application/json' \
  -d '{"delay":"5s"}'
# Response: {"status":"success","paused":false,"processing_delay":"5s","message":"Processing delay updated to 5s"}
```

**Check Status**:
```bash
curl http://localhost:8080/control/status
# Response: {"status":"success","paused":false,"processing_delay":"100ms","message":"Current consumer status"}
```

### Health Responses

**Healthy Consumer**:
```json
{
  "status": "healthy",
  "timestamp": "2024-01-15T10:30:00Z",
  "details": {
    "consumer_paused": false,
    "processing_delay": "100ms",
    "stuck_timeout": "10s"
  }
}
```

**Stuck Consumer**:
```json
{
  "status": "unhealthy", 
  "timestamp": "2024-01-15T10:30:45Z",
  "details": {
    "consumer_paused": true,
    "processing_delay": "100ms", 
    "stuck_timeout": "10s",
    "warning": "Consumer is manually paused - simulating stuck state",
    "error": "consumer appears to be stuck"
  }
}
```

## Demo Scenarios

### Scenario 1: Normal Healthy Operation
```
📊 Health Check Status icon=✅ healthy=true consumer_paused=false processing_delay=100ms
```

### Scenario 2: Stuck Consumer Detection
1. Run: `curl -X POST http://localhost:8080/control/pause`
2. Wait 10+ seconds  
3. Observe: `📊 Health Check Status icon=❌ healthy=false consumer_paused=true`

### Scenario 3: Consumer Recovery
1. Run: `curl -X POST http://localhost:8080/control/resume`
2. Observe: `📊 Health Check Status icon=✅ healthy=true consumer_paused=false`

### Scenario 4: Slow Consumer Simulation
1. Run: `curl -X POST http://localhost:8080/control/slow -H 'Content-Type: application/json' -d '{"delay":"5s"}'`
2. Consumer will still be healthy but processing slowly

## Key Benefits Demonstrated

1. **Real-time Detection**: Immediately identifies when consumers stop processing
2. **False Positive Prevention**: Distinguishes stuck consumers from idle (caught-up) consumers
3. **Operational Control**: Allows runtime manipulation for testing and debugging
4. **Production Ready**: Uses real Kafka with proper consumer group handling
5. **Monitoring Integration**: HTTP endpoints compatible with monitoring systems
