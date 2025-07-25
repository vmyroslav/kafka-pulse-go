package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	adapter "github.com/vmyroslav/kafka-pulse-go/adapter/sarama"
	"github.com/vmyroslav/kafka-pulse-go/pulse"
)

const (
	topic                     = "kafka-pulse-test-topic"
	consumerGroup             = "kafka-pulse-consumer-group"
	healthPort                = "8888"
	initialMessageCount       = 20
	continuousMessageInterval = 3 * time.Second
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	kafkaContainer, err := StartKafkaContainer(ctx, logger)
	if err != nil {
		logger.Error("Failed to start Kafka container", "error", err)
		os.Exit(1)
	}
	defer func(kafkaContainer *KafkaContainer, ctx context.Context) {
		_ = kafkaContainer.Stop(ctx)
	}(kafkaContainer, ctx)

	if err = kafkaContainer.CreateTopic(topic, 3, logger); err != nil {
		logger.Error("Failed to create topic", "error", err)
		os.Exit(1)
	}

	config := pulse.Config{
		Logger:       logger,
		StuckTimeout: 5 * time.Second, // reduced for faster demonstration
	}

	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = sarama.V3_8_1_0

	client, err := sarama.NewClient(kafkaContainer.GetBrokers(), saramaConfig)
	if err != nil {
		logger.Error("failed to create Kafka client", "error", err)
		os.Exit(1)
	}
	defer func(client sarama.Client) {
		_ = client.Close()
	}(client)

	brokerClient := adapter.NewClientAdapter(client)

	monitor, err := pulse.NewHealthChecker(config, brokerClient)
	if err != nil {
		logger.Error("Failed to create health checker", "error", err)
		os.Exit(1)
	}

	consumer, err := NewConsumer(kafkaContainer.GetBrokers(), consumerGroup, topic, monitor, logger)
	if err != nil {
		logger.Error("Failed to create consumer", "error", err)
		os.Exit(1)
	}

	healthServer := NewHealthServer(monitor, consumer, logger, healthPort)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err = consumer.Start(topic); err != nil {
			logger.Error("consumer failed", "error", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err = healthServer.Start(); err != nil {
			logger.Error("health server failed", "error", err)
		}
	}()

	// initial message production
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(5 * time.Second)

		logger.Info("Starting initial message production...")
		if err = kafkaContainer.ProduceMessages(topic, initialMessageCount, logger); err != nil {
			logger.Error("Failed to produce initial messages", "error", err)
		}
	}()

	// producer
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(1 * time.Second) // Wait for initial messages to be produced

		ticker := time.NewTicker(continuousMessageInterval)
		defer ticker.Stop()

		messageCounter := initialMessageCount
		for {
			select {
			case <-ticker.C:
				if err = kafkaContainer.ProduceSingleMessage(topic, messageCounter, logger); err != nil {
					logger.Error("failed to produce continuous message", "error", err)
				}
				messageCounter++
			case <-ctx.Done():
				return
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				healthCtx, healthCancel := context.WithTimeout(ctx, 5*time.Second)
				isHealthy, err := monitor.Healthy(healthCtx)
				details := map[string]interface{}{
					"consumer_paused":  consumer.IsPaused(),
					"processing_delay": consumer.GetProcessingDelay().String(),
				}
				if err != nil {
					details["error"] = err.Error()
				}
				healthCancel()

				statusIcon := "✅"
				if !isHealthy {
					statusIcon = "❌"
				}
				if consumer.IsPaused() {
					statusIcon = "⏸️"
				}

				logger.Info("📊 Health Check Status",
					"icon", statusIcon,
					"healthy", isHealthy,
					"consumer_paused", consumer.IsPaused(),
					"processing_delay", consumer.GetProcessingDelay().String(),
					"details", details)

			case <-ctx.Done():
				return
			}
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	logger.Info("🚀 Application started",
		"topic", topic,
		"consumer_group", consumerGroup,
		"health_port", healthPort,
		"stuck_timeout", "10s")

	logger.Info("📊 Health endpoints available:",
		"health", "http://localhost:"+healthPort+"/health",
		"readiness", "http://localhost:"+healthPort+"/health/ready",
		"liveness", "http://localhost:"+healthPort+"/health/live")

	logger.Info("🎛️  Control endpoints for stuck consumer demo:",
		"pause", "curl -X POST http://localhost:"+healthPort+"/control/pause",
		"resume", "curl -X POST http://localhost:"+healthPort+"/control/resume",
		"rebalance", "curl -X POST http://localhost:"+healthPort+"/control/rebalance",
		"slow", "curl -X POST http://localhost:"+healthPort+"/control/slow -H 'Content-Type: application/json' -d '{\"delay\":\"5s\"}'",
		"status", "curl http://localhost:"+healthPort+"/control/status")

	logger.Info("🔍 Demo Instructions:",
		"step1", "Wait for consumer to process initial messages (healthy status)",
		"step2", "Run pause command to simulate stuck consumer",
		"step3", "Watch health checks transition from healthy ✅ to unhealthy ❌ after 10s",
		"step4", "Run resume command to recover consumer",
		"step5", "Watch health checks return to healthy ✅")

	<-sigChan
	logger.Info("shutting down...")

	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	consumer.Stop()
	_ = healthServer.Stop(shutdownCtx)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		logger.Info("application stopped gracefully")
	case <-shutdownCtx.Done():
		logger.Warn("shutdown timeout exceeded")
	}
}
