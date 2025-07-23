package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/IBM/sarama"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
)

var kafkaContainerImage = "confluentinc/confluent-local:7.8.3"

type KafkaContainer struct {
	container *kafka.KafkaContainer
	brokers   []string
}

func StartKafkaContainer(ctx context.Context, logger *slog.Logger) (*KafkaContainer, error) {
	logger.Info("starting Kafka container...")

	kafkaContainer, err := kafka.Run(ctx,
		kafkaContainerImage,
		kafka.WithClusterID("test-cluster"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to start Kafka container: %w", err)
	}

	brokers, err := kafkaContainer.Brokers(ctx)
	if err != nil {
		_ = kafkaContainer.Terminate(ctx)
		return nil, fmt.Errorf("failed to get Kafka brokers: %w", err)
	}

	logger.Info("Kafka container started", "brokers", brokers)

	if err = waitForKafka(brokers, logger); err != nil {
		_ = kafkaContainer.Terminate(ctx)

		return nil, fmt.Errorf("Kafka not ready: %w", err)
	}

	return &KafkaContainer{
		container: kafkaContainer,
		brokers:   brokers,
	}, nil
}

func (kc *KafkaContainer) GetBrokers() []string {
	return kc.brokers
}

func (kc *KafkaContainer) CreateTopic(topic string, partitions int32, logger *slog.Logger) error {
	config := sarama.NewConfig()
	config.Version = sarama.V3_8_1_0

	admin, err := sarama.NewClusterAdmin(kc.brokers, config)
	if err != nil {
		return fmt.Errorf("failed to create cluster admin: %w", err)
	}
	defer func(admin sarama.ClusterAdmin) {
		_ = admin.Close()
	}(admin)

	topicDetail := &sarama.TopicDetail{
		NumPartitions:     partitions,
		ReplicationFactor: 1,
	}

	err = admin.CreateTopic(topic, topicDetail, false)
	if err != nil {
		return fmt.Errorf("failed to create topic %s: %w", topic, err)
	}

	logger.Info("Topic created", "topic", topic, "partitions", partitions)

	return nil
}

func (kc *KafkaContainer) ProduceMessages(topic string, count int, logger *slog.Logger) error {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll

	producer, err := sarama.NewSyncProducer(kc.brokers, config)
	if err != nil {
		return fmt.Errorf("failed to create producer: %w", err)
	}
	defer func(producer sarama.SyncProducer) {
		_ = producer.Close()
	}(producer)

	for i := 0; i < count; i++ {
		message := &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(fmt.Sprintf("Message %d - %s", i, time.Now().Format(time.RFC3339))),
		}

		partition, offset, err := producer.SendMessage(message)
		if err != nil {
			return fmt.Errorf("failed to send message %d: %w", i, err)
		}

		logger.Debug("Message sent",
			"message", i,
			"topic", topic,
			"partition", partition,
			"offset", offset)

		time.Sleep(500 * time.Millisecond)
	}

	logger.Info("Messages produced", "topic", topic, "count", count)
	return nil
}

func (kc *KafkaContainer) ProduceSingleMessage(topic string, messageNumber int, logger *slog.Logger) error {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll

	producer, err := sarama.NewSyncProducer(kc.brokers, config)
	if err != nil {
		return fmt.Errorf("failed to create producer: %w", err)
	}
	defer func(producer sarama.SyncProducer) {
		_ = producer.Close()
	}(producer)

	message := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(fmt.Sprintf("Continuous Message %d - %s", messageNumber, time.Now().Format(time.RFC3339))),
	}

	partition, offset, err := producer.SendMessage(message)
	if err != nil {
		return fmt.Errorf("failed to send message %d: %w", messageNumber, err)
	}

	logger.Debug("Continuous message sent",
		"message", messageNumber,
		"topic", topic,
		"partition", partition,
		"offset", offset)

	return nil
}

func (kc *KafkaContainer) Stop(ctx context.Context) error {
	if kc.container != nil {
		return kc.container.Terminate(ctx)
	}
	return nil
}

func waitForKafka(brokers []string, logger *slog.Logger) error {
	config := sarama.NewConfig()
	config.Version = sarama.V3_8_1_0

	maxRetries := 10
	for i := 0; i < maxRetries; i++ {
		client, err := sarama.NewClient(brokers, config)
		if err == nil {
			_ = client.Close()
			logger.Info("Kafka is ready")
			return nil
		}

		logger.Debug("Waiting for Kafka", "attempt", i+1, "error", err)
		time.Sleep(2 * time.Second)
	}

	return fmt.Errorf("kafka not ready after %d attempts", maxRetries)
}
