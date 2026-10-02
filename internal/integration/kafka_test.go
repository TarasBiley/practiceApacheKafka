package integration

import (
	"context"
	"testing"
	"time"

	"audit-service/internal/kafkaclient"

	kafkago "github.com/segmentio/kafka-go"
	kafkacontainer "github.com/testcontainers/testcontainers-go/modules/kafka"
)

func TestKafkaProducerConsumer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	const (
		topic          = "user-actions"
		commitInterval = 5 * time.Second
		readMaxBytes   = 1024 * 1024
		key            = "123"
		payload        = `{"user_id":"123","action":"login"}`
	)

	kafkaContainer, err := kafkacontainer.Run(
		ctx,
		"confluentinc/confluent-local:7.5.0",
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		if err := kafkaContainer.Terminate(cleanupCtx); err != nil {
			t.Errorf("terminate Kafka container: %v", err)
		}
	})

	brokers, err := kafkaContainer.Brokers(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Создаём topic user-actions.
	conn, err := kafkago.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	deadline, _ := ctx.Deadline()
	if err := conn.SetDeadline(deadline); err != nil {
		t.Fatal(err)
	}

	err = conn.CreateTopics(
		kafkago.TopicConfig{
			Topic:             topic,
			NumPartitions:     3,
			ReplicationFactor: 1,
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	writer := kafkaclient.NewWriter(brokers[0], topic)
	defer writer.Close()

	reader := kafkaclient.NewReader(brokers[0], topic, "test-group", commitInterval, readMaxBytes)
	defer reader.Close()
	readerConfig := reader.Config()
	if readerConfig.CommitInterval != commitInterval || readerConfig.MaxBytes != readMaxBytes {
		t.Fatalf("reader settings: got commit interval %s and max bytes %d, want %s and %d",
			readerConfig.CommitInterval, readerConfig.MaxBytes, commitInterval, readMaxBytes)
	}

	err = writer.WriteMessages(
		ctx,
		kafkago.Message{
			Key:   []byte(key),
			Value: []byte(payload),
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	readCtx, readCancel := context.WithTimeout(
		ctx,
		30*time.Second,
	)
	defer readCancel()

	msg, err := reader.FetchMessage(readCtx)
	if err != nil {
		t.Fatal(err)
	}

	if string(msg.Key) != key {
		t.Fatalf(
			"expected key %s, got %s",
			key,
			string(msg.Key),
		)
	}
	if string(msg.Value) != payload {
		t.Fatalf("expected payload %s, got %s", payload, msg.Value)
	}
	if err := reader.CommitMessages(readCtx, msg); err != nil {
		t.Fatal(err)
	}

	t.Log(
		"received message:",
		string(msg.Value),
	)
}
