package kafkaclient

import (
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

type rebalanceLogger struct {
	mu             sync.Mutex
	lastAssignment string
}

func (l *rebalanceLogger) Printf(
	format string,
	args ...interface{},
) {
	message := fmt.Sprintf(format, args...)

	l.mu.Lock()
	defer l.mu.Unlock()

	if strings.HasPrefix(
		message,
		"subscribed to topics and partitions:",
	) {
		l.lastAssignment = strings.TrimSpace(
			strings.TrimPrefix(
				message,
				"subscribed to topics and partitions:",
			),
		)

		fmt.Println(
			"assigned:",
			l.lastAssignment,
		)

		return
	}

	if strings.HasPrefix(
		message,
		"stopped commit for group",
	) {
		if l.lastAssignment != "" {
			fmt.Println(
				"revoked:",
				l.lastAssignment,
			)

			l.lastAssignment = ""
		}

		return
	}
}

func NewWriter(
	broker string,
	topic string,
) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(broker),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
	}
}

func NewReader(
	broker string,
	topic string,
	group string,
	commitInterval time.Duration,
	readMaxBytes int,
) *kafka.Reader {
	logger := &rebalanceLogger{}

	return kafka.NewReader(
		kafka.ReaderConfig{
			Brokers: []string{
				broker,
			},
			Topic:                 topic,
			GroupID:               group,
			MinBytes:              1,
			MaxBytes:              readMaxBytes,
			CommitInterval:        commitInterval,
			WatchPartitionChanges: true,
			Logger:                logger,
			ErrorLogger:           log.New(os.Stderr, "[kafka-error] ", log.LstdFlags),
		},
	)
}
