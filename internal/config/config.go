package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	DatabaseURL         string
	KafkaBroker         string
	KafkaTopic          string
	KafkaGroup          string
	HTTPPort            string
	AuditPublishTimeout time.Duration

	KafkaCommitInterval time.Duration
	KafkaReadMaxBytes   int

	AnalyticsInterval time.Duration
	AnalyticsWindow   time.Duration
}

func Load() Config {
	return Config{
		DatabaseURL: getEnv(
			"DATABASE_URL",
			"postgres://audit:audit@localhost:5435/audit?sslmode=disable",
		),

		KafkaBroker: getEnv(
			"KAFKA_BROKER",
			"localhost:9092",
		),

		KafkaTopic: getEnv(
			"KAFKA_TOPIC",
			"user-actions",
		),

		KafkaGroup: getEnv(
			"KAFKA_GROUP",
			"analytics-group",
		),

		HTTPPort: getEnv(
			"HTTP_PORT",
			"8080",
		),

		AuditPublishTimeout: getPositiveDurationEnv(
			"AUDIT_PUBLISH_TIMEOUT",
			30*time.Second,
		),

		KafkaCommitInterval: getDurationEnv(
			"KAFKA_COMMIT_INTERVAL",
			5*time.Second,
		),

		KafkaReadMaxBytes: getIntEnv(
			"KAFKA_READ_MAX_BYTES",
			10*1024*1024,
		),

		AnalyticsInterval: getDurationEnv(
			"ANALYTICS_INTERVAL",
			5*time.Minute,
		),

		AnalyticsWindow: getDurationEnv(
			"ANALYTICS_WINDOW",
			1*time.Hour,
		),
	}
}

func getEnv(key string, fallback string) string {
	value := os.Getenv(key)

	if value == "" {
		return fallback
	}

	return value
}

func getDurationEnv(
	key string,
	fallback time.Duration,
) time.Duration {
	value := os.Getenv(key)

	if value == "" {
		return fallback
	}

	duration, err := time.ParseDuration(value)
	if err != nil {
		return fallback
	}

	return duration
}

func getIntEnv(
	key string,
	fallback int,
) int {
	value := os.Getenv(key)

	if value == "" {
		return fallback
	}

	number, err := strconv.Atoi(value)
	if err != nil || number <= 0 {
		return fallback
	}

	return number
}

func getPositiveDurationEnv(key string, fallback time.Duration) time.Duration {
	duration := getDurationEnv(key, fallback)
	if duration <= 0 {
		return fallback
	}
	return duration
}
