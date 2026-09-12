// Package config loads environment-backed service configuration.
package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	DatabaseURL       string
	RabbitURL         string
	ReceiverPort      string
	SimulatorPort     string
	QueueName         string
	DLQName           string
	Quantum           int
	HighWatermark     int
	LowWatermark      int
	LeaseDuration     time.Duration
	DeliveryTimeout   time.Duration
	MaxAttempts       int
	BaseBackoff       time.Duration
	WorkerConcurrency int
}

func Load() Config {
	return Config{
		DatabaseURL:       env("DATABASE_URL", "postgres://webhook:webhook@localhost:5432/webhooknotifier?sslmode=disable"),
		RabbitURL:         env("RABBITMQ_URL", "amqp://webhook:webhook@localhost:5672/"),
		ReceiverPort:      env("RECEIVER_PORT", "8080"),
		SimulatorPort:     env("SIMULATOR_PORT", "8084"),
		QueueName:         env("QUEUE_NAME", "webhook.events"),
		DLQName:           env("DLQ_NAME", "webhook.dead_letters"),
		Quantum:           envInt("DISPATCH_QUANTUM", 5),
		HighWatermark:     envInt("QUEUE_HIGH_WATERMARK", 1000),
		LowWatermark:      envInt("QUEUE_LOW_WATERMARK", 500),
		LeaseDuration:     envDuration("LEASE_DURATION", 30*time.Second),
		DeliveryTimeout:   envDuration("DELIVERY_TIMEOUT", 5*time.Second),
		MaxAttempts:       envInt("MAX_ATTEMPTS", 5),
		BaseBackoff:       envDuration("BASE_BACKOFF", time.Second),
		WorkerConcurrency: envInt("WORKER_CONCURRENCY", 4),
	}
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
func envInt(key string, fallback int) int {
	value, err := strconv.Atoi(env(key, ""))
	if err != nil {
		return fallback
	}
	return value
}
func envDuration(key string, fallback time.Duration) time.Duration {
	value, err := time.ParseDuration(env(key, ""))
	if err != nil {
		return fallback
	}
	return value
}
