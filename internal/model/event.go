// Package model defines webhook ingestion and delivery domain types.
package model

import (
	"encoding/json"
	"fmt"
	"net/url"
	"time"
)

type EventStatus string

const (
	StatusPending    EventStatus = "pending"
	StatusPublished  EventStatus = "published"
	StatusDelivered  EventStatus = "delivered"
	StatusRetriable  EventStatus = "retriable"
	StatusDeadLetter EventStatus = "dead_letter"
)

type IngestionRequest struct {
	AccountID      string          `json:"account_id"`
	DestinationURL string          `json:"destination_url"`
	IdempotencyKey string          `json:"idempotency_key"`
	Payload        json.RawMessage `json:"payload"`
}

type Event struct {
	ID                string          `json:"id"`
	AccountID         string          `json:"account_id"`
	DestinationURL    string          `json:"destination_url"`
	IdempotencyKey    string          `json:"idempotency_key"`
	Payload           json.RawMessage `json:"payload"`
	Status            EventStatus     `json:"status"`
	AttemptCount      int             `json:"attempt_count"`
	MaxAttempts       int             `json:"max_attempts"`
	NextRetryAt       *time.Time      `json:"next_retry_at,omitempty"`
	LastFailureReason string          `json:"last_failure_reason,omitempty"`
	CreatedAt         time.Time       `json:"created_at"`
}

func (request IngestionRequest) Validate() error {
	if request.AccountID == "" || request.DestinationURL == "" || request.IdempotencyKey == "" || len(request.Payload) == 0 {
		return fmt.Errorf("account_id, destination_url, idempotency_key, and payload are required")
	}
	parsed, err := url.ParseRequestURI(request.DestinationURL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return fmt.Errorf("destination_url must be an absolute URL")
	}
	var payload struct {
		EventName string `json:"event_name"`
		WebhookID string `json:"webhook_id"`
	}
	if err := json.Unmarshal(request.Payload, &payload); err != nil {
		return fmt.Errorf("payload must be valid JSON: %w", err)
	}
	if payload.WebhookID == "" {
		return fmt.Errorf("payload.webhook_id is required")
	}
	switch payload.EventName {
	case "subscriber.created", "subscriber.added_to_segment", "subscriber.unsubscribed":
	default:
		return fmt.Errorf("unsupported payload event_name")
	}
	return nil
}
