// Package simulator manages local webhook registrations and delivery captures.
package simulator

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var supportedEventTypes = map[string]struct{}{
	"subscriber.created":          {},
	"subscriber.added_to_segment": {},
	"subscriber.unsubscribed":     {},
}

type Account struct {
	ID        string    `json:"id"`
	CreatedAt time.Time `json:"created_at"`
}

type Webhook struct {
	ID              string   `json:"id"`
	AccountID       string   `json:"account_id"`
	EventTypes      []string `json:"event_types"`
	ResponseStatus  int      `json:"response_status"`
	ResponseDelayMS int      `json:"response_delay_ms"`
}

type Delivery struct {
	ID             int64           `json:"id"`
	WebhookID      string          `json:"webhook_id"`
	Payload        json.RawMessage `json:"payload"`
	Headers        json.RawMessage `json:"headers"`
	ResponseStatus int             `json:"response_status"`
	ReceivedAt     time.Time       `json:"received_at"`
}

type Registry struct {
	pool *pgxpool.Pool
}

func NewRegistry(pool *pgxpool.Pool) *Registry {
	return &Registry{pool: pool}
}

func (registry *Registry) CreateAccount(ctx context.Context, accountID string) (Account, error) {
	accountID = strings.TrimSpace(accountID)
	if accountID == "" {
		return Account{}, fmt.Errorf("account id is required")
	}
	var account Account
	err := registry.pool.QueryRow(ctx, `INSERT INTO simulator_accounts (id) VALUES ($1) RETURNING id, created_at`, accountID).Scan(&account.ID, &account.CreatedAt)
	return account, err
}

func (registry *Registry) ListAccounts(ctx context.Context) ([]Account, error) {
	rows, err := registry.pool.Query(ctx, `SELECT id, created_at FROM simulator_accounts ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	accounts := make([]Account, 0)
	for rows.Next() {
		var account Account
		if err := rows.Scan(&account.ID, &account.CreatedAt); err != nil {
			return nil, err
		}
		accounts = append(accounts, account)
	}
	return accounts, rows.Err()
}

func (registry *Registry) CreateWebhook(ctx context.Context, accountID string, eventTypes []string, responseStatus, responseDelayMS int) (Webhook, error) {
	if err := validateWebhook(eventTypes, responseStatus, responseDelayMS); err != nil {
		return Webhook{}, err
	}
	webhook := Webhook{ID: "wh_" + uuid.NewString(), AccountID: accountID, EventTypes: eventTypes, ResponseStatus: responseStatus, ResponseDelayMS: responseDelayMS}
	err := registry.pool.QueryRow(ctx, `INSERT INTO simulator_webhooks (id, account_id, event_types, response_status, response_delay_ms) VALUES ($1,$2,$3,$4,$5) RETURNING id, account_id, event_types, response_status, response_delay_ms`, webhook.ID, webhook.AccountID, webhook.EventTypes, webhook.ResponseStatus, webhook.ResponseDelayMS).Scan(&webhook.ID, &webhook.AccountID, &webhook.EventTypes, &webhook.ResponseStatus, &webhook.ResponseDelayMS)
	return webhook, err
}

func (registry *Registry) ListWebhooks(ctx context.Context) ([]Webhook, error) {
	rows, err := registry.pool.Query(ctx, `SELECT id, account_id, event_types, response_status, response_delay_ms FROM simulator_webhooks ORDER BY account_id, id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return collectWebhooks(rows)
}

func (registry *Registry) FindWebhooksForEvent(ctx context.Context, accountID, eventType string) ([]Webhook, error) {
	rows, err := registry.pool.Query(ctx, `SELECT id, account_id, event_types, response_status, response_delay_ms FROM simulator_webhooks WHERE account_id = $1 AND $2 = ANY(event_types) ORDER BY id`, accountID, eventType)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return collectWebhooks(rows)
}

func (registry *Registry) GetWebhook(ctx context.Context, webhookID string) (Webhook, error) {
	var webhook Webhook
	err := registry.pool.QueryRow(ctx, `SELECT id, account_id, event_types, response_status, response_delay_ms FROM simulator_webhooks WHERE id = $1`, webhookID).Scan(&webhook.ID, &webhook.AccountID, &webhook.EventTypes, &webhook.ResponseStatus, &webhook.ResponseDelayMS)
	return webhook, err
}

func (registry *Registry) RecordDelivery(ctx context.Context, webhookID string, payload json.RawMessage, headers json.RawMessage, responseStatus int) (Delivery, error) {
	var delivery Delivery
	err := registry.pool.QueryRow(ctx, `INSERT INTO simulator_deliveries (webhook_id, payload, headers, response_status) VALUES ($1,$2,$3,$4) RETURNING id, webhook_id, payload, headers, response_status, received_at`, webhookID, payload, headers, responseStatus).Scan(&delivery.ID, &delivery.WebhookID, &delivery.Payload, &delivery.Headers, &delivery.ResponseStatus, &delivery.ReceivedAt)
	return delivery, err
}

func (registry *Registry) ListDeliveries(ctx context.Context, limit int) ([]Delivery, error) {
	if limit < 1 || limit > 500 {
		limit = 100
	}
	rows, err := registry.pool.Query(ctx, `SELECT id, webhook_id, payload, headers, response_status, received_at FROM simulator_deliveries ORDER BY received_at DESC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	deliveries := make([]Delivery, 0)
	for rows.Next() {
		var delivery Delivery
		if err := rows.Scan(&delivery.ID, &delivery.WebhookID, &delivery.Payload, &delivery.Headers, &delivery.ResponseStatus, &delivery.ReceivedAt); err != nil {
			return nil, err
		}
		deliveries = append(deliveries, delivery)
	}
	return deliveries, rows.Err()
}

func collectWebhooks(rows pgx.Rows) ([]Webhook, error) {
	webhooks := make([]Webhook, 0)
	for rows.Next() {
		var webhook Webhook
		if err := rows.Scan(&webhook.ID, &webhook.AccountID, &webhook.EventTypes, &webhook.ResponseStatus, &webhook.ResponseDelayMS); err != nil {
			return nil, err
		}
		webhooks = append(webhooks, webhook)
	}
	return webhooks, rows.Err()
}

func validateWebhook(eventTypes []string, responseStatus, responseDelayMS int) error {
	if len(eventTypes) < 1 || len(eventTypes) > 3 {
		return fmt.Errorf("select between one and three event types")
	}
	seen := make(map[string]struct{}, len(eventTypes))
	for _, eventType := range eventTypes {
		if _, supported := supportedEventTypes[eventType]; !supported {
			return fmt.Errorf("unsupported event type %q", eventType)
		}
		if _, duplicate := seen[eventType]; duplicate {
			return fmt.Errorf("event types must be unique")
		}
		seen[eventType] = struct{}{}
	}
	if responseStatus < 100 || responseStatus > 599 {
		return fmt.Errorf("response status must be between 100 and 599")
	}
	if responseDelayMS < 0 || responseDelayMS > 30000 {
		return fmt.Errorf("response delay must be between 0 and 30000 milliseconds")
	}
	return nil
}