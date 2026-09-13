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
	ID                 string   `json:"id"`
	AccountID          string   `json:"account_id"`
	EventTypes         []string `json:"event_types"`
	Response2xxPercent int      `json:"response_2xx_percent"`
	Response429Percent int      `json:"response_429_percent"`
	Response4xxPercent int      `json:"response_4xx_percent"`
	Response5xxPercent int      `json:"response_5xx_percent"`
	ResponseDelayMS    int      `json:"response_delay_ms"`
}

type Delivery struct {
	ID             int64           `json:"id"`
	WebhookID      string          `json:"webhook_id"`
	AccountID      string          `json:"account_id"`
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

func (registry *Registry) CreateWebhook(ctx context.Context, accountID string, eventTypes []string, response2xxPercent, response429Percent, response4xxPercent, response5xxPercent, responseDelayMS int) (Webhook, error) {
	if err := validateWebhook(eventTypes, response2xxPercent, response429Percent, response4xxPercent, response5xxPercent, responseDelayMS); err != nil {
		return Webhook{}, err
	}
	webhook := Webhook{ID: "wh_" + uuid.NewString(), AccountID: accountID, EventTypes: eventTypes, Response2xxPercent: response2xxPercent, Response429Percent: response429Percent, Response4xxPercent: response4xxPercent, Response5xxPercent: response5xxPercent, ResponseDelayMS: responseDelayMS}
	err := registry.pool.QueryRow(ctx, `INSERT INTO simulator_webhooks (id, account_id, event_types, response_2xx_percent, response_429_percent, response_4xx_percent, response_5xx_percent, response_delay_ms) VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING id, account_id, event_types, response_2xx_percent, response_429_percent, response_4xx_percent, response_5xx_percent, response_delay_ms`, webhook.ID, webhook.AccountID, webhook.EventTypes, webhook.Response2xxPercent, webhook.Response429Percent, webhook.Response4xxPercent, webhook.Response5xxPercent, webhook.ResponseDelayMS).Scan(&webhook.ID, &webhook.AccountID, &webhook.EventTypes, &webhook.Response2xxPercent, &webhook.Response429Percent, &webhook.Response4xxPercent, &webhook.Response5xxPercent, &webhook.ResponseDelayMS)
	return webhook, err
}

func (registry *Registry) ListWebhooks(ctx context.Context) ([]Webhook, error) {
	rows, err := registry.pool.Query(ctx, `SELECT id, account_id, event_types, response_2xx_percent, response_429_percent, response_4xx_percent, response_5xx_percent, response_delay_ms FROM simulator_webhooks ORDER BY account_id, id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return collectWebhooks(rows)
}

func (registry *Registry) FindWebhooksForEvent(ctx context.Context, accountID, eventType string) ([]Webhook, error) {
	rows, err := registry.pool.Query(ctx, `SELECT id, account_id, event_types, response_2xx_percent, response_429_percent, response_4xx_percent, response_5xx_percent, response_delay_ms FROM simulator_webhooks WHERE account_id = $1 AND $2 = ANY(event_types) ORDER BY id`, accountID, eventType)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return collectWebhooks(rows)
}

func (registry *Registry) GetWebhook(ctx context.Context, webhookID string) (Webhook, error) {
	var webhook Webhook
	err := registry.pool.QueryRow(ctx, `SELECT id, account_id, event_types, response_2xx_percent, response_429_percent, response_4xx_percent, response_5xx_percent, response_delay_ms FROM simulator_webhooks WHERE id = $1`, webhookID).Scan(&webhook.ID, &webhook.AccountID, &webhook.EventTypes, &webhook.Response2xxPercent, &webhook.Response429Percent, &webhook.Response4xxPercent, &webhook.Response5xxPercent, &webhook.ResponseDelayMS)
	return webhook, err
}

// DeleteWebhook removes a webhook and all deliveries captured for it.
func (registry *Registry) DeleteWebhook(ctx context.Context, webhookID string) error {
	commandTag, err := registry.pool.Exec(ctx, `DELETE FROM simulator_webhooks WHERE id = $1`, webhookID)
	if err != nil {
		return err
	}
	if commandTag.RowsAffected() == 0 {
		return fmt.Errorf("webhook %q does not exist", webhookID)
	}
	return nil
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
	rows, err := registry.pool.Query(ctx, `SELECT deliveries.id, deliveries.webhook_id, webhooks.account_id, deliveries.payload, deliveries.headers, deliveries.response_status, deliveries.received_at FROM simulator_deliveries AS deliveries JOIN simulator_webhooks AS webhooks ON webhooks.id = deliveries.webhook_id ORDER BY deliveries.received_at DESC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	deliveries := make([]Delivery, 0)
	for rows.Next() {
		var delivery Delivery
		if err := rows.Scan(&delivery.ID, &delivery.WebhookID, &delivery.AccountID, &delivery.Payload, &delivery.Headers, &delivery.ResponseStatus, &delivery.ReceivedAt); err != nil {
			return nil, err
		}
		deliveries = append(deliveries, delivery)
	}
	return deliveries, rows.Err()
}

// ClearDeliveries removes all deliveries captured by the simulator.
func (registry *Registry) ClearDeliveries(ctx context.Context) error {
	_, err := registry.pool.Exec(ctx, `DELETE FROM simulator_deliveries`)
	return err
}

func collectWebhooks(rows pgx.Rows) ([]Webhook, error) {
	webhooks := make([]Webhook, 0)
	for rows.Next() {
		var webhook Webhook
		if err := rows.Scan(&webhook.ID, &webhook.AccountID, &webhook.EventTypes, &webhook.Response2xxPercent, &webhook.Response429Percent, &webhook.Response4xxPercent, &webhook.Response5xxPercent, &webhook.ResponseDelayMS); err != nil {
			return nil, err
		}
		webhooks = append(webhooks, webhook)
	}
	return webhooks, rows.Err()
}

func validateWebhook(eventTypes []string, response2xxPercent, response429Percent, response4xxPercent, response5xxPercent, responseDelayMS int) error {
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
	responsePercentages := []int{response2xxPercent, response429Percent, response4xxPercent, response5xxPercent}
	totalPercent := 0
	for _, percentage := range responsePercentages {
		if percentage < 0 || percentage > 100 {
			return fmt.Errorf("response percentages must be between 0 and 100")
		}
		totalPercent += percentage
	}
	if totalPercent != 100 {
		return fmt.Errorf("response percentages must total 100")
	}
	if responseDelayMS < 0 || responseDelayMS > 30000 {
		return fmt.Errorf("response delay must be between 0 and 30000 milliseconds")
	}
	return nil
}
