package web

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"webhooknotifier/internal/simulator"
)

type memoryRegistry struct {
	accounts   []simulator.Account
	webhooks   []simulator.Webhook
	deliveries []simulator.Delivery
}

func (registry *memoryRegistry) CreateAccount(_ context.Context, accountID string) (simulator.Account, error) {
	account := simulator.Account{ID: accountID, CreatedAt: time.Now()}
	registry.accounts = append(registry.accounts, account)
	return account, nil
}

func (registry *memoryRegistry) ListAccounts(context.Context) ([]simulator.Account, error) {
	return registry.accounts, nil
}

func (registry *memoryRegistry) CreateWebhook(_ context.Context, accountID string, eventTypes []string, responseStatus, responseDelayMS int) (simulator.Webhook, error) {
	webhook := simulator.Webhook{ID: "wh_test", AccountID: accountID, EventTypes: eventTypes, ResponseStatus: responseStatus, ResponseDelayMS: responseDelayMS}
	registry.webhooks = append(registry.webhooks, webhook)
	return webhook, nil
}

func (registry *memoryRegistry) ListWebhooks(context.Context) ([]simulator.Webhook, error) {
	return registry.webhooks, nil
}

func (registry *memoryRegistry) GetWebhook(_ context.Context, webhookID string) (simulator.Webhook, error) {
	for _, webhook := range registry.webhooks {
		if webhook.ID == webhookID {
			return webhook, nil
		}
	}
	return simulator.Webhook{}, context.Canceled
}

func (registry *memoryRegistry) RecordDelivery(_ context.Context, webhookID string, payload json.RawMessage, headers json.RawMessage, responseStatus int) (simulator.Delivery, error) {
	delivery := simulator.Delivery{ID: int64(len(registry.deliveries) + 1), WebhookID: webhookID, Payload: payload, Headers: headers, ResponseStatus: responseStatus, ReceivedAt: time.Now()}
	registry.deliveries = append(registry.deliveries, delivery)
	return delivery, nil
}

func (registry *memoryRegistry) ListDeliveries(context.Context, int) ([]simulator.Delivery, error) {
	return registry.deliveries, nil
}

func TestHandlerRegistersWebhookAndCapturesDelivery(t *testing.T) {
	registry := &memoryRegistry{}
	handler := Handler(registry, func(context.Context, string, int, int) (SimulationResult, error) {
		return SimulationResult{}, nil
	})

	createAccount := httptest.NewRequest(http.MethodPost, "/api/accounts", strings.NewReader(`{"id":"acme"}`))
	createAccount.Header.Set("Content-Type", "application/json")
	accountResponse := httptest.NewRecorder()
	handler.ServeHTTP(accountResponse, createAccount)
	if accountResponse.Code != http.StatusOK {
		t.Fatalf("account response = %d, want %d", accountResponse.Code, http.StatusOK)
	}

	createWebhook := httptest.NewRequest(http.MethodPost, "/api/webhooks", strings.NewReader(`{"account_id":"acme","event_types":["subscriber.created"],"response_status":429,"response_delay_ms":0}`))
	createWebhook.Header.Set("Content-Type", "application/json")
	webhookResponse := httptest.NewRecorder()
	handler.ServeHTTP(webhookResponse, createWebhook)
	if webhookResponse.Code != http.StatusOK {
		t.Fatalf("webhook response = %d, want %d", webhookResponse.Code, http.StatusOK)
	}

	deliveryRequest := httptest.NewRequest(http.MethodPost, "/internal/webhooks/wh_test", strings.NewReader(`{"event_name":"subscriber.created"}`))
	deliveryResponse := httptest.NewRecorder()
	handler.ServeHTTP(deliveryResponse, deliveryRequest)
	if deliveryResponse.Code != http.StatusTooManyRequests {
		t.Fatalf("delivery response = %d, want %d", deliveryResponse.Code, http.StatusTooManyRequests)
	}
	if len(registry.deliveries) != 1 {
		t.Fatalf("captured deliveries = %d, want 1", len(registry.deliveries))
	}
	if registry.deliveries[0].WebhookID != "wh_test" {
		t.Fatalf("captured webhook = %q, want wh_test", registry.deliveries[0].WebhookID)
	}
}