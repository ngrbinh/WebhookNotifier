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
	limit      int
	cleared    bool
	deletedID  string
}

func (registry *memoryRegistry) CreateAccount(_ context.Context, accountID string) (simulator.Account, error) {
	account := simulator.Account{ID: accountID, CreatedAt: time.Now()}
	registry.accounts = append(registry.accounts, account)
	return account, nil
}

func (registry *memoryRegistry) ListAccounts(context.Context) ([]simulator.Account, error) {
	return registry.accounts, nil
}

func (registry *memoryRegistry) CreateWebhook(_ context.Context, accountID string, eventTypes []string, response2xxPercent, response429Percent, response4xxPercent, response5xxPercent, responseDelayMS int) (simulator.Webhook, error) {
	webhook := simulator.Webhook{ID: "wh_test", AccountID: accountID, EventTypes: eventTypes, Response2xxPercent: response2xxPercent, Response429Percent: response429Percent, Response4xxPercent: response4xxPercent, Response5xxPercent: response5xxPercent, ResponseDelayMS: responseDelayMS}
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

func (registry *memoryRegistry) DeleteWebhook(_ context.Context, webhookID string) error {
	for index, webhook := range registry.webhooks {
		if webhook.ID == webhookID {
			registry.webhooks = append(registry.webhooks[:index], registry.webhooks[index+1:]...)
			registry.deliveries = nil
			registry.deletedID = webhookID
			return nil
		}
	}
	return context.Canceled
}

func (registry *memoryRegistry) RecordDelivery(_ context.Context, webhookID string, payload json.RawMessage, headers json.RawMessage, responseStatus int) (simulator.Delivery, error) {
	delivery := simulator.Delivery{ID: int64(len(registry.deliveries) + 1), WebhookID: webhookID, Payload: payload, Headers: headers, ResponseStatus: responseStatus, ReceivedAt: time.Now()}
	registry.deliveries = append(registry.deliveries, delivery)
	return delivery, nil
}

func (registry *memoryRegistry) ListDeliveries(_ context.Context, limit int) ([]simulator.Delivery, error) {
	registry.limit = limit
	return registry.deliveries, nil
}

func (registry *memoryRegistry) ClearDeliveries(context.Context) error {
	registry.deliveries = nil
	registry.cleared = true
	return nil
}

func TestHandlerRegistersWebhookAndCapturesDelivery(t *testing.T) {
	registry := &memoryRegistry{}
	var simulationRequest SimulationRequest
	handler := Handler(registry, func(_ context.Context, request SimulationRequest) (SimulationResult, error) {
		simulationRequest = request
		return SimulationResult{}, nil
	})

	createAccount := httptest.NewRequest(http.MethodPost, "/api/accounts", strings.NewReader(`{"id":"acme"}`))
	createAccount.Header.Set("Content-Type", "application/json")
	accountResponse := httptest.NewRecorder()
	handler.ServeHTTP(accountResponse, createAccount)
	if accountResponse.Code != http.StatusOK {
		t.Fatalf("account response = %d, want %d", accountResponse.Code, http.StatusOK)
	}

	createWebhook := httptest.NewRequest(http.MethodPost, "/api/webhooks", strings.NewReader(`{"account_id":"acme","event_types":["subscriber.created"],"response_2xx_percent":0,"response_429_percent":100,"response_4xx_percent":0,"response_5xx_percent":0,"response_delay_ms":0}`))
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

	deleteWebhookRequest := httptest.NewRequest(http.MethodDelete, "/api/webhooks/wh_test", nil)
	deleteWebhookResponse := httptest.NewRecorder()
	handler.ServeHTTP(deleteWebhookResponse, deleteWebhookRequest)
	if deleteWebhookResponse.Code != http.StatusOK {
		t.Fatalf("delete webhook response = %d, want %d", deleteWebhookResponse.Code, http.StatusOK)
	}
	if registry.deletedID != "wh_test" || len(registry.deliveries) != 0 {
		t.Fatal("webhook and its deliveries were not deleted")
	}

	deliveriesRequest := httptest.NewRequest(http.MethodGet, "/api/deliveries?limit=7", nil)
	deliveriesResponse := httptest.NewRecorder()
	handler.ServeHTTP(deliveriesResponse, deliveriesRequest)
	if deliveriesResponse.Code != http.StatusOK {
		t.Fatalf("deliveries response = %d, want %d", deliveriesResponse.Code, http.StatusOK)
	}
	if registry.limit != 7 {
		t.Fatalf("delivery limit = %d, want %d", registry.limit, 7)
	}

	clearDeliveriesRequest := httptest.NewRequest(http.MethodDelete, "/api/deliveries", nil)
	clearDeliveriesResponse := httptest.NewRecorder()
	handler.ServeHTTP(clearDeliveriesResponse, clearDeliveriesRequest)
	if clearDeliveriesResponse.Code != http.StatusOK {
		t.Fatalf("clear deliveries response = %d, want %d", clearDeliveriesResponse.Code, http.StatusOK)
	}
	if !registry.cleared || len(registry.deliveries) != 0 {
		t.Fatal("deliveries were not cleared")
	}

	simulateRequest := httptest.NewRequest(http.MethodPost, "/api/simulate", strings.NewReader(`{"entries":[{"account_id":"acme","event_type":"subscriber.created","event_count":10}],"rate":20}`))
	simulateResponse := httptest.NewRecorder()
	handler.ServeHTTP(simulateResponse, simulateRequest)
	if simulateResponse.Code != http.StatusOK {
		t.Fatalf("simulation response = %d, want %d", simulateResponse.Code, http.StatusOK)
	}
	if len(simulationRequest.Entries) != 1 || simulationRequest.Entries[0].EventCount != 10 {
		t.Fatalf("simulation entries = %#v, want one entry with 10 events", simulationRequest.Entries)
	}
}

func TestSelectResponseStatus(t *testing.T) {
	tests := []struct {
		name     string
		webhook  simulator.Webhook
		expected int
	}{
		{name: "success", webhook: simulator.Webhook{Response2xxPercent: 100}, expected: http.StatusOK},
		{name: "retryable", webhook: simulator.Webhook{Response429Percent: 100}, expected: http.StatusTooManyRequests},
		{name: "permanent failure", webhook: simulator.Webhook{Response4xxPercent: 100}, expected: http.StatusNotFound},
		{name: "server failure", webhook: simulator.Webhook{Response5xxPercent: 100}, expected: http.StatusInternalServerError},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if actual := selectResponseStatus(test.webhook); actual != test.expected {
				t.Fatalf("response status = %d, want %d", actual, test.expected)
			}
		})
	}
}
