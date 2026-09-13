// Package web serves the embedded simulator dashboard.
package web

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"time"

	"webhooknotifier/internal/simulator"
)

//go:embed index.html
var assets embed.FS

type Registry interface {
	CreateAccount(context.Context, string) (simulator.Account, error)
	ListAccounts(context.Context) ([]simulator.Account, error)
	CreateWebhook(context.Context, string, []string, int, int, int, int, int) (simulator.Webhook, error)
	ListWebhooks(context.Context) ([]simulator.Webhook, error)
	GetWebhook(context.Context, string) (simulator.Webhook, error)
	DeleteWebhook(context.Context, string) error
	RecordDelivery(context.Context, string, json.RawMessage, json.RawMessage, int) (simulator.Delivery, error)
	ListDeliveries(context.Context, int) ([]simulator.Delivery, error)
	ClearDeliveries(context.Context) error
}

type SimulationResult struct {
	Generated int `json:"generated"`
	Matched   int `json:"matched"`
	Accepted  int `json:"accepted"`
	Rejected  int `json:"rejected"`
	Unmatched int `json:"unmatched"`
}

type SimulationEntry struct {
	AccountID  string `json:"account_id"`
	EventType  string `json:"event_type"`
	EventCount int    `json:"event_count"`
}

type SimulationRequest struct {
	Entries []SimulationEntry `json:"entries"`
	Rate    int               `json:"rate"`
}

type SimulationStarter func(context.Context, SimulationRequest) (SimulationResult, error)

// Handler returns an HTTP handler for the simulator dashboard and API.
func Handler(registry Registry, start SimulationStarter) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/", http.FileServer(http.FS(assets)))
	mux.HandleFunc("/api/accounts", func(response http.ResponseWriter, request *http.Request) {
		switch request.Method {
		case http.MethodGet:
			accounts, err := registry.ListAccounts(request.Context())
			writeJSON(response, accounts, err)
		case http.MethodPost:
			var input struct {
				ID string `json:"id"`
			}
			if !decodeJSON(response, request, &input) {
				return
			}
			account, err := registry.CreateAccount(request.Context(), input.ID)
			writeJSON(response, account, err)
		default:
			http.Error(response, "method not allowed", http.StatusMethodNotAllowed)
		}
	})
	mux.HandleFunc("/api/webhooks", func(response http.ResponseWriter, request *http.Request) {
		switch request.Method {
		case http.MethodGet:
			webhooks, err := registry.ListWebhooks(request.Context())
			writeJSON(response, webhooks, err)
		case http.MethodPost:
			var input struct {
				AccountID          string   `json:"account_id"`
				EventTypes         []string `json:"event_types"`
				Response2xxPercent int      `json:"response_2xx_percent"`
				Response429Percent int      `json:"response_429_percent"`
				Response4xxPercent int      `json:"response_4xx_percent"`
				Response5xxPercent int      `json:"response_5xx_percent"`
				ResponseDelayMS    int      `json:"response_delay_ms"`
			}
			if !decodeJSON(response, request, &input) {
				return
			}
			webhook, err := registry.CreateWebhook(request.Context(), input.AccountID, input.EventTypes, input.Response2xxPercent, input.Response429Percent, input.Response4xxPercent, input.Response5xxPercent, input.ResponseDelayMS)
			writeJSON(response, webhook, err)
		default:
			http.Error(response, "method not allowed", http.StatusMethodNotAllowed)
		}
	})
	mux.HandleFunc("/api/webhooks/", func(response http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodDelete {
			http.Error(response, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		webhookID := strings.TrimPrefix(request.URL.Path, "/api/webhooks/")
		if webhookID == "" || strings.Contains(webhookID, "/") {
			http.NotFound(response, request)
			return
		}
		writeJSON(response, map[string]bool{"deleted": true}, registry.DeleteWebhook(request.Context(), webhookID))
	})
	mux.HandleFunc("/api/deliveries", func(response http.ResponseWriter, request *http.Request) {
		switch request.Method {
		case http.MethodDelete:
			writeJSON(response, map[string]bool{"cleared": true}, registry.ClearDeliveries(request.Context()))
		case http.MethodGet:
			limit, err := deliveryLimit(request.URL.Query().Get("limit"))
			if err != nil {
				http.Error(response, err.Error(), http.StatusBadRequest)
				return
			}
			deliveries, err := registry.ListDeliveries(request.Context(), limit)
			writeJSON(response, deliveries, err)
		default:
			http.Error(response, "method not allowed", http.StatusMethodNotAllowed)
		}
	})
	mux.HandleFunc("/api/simulate", func(response http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost {
			http.Error(response, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var input SimulationRequest
		if !decodeJSON(response, request, &input) {
			return
		}
		output, err := start(request.Context(), input)
		writeJSON(response, output, err)
	})
	mux.HandleFunc("/internal/webhooks/", func(response http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost {
			http.Error(response, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		webhookID := strings.TrimPrefix(request.URL.Path, "/internal/webhooks/")
		if webhookID == "" || strings.Contains(webhookID, "/") {
			http.NotFound(response, request)
			return
		}
		webhook, err := registry.GetWebhook(request.Context(), webhookID)
		if err != nil {
			http.NotFound(response, request)
			return
		}
		payload, err := readPayload(response, request)
		if err != nil {
			return
		}
		headers, err := json.Marshal(request.Header)
		if err != nil {
			http.Error(response, "encode headers: "+err.Error(), http.StatusInternalServerError)
			return
		}
		responseStatus := selectResponseStatus(webhook)
		if _, err = registry.RecordDelivery(request.Context(), webhook.ID, payload, headers, responseStatus); err != nil {
			http.Error(response, "record delivery: "+err.Error(), http.StatusInternalServerError)
			return
		}
		if webhook.ResponseDelayMS > 0 {
			time.Sleep(time.Duration(webhook.ResponseDelayMS) * time.Millisecond)
		}
		response.WriteHeader(responseStatus)
	})
	return mux
}

func deliveryLimit(value string) (int, error) {
	if value == "" {
		return 100, nil
	}
	limit, err := strconv.Atoi(value)
	if err != nil || limit < 1 || limit > 500 {
		return 0, fmt.Errorf("delivery limit must be between 1 and 500")
	}
	return limit, nil
}

func selectResponseStatus(webhook simulator.Webhook) int {
	selection := rand.Intn(100)
	if selection < webhook.Response2xxPercent {
		return http.StatusOK
	}
	selection -= webhook.Response2xxPercent
	if selection < webhook.Response429Percent {
		return http.StatusTooManyRequests
	}
	selection -= webhook.Response429Percent
	if selection < webhook.Response4xxPercent {
		return http.StatusNotFound
	}
	return http.StatusInternalServerError
}

func decodeJSON(response http.ResponseWriter, request *http.Request, target any) bool {
	if err := json.NewDecoder(http.MaxBytesReader(response, request.Body, 2<<20)).Decode(target); err != nil {
		http.Error(response, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return false
	}
	return true
}

func readPayload(response http.ResponseWriter, request *http.Request) (json.RawMessage, error) {
	var payload json.RawMessage
	if err := json.NewDecoder(http.MaxBytesReader(response, request.Body, 2<<20)).Decode(&payload); err != nil {
		http.Error(response, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return nil, err
	}
	if !json.Valid(payload) {
		err := fmt.Errorf("payload must be valid JSON")
		http.Error(response, err.Error(), http.StatusBadRequest)
		return nil, err
	}
	return payload, nil
}

func writeJSON(response http.ResponseWriter, value any, err error) {
	if err != nil {
		http.Error(response, err.Error(), http.StatusBadRequest)
		return
	}
	response.Header().Set("Content-Type", "application/json")
	json.NewEncoder(response).Encode(value)
}
