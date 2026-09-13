// Command simulator generates configurable webhook traffic against the
// receiver, either as a CLI benchmark run or an embedded web dashboard.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"webhooknotifier/internal/app"
	"webhooknotifier/internal/config"
	"webhooknotifier/internal/model"
	registryservice "webhooknotifier/internal/simulator"
	"webhooknotifier/internal/simulator/web"
)

type runConfig struct {
	receiver string
}

func main() {
	configuration := runConfig{}
	flag.StringVar(&configuration.receiver, "receiver", "http://localhost:8080/api/v1/events", "receiver URL")
	flag.Parse()
	startDashboard(configuration)
}

// startDashboard serves simulator controls and runs simulations requested by the dashboard.
func startDashboard(configuration runConfig) {
	settings := config.Load()
	pool, err := app.OpenDatabase(context.Background(), settings.DatabaseURL)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()
	registry := registryservice.NewRegistry(pool)
	handler := web.Handler(registry, func(ctx context.Context, request web.SimulationRequest) (web.SimulationResult, error) {
		return simulateSimulationEntries(ctx, configuration.receiver, request, registry, settings.SimulatorInternalURL)
	})
	server := &http.Server{Addr: ":" + settings.SimulatorPort, Handler: handler, ReadHeaderTimeout: 5 * time.Second}
	fmt.Printf("simulator dashboard listening on http://localhost:%s\n", settings.SimulatorPort)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}

// simulateSimulationEntries sends the exact account event entries configured in the dashboard.
func simulateSimulationEntries(ctx context.Context, receiver string, request web.SimulationRequest, registry *registryservice.Registry, simulatorURL string) (web.SimulationResult, error) {
	if len(request.Entries) == 0 {
		return web.SimulationResult{}, fmt.Errorf("add at least one simulation entry")
	}
	if request.Rate < 1 || request.Rate > 1000 {
		return web.SimulationResult{}, fmt.Errorf("rate must be between 1 and 1000 events per second")
	}
	accounts, err := registry.ListAccounts(ctx)
	if err != nil {
		return web.SimulationResult{}, err
	}
	accountIDs := make(map[string]struct{}, len(accounts))
	for _, account := range accounts {
		accountIDs[account.ID] = struct{}{}
	}
	for _, entry := range request.Entries {
		if _, exists := accountIDs[entry.AccountID]; !exists {
			return web.SimulationResult{}, fmt.Errorf("account %q does not exist", entry.AccountID)
		}
		if !isSupportedEventType(entry.EventType) {
			return web.SimulationResult{}, fmt.Errorf("unsupported event type %q", entry.EventType)
		}
		if entry.EventCount < 1 || entry.EventCount > 10000 {
			return web.SimulationResult{}, fmt.Errorf("event count must be between 1 and 10000")
		}
	}
	client := &http.Client{Timeout: 5 * time.Second}
	var accepted atomic.Int64
	var rejected atomic.Int64
	var matched atomic.Int64
	var unmatched atomic.Int64
	interval := time.Second / time.Duration(request.Rate)
	var group sync.WaitGroup
	eventIndex := 0
	for _, entry := range request.Entries {
		for count := 0; count < entry.EventCount; count++ {
			webhooks, err := registry.FindWebhooksForEvent(ctx, entry.AccountID, entry.EventType)
			if err != nil {
				return web.SimulationResult{}, err
			}
			if len(webhooks) == 0 {
				unmatched.Add(1)
			}
			for _, webhook := range webhooks {
				matched.Add(1)
				payload := json.RawMessage(fmt.Sprintf(`{"event_name":"%s","event_time":"%s","webhook_id":"%s","subscriber":{"id":"sub-%d","status":"active","email":"user@example.com"}}`, entry.EventType, time.Now().UTC().Format(time.RFC3339), webhook.ID, eventIndex))
				ingestionRequest := model.IngestionRequest{AccountID: entry.AccountID, DestinationURL: fmt.Sprintf("%s/internal/webhooks/%s", simulatorURL, webhook.ID), IdempotencyKey: fmt.Sprintf("sim-%d-%d-%s", time.Now().UnixNano(), eventIndex, webhook.ID), Payload: payload}
				body, err := json.Marshal(ingestionRequest)
				if err != nil {
					return web.SimulationResult{}, err
				}
				group.Add(1)
				go postEvent(client, receiver, body, &group, &accepted, &rejected)
			}
			eventIndex++
			time.Sleep(interval)
		}
	}
	group.Wait()
	return web.SimulationResult{Generated: eventIndex, Matched: int(matched.Load()), Accepted: int(accepted.Load()), Rejected: int(rejected.Load()), Unmatched: int(unmatched.Load())}, nil
}

func isSupportedEventType(eventType string) bool {
	switch eventType {
	case "subscriber.created", "subscriber.added_to_segment", "subscriber.unsubscribed":
		return true
	default:
		return false
	}
}

func postEvent(client *http.Client, receiver string, body []byte, group *sync.WaitGroup, accepted, rejected *atomic.Int64) {
	defer group.Done()
	response, err := client.Post(receiver, "application/json", bytes.NewReader(body))
	if err == nil && response.StatusCode < 300 {
		accepted.Add(1)
	} else {
		rejected.Add(1)
	}
	if response != nil {
		response.Body.Close()
	}
}
