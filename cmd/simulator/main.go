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

	"github.com/jackc/pgx/v5/pgxpool"
)

type runConfig struct {
	receiver string
	accounts int
	events   int
	rate     int
	profile  string
	web      bool
}

func main() {
	configuration := runConfig{}
	flag.StringVar(&configuration.receiver, "receiver", "http://localhost:8080/api/v1/events", "receiver URL")
	flag.IntVar(&configuration.accounts, "accounts", 3, "number of accounts")
	flag.IntVar(&configuration.events, "events", 30, "event count")
	flag.IntVar(&configuration.rate, "rate", 20, "events per second")
	flag.StringVar(&configuration.profile, "profile", "balanced", "balanced or noisy_neighbor")
	flag.BoolVar(&configuration.web, "web", false, "serve the dashboard on port 8084")
	flag.Parse()
	if configuration.web {
		startDashboard(configuration)
		return
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, config.Load().DatabaseURL)
	if err != nil {
		panic(err)
	}
	defer pool.Close()
	if _, err := run(ctx, configuration, registryservice.NewRegistry(pool), config.Load().SimulatorInternalURL); err != nil {
		panic(err)
	}
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
	handler := web.Handler(registry, func(ctx context.Context, profile string, events, rate int) (web.SimulationResult, error) {
		configuration.profile, configuration.events, configuration.rate = profile, events, rate
		return run(ctx, configuration, registry, settings.SimulatorInternalURL)
	})
	server := &http.Server{Addr: ":" + settings.SimulatorPort, Handler: handler, ReadHeaderTimeout: 5 * time.Second}
	fmt.Printf("simulator dashboard listening on http://localhost:%s\n", settings.SimulatorPort)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		panic(err)
	}
}

// run sends account events to each matching simulator-managed webhook subscription.
func run(ctx context.Context, configuration runConfig, registry *registryservice.Registry, simulatorURL string) (web.SimulationResult, error) {
	accounts, err := registry.ListAccounts(ctx)
	if err != nil {
		return web.SimulationResult{}, err
	}
	if len(accounts) == 0 {
		return web.SimulationResult{}, fmt.Errorf("create at least one account before starting a simulation")
	}
	client := &http.Client{Timeout: 5 * time.Second}
	var accepted atomic.Int64
	var rejected atomic.Int64
	var matched atomic.Int64
	var unmatched atomic.Int64
	interval := time.Second / time.Duration(max(configuration.rate, 1))
	var group sync.WaitGroup
	for index := 0; index < configuration.events; index++ {
		accountIndex := index % len(accounts)
		if configuration.profile == "noisy_neighbor" {
			accountIndex = 0
			if index%10 == 0 {
				accountIndex = index % len(accounts)
			}
		}
		accountID := accounts[accountIndex].ID
		eventType := "subscriber." + eventName(index)
		webhooks, err := registry.FindWebhooksForEvent(ctx, accountID, eventType)
		if err != nil {
			return web.SimulationResult{}, err
		}
		if len(webhooks) == 0 {
			unmatched.Add(1)
		}
		for _, webhook := range webhooks {
			matched.Add(1)
			payload := json.RawMessage(fmt.Sprintf(`{"event_name":"%s","event_time":"%s","webhook_id":"%s","subscriber":{"id":"sub-%d","status":"active","email":"user@example.com"}}`, eventType, time.Now().UTC().Format(time.RFC3339), webhook.ID, index))
			request := model.IngestionRequest{AccountID: accountID, DestinationURL: fmt.Sprintf("%s/internal/webhooks/%s", simulatorURL, webhook.ID), IdempotencyKey: fmt.Sprintf("sim-%d-%d-%s", time.Now().UnixNano(), index, webhook.ID), Payload: payload}
			body, err := json.Marshal(request)
			if err != nil {
				return web.SimulationResult{}, err
			}
			group.Add(1)
			go postEvent(client, configuration.receiver, body, &group, &accepted, &rejected)
		}
		time.Sleep(interval)
	}
	group.Wait()
	result := web.SimulationResult{Generated: configuration.events, Matched: int(matched.Load()), Accepted: int(accepted.Load()), Rejected: int(rejected.Load()), Unmatched: int(unmatched.Load())}
	fmt.Printf("profile=%s generated=%d matched=%d accepted=%d rejected=%d unmatched=%d\n", configuration.profile, result.Generated, result.Matched, result.Accepted, result.Rejected, result.Unmatched)
	return result, nil
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

// eventName returns the payload event name for a simulator request index.
func eventName(index int) string {
	switch index % 3 {
	case 1:
		return "added_to_segment"
	case 2:
		return "unsubscribed"
	default:
		return "created"
	}
}

// max returns the greater of two integers.
func max(left, right int) int {
	if left > right {
		return left
	}
	return right
}
