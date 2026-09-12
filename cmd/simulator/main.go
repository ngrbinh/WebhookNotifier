package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"webhooknotifier/internal/config"
	"webhooknotifier/internal/model"
	"webhooknotifier/internal/simulator/web"
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
	if err := run(context.Background(), configuration); err != nil {
		panic(err)
	}
}
func startDashboard(configuration runConfig) {
	port := config.Load().SimulatorPort
	handler := web.Handler(func(profile string, events, rate int) (string, error) {
		configuration.profile, configuration.events, configuration.rate = profile, events, rate
		if err := run(context.Background(), configuration); err != nil {
			return "", err
		}
		return "simulation complete", nil
	})
	server := &http.Server{Addr: ":" + port, Handler: handler, ReadHeaderTimeout: 5 * time.Second}
	fmt.Printf("simulator dashboard listening on http://localhost:%s\n", port)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		panic(err)
	}
}
func run(ctx context.Context, configuration runConfig) error {
	client := &http.Client{Timeout: 5 * time.Second}
	var accepted atomic.Int64
	var rejected atomic.Int64
	interval := time.Second / time.Duration(max(configuration.rate, 1))
	var group sync.WaitGroup
	for index := 0; index < configuration.events; index++ {
		accountIndex := index % configuration.accounts
		if configuration.profile == "noisy_neighbor" {
			accountIndex = 0
			if index%10 == 0 {
				accountIndex = index % configuration.accounts
			}
		}
		request := model.IngestionRequest{AccountID: fmt.Sprintf("sim_account_%02d", accountIndex), DestinationURL: "http://localhost:8090/webhook", IdempotencyKey: fmt.Sprintf("sim-%d-%d", time.Now().UnixNano(), index), Payload: json.RawMessage(fmt.Sprintf(`{"event_name":"subscriber.%s","event_time":"%s","webhook_id":"wh-%d","subscriber":{"id":"sub-%d","status":"active","email":"user@example.com"}}`, eventName(index), time.Now().UTC().Format(time.RFC3339), accountIndex, index))}
		body, _ := json.Marshal(request)
		group.Add(1)
		go func() {
			defer group.Done()
			response, err := client.Post(configuration.receiver, "application/json", bytes.NewReader(body))
			if err == nil && response.StatusCode < 300 {
				accepted.Add(1)
			} else {
				rejected.Add(1)
			}
			if response != nil {
				response.Body.Close()
			}
		}()
		time.Sleep(interval)
	}
	group.Wait()
	fmt.Printf("profile=%s accepted=%d rejected=%d\n", configuration.profile, accepted.Load(), rejected.Load())
	return nil
}
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
func max(left, right int) int {
	if left > right {
		return left
	}
	return right
}
