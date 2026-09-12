// Command receiver runs the HTTP event ingestion API that durably persists
// events, idempotently, before returning a response to the caller.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"webhooknotifier/internal/config"
	"webhooknotifier/internal/model"
	"webhooknotifier/internal/storage"

	"github.com/jackc/pgx/v5/pgxpool"
)

type receiver struct {
	repository  *storage.EventRepository
	maxAttempts int
}

// handleEvent validates and persists one webhook event before returning its status.
func (service *receiver) handleEvent(response http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		http.Error(response, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var payload model.IngestionRequest
	decoder := json.NewDecoder(http.MaxBytesReader(response, request.Body, 2<<20))
	if err := decoder.Decode(&payload); err != nil {
		http.Error(response, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	if payload.IdempotencyKey == "" {
		payload.IdempotencyKey = request.Header.Get("Idempotency-Key")
	}
	if err := payload.Validate(); err != nil {
		http.Error(response, err.Error(), http.StatusBadRequest)
		return
	}
	event, inserted, err := service.repository.InsertEvent(request.Context(), payload, service.maxAttempts)
	if err != nil {
		http.Error(response, "persist event: "+err.Error(), http.StatusInternalServerError)
		return
	}
	response.Header().Set("Content-Type", "application/json")
	if inserted {
		response.WriteHeader(http.StatusAccepted)
	} else {
		response.WriteHeader(http.StatusOK)
	}
	json.NewEncoder(response).Encode(map[string]any{"event_id": event.ID, "accepted": inserted, "status": event.Status})
}

func main() {
	configuration := config.Load()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	pool, err := pgxpool.New(ctx, configuration.DatabaseURL)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()
	service := &receiver{repository: storage.NewEventRepository(pool), maxAttempts: configuration.MaxAttempts}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/events", service.handleEvent)
	mux.HandleFunc("/healthz", func(response http.ResponseWriter, _ *http.Request) { response.WriteHeader(http.StatusOK) })
	server := &http.Server{Addr: ":" + configuration.ReceiverPort, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		log.Printf("receiver listening on %s", server.Addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()
	<-ctx.Done()
	shutdownContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownContext); err != nil {
		fmt.Println(err)
	}
}
