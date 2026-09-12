package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"webhooknotifier/internal/config"
	"webhooknotifier/internal/delivery"
	"webhooknotifier/internal/model"
	"webhooknotifier/internal/queue"
	"webhooknotifier/internal/storage"
)

type worker struct {
	repository    *storage.EventRepository
	broker        *queue.Client
	client        *http.Client
	configuration config.Config
}

func (service *worker) process(ctx context.Context, messageBody []byte) error {
	var event model.Event
	if err := json.Unmarshal(messageBody, &event); err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, event.DestinationURL, bytes.NewReader(event.Payload))
	if err != nil {
		return service.finishPermanent(ctx, event, err.Error(), 0)
	}
	request.Header.Set("Content-Type", "application/json")
	response, requestError := service.client.Do(request)
	statusCode := 0
	if response != nil {
		statusCode = response.StatusCode
		io.Copy(io.Discard, response.Body)
		response.Body.Close()
	}
	outcome := delivery.Classify(statusCode, requestError)
	attempt := event.AttemptCount + 1
	switch outcome {
	case delivery.Success:
		return service.repository.RecordDeliverySuccess(ctx, event.ID, attempt, statusCode)
	case delivery.Retryable:
		if attempt >= event.MaxAttempts {
			return service.finishDeadLetter(ctx, event, attempt, delivery.ErrorText(requestError, statusCode), statusCode)
		}
		next := time.Now().Add(delivery.Backoff(service.configuration.BaseBackoff, attempt))
		return service.repository.RecordDeliveryRetry(ctx, event.ID, attempt, next, delivery.ErrorText(requestError, statusCode), statusCode)
	default:
		return service.finishDeadLetter(ctx, event, attempt, delivery.ErrorText(requestError, statusCode), statusCode)
	}
}
func (service *worker) finishPermanent(ctx context.Context, event model.Event, reason string, statusCode int) error {
	return service.finishDeadLetter(ctx, event, event.AttemptCount+1, reason, statusCode)
}
func (service *worker) finishDeadLetter(ctx context.Context, event model.Event, attempt int, reason string, statusCode int) error {
	if err := service.repository.RecordDeadLetter(ctx, event.ID, attempt, reason, statusCode); err != nil {
		return err
	}
	return service.broker.PublishDeadLetter(ctx, event, reason)
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
	broker, err := queue.New(ctx, configuration.RabbitURL, configuration.QueueName, configuration.DLQName)
	if err != nil {
		log.Fatal(err)
	}
	defer broker.Close()
	deliveries, err := broker.Consume(ctx, configuration.WorkerConcurrency)
	if err != nil {
		log.Fatal(err)
	}
	service := &worker{repository: storage.NewEventRepository(pool), broker: broker, client: &http.Client{Timeout: configuration.DeliveryTimeout}, configuration: configuration}
	for {
		select {
		case <-ctx.Done():
			return
		case message, ok := <-deliveries:
			if !ok {
				return
			}
			if err := service.process(ctx, message.Body); err != nil {
				log.Printf("event processing failed: %v", err)
				continue
			}
			if err := message.Ack(false); err != nil {
				log.Printf("ack failed: %v", err)
			}
		}
	}
}
