package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"webhooknotifier/internal/config"
	"webhooknotifier/internal/fairness"
	"webhooknotifier/internal/queue"
	"webhooknotifier/internal/storage"

	"github.com/jackc/pgx/v5/pgxpool"
)

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
	repository := storage.NewEventRepository(pool)
	accountOrder := &fairness.AccountRoundRobin{}
	watermarkGate := &fairness.WatermarkGate{}
	owner, _ := os.Hostname()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			dispatch(ctx, repository, broker, accountOrder, watermarkGate, owner, configuration)
		}
	}
}
func dispatch(ctx context.Context, repository *storage.EventRepository, broker *queue.Client, accountOrder *fairness.AccountRoundRobin, watermarkGate *fairness.WatermarkGate, owner string, configuration config.Config) {
	ready, _, err := broker.Depth()
	if err != nil {
		log.Printf("queue depth: %v", err)
		return
	}
	if watermarkGate.Update(ready, configuration.LowWatermark, configuration.HighWatermark) {
		return
	}
	accounts, err := repository.GetDistinctAccountsWithPendingWork(ctx)
	if err != nil {
		log.Printf("pending accounts: %v", err)
		return
	}
	for _, account := range accountOrder.OrderAccounts(accounts) {
		available := configuration.HighWatermark - ready
		if available <= 0 {
			return
		}
		quantum := configuration.Quantum
		if quantum > available {
			quantum = available
		}
		events, err := repository.ClaimBatchForAccount(ctx, account, owner, quantum, configuration.LeaseDuration)
		if err != nil {
			log.Printf("claim %s: %v", account, err)
			continue
		}
		published := make([]string, 0, len(events))
		for _, event := range events {
			if err := broker.Publish(ctx, event); err != nil {
				log.Printf("publish %s: %v", event.ID, err)
				continue
			}
			published = append(published, event.ID)
			ready++
		}
		if len(published) > 0 {
			if err := repository.MarkPublished(ctx, published); err != nil {
				log.Printf("mark published: %v", err)
			}
		}
	}
}
