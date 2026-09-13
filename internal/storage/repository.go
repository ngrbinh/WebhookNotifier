// Package storage persists webhook events and their delivery outcomes.
package storage

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"webhooknotifier/internal/model"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type EventRepository struct{ Pool *pgxpool.Pool }

// NewEventRepository creates a repository backed by the supplied PostgreSQL pool.
func NewEventRepository(pool *pgxpool.Pool) *EventRepository { return &EventRepository{Pool: pool} }

// InsertEvent stores an event idempotently and returns the existing event when duplicated.
func (repository *EventRepository) InsertEvent(ctx context.Context, request model.IngestionRequest, maxAttempts int) (model.Event, bool, error) {
	id := uuid.New()
	var event model.Event
	err := repository.Pool.QueryRow(ctx, `INSERT INTO events (id, account_id, destination_url, idempotency_key, payload, status, max_attempts) VALUES ($1,$2,$3,$4,$5,'pending',$6) ON CONFLICT (account_id,idempotency_key) DO NOTHING RETURNING id,account_id,destination_url,idempotency_key,payload,status,attempt_count,max_attempts,next_retry_at,COALESCE(last_failure_reason, ''),created_at`, id, request.AccountID, request.DestinationURL, request.IdempotencyKey, request.Payload, maxAttempts).Scan(&event.ID, &event.AccountID, &event.DestinationURL, &event.IdempotencyKey, &event.Payload, &event.Status, &event.AttemptCount, &event.MaxAttempts, &event.NextRetryAt, &event.LastFailureReason, &event.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		err = repository.Pool.QueryRow(ctx, `SELECT id,account_id,destination_url,idempotency_key,payload,status,attempt_count,max_attempts,next_retry_at,COALESCE(last_failure_reason, ''),created_at FROM events WHERE account_id=$1 AND idempotency_key=$2`, request.AccountID, request.IdempotencyKey).Scan(&event.ID, &event.AccountID, &event.DestinationURL, &event.IdempotencyKey, &event.Payload, &event.Status, &event.AttemptCount, &event.MaxAttempts, &event.NextRetryAt, &event.LastFailureReason, &event.CreatedAt)
		return event, false, err
	}
	return event, err == nil, err
}

// GetDistinctAccountsWithPendingWork lists accounts with pending or due retryable events.
func (repository *EventRepository) GetDistinctAccountsWithPendingWork(ctx context.Context) ([]string, error) {
	rows, err := repository.Pool.Query(ctx, `SELECT DISTINCT account_id FROM events WHERE status='pending' OR (status='retriable' AND next_retry_at <= NOW()) ORDER BY account_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var accounts []string
	for rows.Next() {
		var account string
		if err := rows.Scan(&account); err != nil {
			return nil, err
		}
		accounts = append(accounts, account)
	}
	return accounts, rows.Err()
}

// ClaimBatchForAccount leases the oldest available events for one account.
func (repository *EventRepository) ClaimBatchForAccount(ctx context.Context, accountID, owner string, quantum int, lease time.Duration) ([]model.Event, error) {
	tx, err := repository.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `SELECT id,account_id,destination_url,idempotency_key,payload,status,attempt_count,max_attempts,next_retry_at,COALESCE(last_failure_reason, ''),created_at FROM events WHERE account_id=$1 AND (status='pending' OR (status='retriable' AND next_retry_at <= NOW())) AND (lease_expires_at IS NULL OR lease_expires_at < NOW()) ORDER BY created_at ASC LIMIT $2 FOR UPDATE SKIP LOCKED`, accountID, quantum)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var events []model.Event
	for rows.Next() {
		var event model.Event
		if err := rows.Scan(&event.ID, &event.AccountID, &event.DestinationURL, &event.IdempotencyKey, &event.Payload, &event.Status, &event.AttemptCount, &event.MaxAttempts, &event.NextRetryAt, &event.LastFailureReason, &event.CreatedAt); err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for _, event := range events {
		if _, err := tx.Exec(ctx, `UPDATE events SET lease_owner=$1, lease_expires_at=NOW()+$2, updated_at=NOW() WHERE id=$3`, owner, lease, event.ID); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return events, nil
}

// MarkPublished marks successfully published events and clears their leases.
func (repository *EventRepository) MarkPublished(ctx context.Context, ids []string) error {
	_, err := repository.Pool.Exec(ctx, `UPDATE events SET status='published', lease_owner=NULL, lease_expires_at=NULL, updated_at=NOW() WHERE id = ANY($1::uuid[])`, ids)
	return err
}

// RecordDeliverySuccess records a delivered event and its successful attempt.
func (repository *EventRepository) RecordDeliverySuccess(ctx context.Context, id string, attempt int, statusCode int) error {
	tx, err := repository.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `UPDATE events SET status='delivered', attempt_count=$2, lease_owner=NULL, lease_expires_at=NULL, updated_at=NOW() WHERE id=$1`, id, attempt); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO event_attempts(event_id,attempt_number,outcome,status_code) VALUES($1,$2,'delivered',$3)`, id, attempt, statusCode)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// RecordDeliveryRetry records a failed attempt and schedules the next delivery.
func (repository *EventRepository) RecordDeliveryRetry(ctx context.Context, id string, attempt int, next time.Time, reason string, statusCode int) error {
	tx, err := repository.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `UPDATE events SET status='retriable', attempt_count=$2, next_retry_at=$3, last_failure_reason=$4, lease_owner=NULL, lease_expires_at=NULL, updated_at=NOW() WHERE id=$1`, id, attempt, next, reason); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO event_attempts(event_id,attempt_number,outcome,status_code,failure_reason) VALUES($1,$2,'retry',$3,$4)`, id, attempt, statusCode, reason)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// RecordDeadLetter records a terminal failure and clears the event lease.
func (repository *EventRepository) RecordDeadLetter(ctx context.Context, id string, attempt int, reason string, statusCode int) error {
	tx, err := repository.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `UPDATE events SET status='dead_letter', attempt_count=$2, last_failure_reason=$3, lease_owner=NULL, lease_expires_at=NULL, updated_at=NOW() WHERE id=$1`, id, attempt, reason); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO event_attempts(event_id,attempt_number,outcome,status_code,failure_reason) VALUES($1,$2,'dead_letter',$3,$4)`, id, attempt, statusCode, reason)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// EncodeEvent serializes an event as JSON.
func EncodeEvent(event model.Event) ([]byte, error) { return json.Marshal(event) }
