-- Inspect the planner's choice for eligible account discovery.
-- Run against a representative local dataset after applying the migration.
EXPLAIN (ANALYZE, BUFFERS)
SELECT DISTINCT account_id
FROM events
WHERE status = 'pending'
   OR (status = 'retriable' AND next_retry_at <= NOW())
ORDER BY account_id;

-- Inspect the FIFO claim path for one account.
EXPLAIN (ANALYZE, BUFFERS)
SELECT id, account_id, destination_url, idempotency_key, payload, status,
       attempt_count, max_attempts, next_retry_at, last_failure_reason, created_at
FROM events
WHERE account_id = 'acc_tenant_01'
  AND (
    status = 'pending'
    OR (status = 'retriable' AND next_retry_at <= NOW())
  )
  AND (lease_expires_at IS NULL OR lease_expires_at < NOW())
ORDER BY created_at ASC
LIMIT 5;
