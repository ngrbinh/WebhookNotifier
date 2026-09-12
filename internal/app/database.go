// Package app contains shared application bootstrap helpers.
package app

import (
	"context"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
)

// OpenDatabase opens and verifies a PostgreSQL connection pool.
func OpenDatabase(ctx context.Context, connectionString string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, connectionString)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}

// ReadFile reads a file and returns its contents as a string.
func ReadFile(path string) (string, error) {
	content, err := os.ReadFile(path)
	return string(content), err
}
