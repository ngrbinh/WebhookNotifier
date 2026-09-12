// Package app contains shared application bootstrap helpers.
package app

import (
	"context"
	"os"
	"strings"

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

// ApplyMigrations executes each semicolon-delimited SQL statement in migration.
func ApplyMigrations(ctx context.Context, pool *pgxpool.Pool, migration string) error {
	statements := strings.Split(migration, ";")
	for _, statement := range statements {
		if strings.TrimSpace(statement) != "" {
			if _, err := pool.Exec(ctx, statement); err != nil {
				return err
			}
		}
	}
	return nil
}

// ReadFile reads a file and returns its contents as a string.
func ReadFile(path string) (string, error) {
	content, err := os.ReadFile(path)
	return string(content), err
}
