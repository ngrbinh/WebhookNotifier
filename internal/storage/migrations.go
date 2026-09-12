package storage

import (
	"context"
	"embed"
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

// ApplyMigrations applies the repository's additive PostgreSQL migrations in filename order.
func ApplyMigrations(ctx context.Context, pool *pgxpool.Pool) error {
	paths, err := migrationFiles.ReadDir("migrations")
	if err != nil {
		return err
	}
	fileNames := make([]string, 0, len(paths))
	for _, path := range paths {
		if !path.IsDir() {
			fileNames = append(fileNames, path.Name())
		}
	}
	sort.Strings(fileNames)
	for _, fileName := range fileNames {
		content, err := migrationFiles.ReadFile(filepath.Join("migrations", fileName))
		if err != nil {
			return err
		}
		for _, statement := range strings.Split(string(content), ";") {
			if strings.TrimSpace(statement) == "" {
				continue
			}
			if _, err := pool.Exec(ctx, statement); err != nil {
				return fmt.Errorf("apply migration %s: %w", fileName, err)
			}
		}
	}
	return nil
}