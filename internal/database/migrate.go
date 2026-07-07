package database

import (
	"embed"
	"errors"
	"fmt"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres" // postgres driver for migrations
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// MigrateUp runs all pending migrations. It is designed to run and then exit
// (init container pattern: `control-plane-api migrate up`).
func MigrateUp(databaseURL string) error {
	m, err := newMigrator(databaseURL)
	if err != nil {
		return err
	}
	defer func() { _, _ = m.Close() }()

	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("running migrations up: %w", err)
	}
	return nil
}

// MigrateDown rolls back migrations. If steps is 0, it rolls back all
// migrations; otherwise it rolls back the given number of steps.
func MigrateDown(databaseURL string, steps int) error {
	m, err := newMigrator(databaseURL)
	if err != nil {
		return err
	}
	defer func() { _, _ = m.Close() }()

	if steps <= 0 {
		if err := m.Down(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
			return fmt.Errorf("running migrations down: %w", err)
		}
		return nil
	}

	if err := m.Steps(-steps); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("rolling back %d migrations: %w", steps, err)
	}
	return nil
}

// MigrateVersion returns the current migration version and dirty state.
func MigrateVersion(databaseURL string) (version uint, dirty bool, err error) {
	m, err := newMigrator(databaseURL)
	if err != nil {
		return 0, false, err
	}
	defer func() { _, _ = m.Close() }()

	return m.Version()
}

func newMigrator(databaseURL string) (*migrate.Migrate, error) {
	source, err := iofs.New(migrationsFS, "migrations")
	if err != nil {
		return nil, fmt.Errorf("creating migration source: %w", err)
	}

	m, err := migrate.NewWithSourceInstance("iofs", source, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("creating migrator: %w", err)
	}

	return m, nil
}
