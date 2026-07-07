package database

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nunocgoncalves/control-plane/internal/config"
)

// Connect establishes a connection pool to PostgreSQL and verifies
// connectivity with a ping.
func Connect(ctx context.Context, cfg config.DatabaseConfig) (*pgxpool.Pool, error) {
	poolCfg, err := pgxpool.ParseConfig(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("parsing database URL: %w", err)
	}

	poolCfg.MaxConns = clampInt32(cfg.MaxOpenConns)
	poolCfg.MinConns = clampInt32(cfg.MaxIdleConns)

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("creating connection pool: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pinging database: %w", err)
	}

	return pool, nil
}

// clampInt32 converts an int to int32, clamping to the valid int32 range.
// Pool sizes are config-driven and bounded by validation, but this guards
// against overflow on platforms where int is wider than int32 (gosec G115).
func clampInt32(v int) int32 {
	switch {
	case v > math.MaxInt32:
		return math.MaxInt32
	case v < 0:
		return 0
	default:
		return int32(v)
	}
}
