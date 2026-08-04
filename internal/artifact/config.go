package artifact

import (
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nunocgoncalves/control-plane/internal/config"
)

// NewConfiguredService builds the shared artifact domain from process config.
func NewConfiguredService(cfg *config.Config, pool *pgxpool.Pool, log *slog.Logger) (*Service, error) {
	objects, err := NewMinIOStore(MinIOConfig{
		Endpoint: cfg.Artifact.Endpoint, AccessKey: cfg.Artifact.AccessKey,
		SecretKey: cfg.Artifact.SecretKey, Bucket: cfg.Artifact.Bucket, Secure: cfg.Artifact.Secure,
	})
	if err != nil {
		return nil, err
	}
	retention, pendingTTL, sweepInterval := config.ArtifactDurations(cfg)
	return NewService(NewStore(pool), objects, Config{
		MaxSize: cfg.Artifact.MaxSizeBytes, DefaultRetention: retention,
		PendingTTL: pendingTTL, SweepInterval: sweepInterval,
	}, log), nil
}
