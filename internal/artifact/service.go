package artifact

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Config bounds artifact storage and lifecycle reconciliation. A zero default
// retention means indefinite retention, as approved for v1.
type Config struct {
	MaxSize          int64
	DefaultRetention time.Duration
	PendingTTL       time.Duration
	SweepInterval    time.Duration
}

func (c Config) defaults() Config {
	if c.MaxSize == 0 {
		c.MaxSize = 1 << 30 // 1 GiB; deployment-configurable
	}
	if c.PendingTTL == 0 {
		c.PendingTTL = time.Hour
	}
	if c.SweepInterval == 0 {
		c.SweepInterval = time.Minute
	}
	return c
}

// Service coordinates the non-atomic Postgres + MinIO lifecycle.
type Service struct {
	store   *Store
	objects ObjectStore
	cfg     Config
	log     *slog.Logger
}

func NewService(store *Store, objects ObjectStore, cfg Config, log *slog.Logger) *Service {
	if log == nil {
		log = slog.Default()
	}
	return &Service{store: store, objects: objects, cfg: cfg.defaults(), log: log}
}

func (s *Service) Ready(ctx context.Context) error { return s.objects.Ready(ctx) }

// Store exposes authorization queries to the gateway adapter; byte operations
// remain on Service so callers cannot bypass lifecycle checks.
func (s *Service) Store() *Store { return s.store }

func (s *Service) Upload(ctx context.Context, in UploadInput, body io.Reader) (Artifact, error) {
	if err := validateUpload(in); err != nil {
		return Artifact{}, err
	}
	if body == nil {
		return Artifact{}, fmt.Errorf("%w: body is required", ErrInvalidInput)
	}
	if in.RetentionUntil == nil && s.cfg.DefaultRetention > 0 {
		t := time.Now().Add(s.cfg.DefaultRetention)
		in.RetentionUntil = &t
	}

	id := uuid.NewString()
	key := "artifacts/" + id
	pending, err := s.store.CreatePending(ctx, id, key, in)
	if err != nil {
		return Artifact{}, err
	}

	h := sha256.New()
	counter := &countWriter{}
	limited := &io.LimitedReader{R: body, N: s.cfg.MaxSize + 1}
	stream := io.TeeReader(limited, io.MultiWriter(h, counter))
	if err := s.objects.Put(ctx, key, stream, in.MIMEType); err != nil {
		s.cleanupPending(ctx, pending)
		return Artifact{}, err
	}
	if counter.n > s.cfg.MaxSize {
		s.cleanupPending(ctx, pending)
		return Artifact{}, ErrTooLarge
	}
	digest := "sha256:" + hex.EncodeToString(h.Sum(nil))
	if in.ExpectedSize != nil && *in.ExpectedSize != counter.n {
		s.cleanupPending(ctx, pending)
		return Artifact{}, fmt.Errorf("%w: expected %d, got %d", ErrSize, *in.ExpectedSize, counter.n)
	}
	if in.ExpectedDigest != "" && !strings.EqualFold(in.ExpectedDigest, digest) {
		s.cleanupPending(ctx, pending)
		return Artifact{}, fmt.Errorf("%w: expected %s, got %s", ErrDigest, in.ExpectedDigest, digest)
	}

	a, err := s.store.Finalize(ctx, id, counter.n, digest, in.Scope)
	if err != nil {
		// The row intentionally remains pending if DB finalization failed; the
		// sweeper removes it and the inaccessible object after PendingTTL.
		_ = s.objects.Delete(context.WithoutCancel(ctx), key)
		return Artifact{}, err
	}
	return a, nil
}

func (s *Service) Open(ctx context.Context, id string) (Artifact, io.ReadCloser, error) {
	a, err := s.store.GetAvailable(ctx, id)
	if err != nil {
		return Artifact{}, nil, err
	}
	r, err := s.objects.Get(ctx, a.StorageKey)
	if err != nil {
		return Artifact{}, nil, err
	}
	return a, r, nil
}

func (s *Service) Stat(ctx context.Context, id string) (Artifact, error) {
	return s.store.Get(ctx, id)
}

func (s *Service) Delete(ctx context.Context, id, reason string) (Artifact, error) {
	if reason == "" {
		reason = "explicit"
	}
	a, err := s.store.BeginDelete(ctx, id, reason)
	if err != nil {
		return Artifact{}, err
	}
	if a.State == StateDeleted {
		return a, nil
	}
	if err := s.objects.Delete(ctx, a.StorageKey); err != nil {
		s.store.RecordDeleteError(context.WithoutCancel(ctx), id, err)
		return Artifact{}, err
	}
	if err := s.store.FinishDelete(ctx, id); err != nil {
		return Artifact{}, err
	}
	return s.store.Get(ctx, id)
}

// StartSweeper reconciles stale pending uploads, retryable deletion, and finite
// retention. It is safe to run in both API and gateway processes: transitions
// and object deletion are idempotent.
func (s *Service) StartSweeper(ctx context.Context) {
	s.sweep(ctx)
	t := time.NewTicker(s.cfg.SweepInterval)
	go func() {
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				s.sweep(ctx)
			}
		}
	}()
}

func (s *Service) sweep(ctx context.Context) {
	pending, err := s.store.StalePending(ctx, time.Now().Add(-s.cfg.PendingTTL), 100)
	if err != nil {
		s.log.Warn("artifact pending sweep failed", "error", err)
	} else {
		for _, a := range pending {
			if err := s.objects.Delete(ctx, a.StorageKey); err != nil {
				s.log.Warn("delete stale pending artifact object", "artifact", a.ID, "error", err)
				continue
			}
			s.store.DeletePending(ctx, a.ID)
		}
	}

	expired, err := s.store.Expired(ctx, time.Now(), 100)
	if err != nil {
		s.log.Warn("artifact retention sweep failed", "error", err)
	} else {
		for _, a := range expired {
			if _, err := s.Delete(ctx, a.ID, "retention_expired"); err != nil {
				s.log.Warn("delete expired artifact", "artifact", a.ID, "error", err)
			}
		}
	}

	deleting, err := s.store.Deleting(ctx, time.Now(), 100)
	if err != nil {
		s.log.Warn("artifact deletion retry sweep failed", "error", err)
	} else {
		for _, a := range deleting {
			if _, err := s.Delete(ctx, a.ID, valueOr(a.DeletionReason, "deletion_retry")); err != nil {
				s.log.Warn("retry artifact deletion", "artifact", a.ID, "error", err)
			}
		}
	}
}

func (s *Service) cleanupPending(ctx context.Context, a Artifact) {
	cleanupCtx := context.WithoutCancel(ctx)
	_ = s.objects.Delete(cleanupCtx, a.StorageKey)
	s.store.DeletePending(cleanupCtx, a.ID)
}

func validateUpload(in UploadInput) error {
	switch in.SourceType {
	case SourceUserUpload, SourceSandboxPublish, SourceToolOutput, SourceWorkflow:
	default:
		return fmt.Errorf("%w: invalid source type", ErrInvalidInput)
	}
	if in.CreatedByIdentityID == "" {
		return fmt.Errorf("%w: creator identity is required", ErrInvalidInput)
	}
	if in.MIMEType == "" {
		return fmt.Errorf("%w: MIME type is required", ErrInvalidInput)
	}
	if _, _, err := mime.ParseMediaType(in.MIMEType); err != nil {
		return fmt.Errorf("%w: invalid MIME type: %v", ErrInvalidInput, err)
	}
	if in.ExpectedSize != nil && *in.ExpectedSize < 0 {
		return fmt.Errorf("%w: expected size cannot be negative", ErrInvalidInput)
	}
	if in.ExpectedDigest != "" {
		if len(in.ExpectedDigest) != len("sha256:")+64 || !strings.HasPrefix(strings.ToLower(in.ExpectedDigest), "sha256:") {
			return fmt.Errorf("%w: expected digest must be sha256:<hex>", ErrInvalidInput)
		}
		if _, err := hex.DecodeString(in.ExpectedDigest[len("sha256:"):]); err != nil {
			return fmt.Errorf("%w: malformed expected digest", ErrInvalidInput)
		}
	}
	return nil
}

type countWriter struct{ n int64 }

func (w *countWriter) Write(p []byte) (int, error) {
	w.n += int64(len(p))
	return len(p), nil
}

func valueOr(p *string, fallback string) string {
	if p != nil && *p != "" {
		return *p
	}
	return fallback
}
