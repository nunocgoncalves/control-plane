// Package artifact implements HOR-399's immutable artifact metadata and MinIO
// byte lifecycle. The installation is the tenant boundary; identity records
// attribution and work.artifact_links records execution scope.
package artifact

import (
	"errors"
	"time"
)

var (
	ErrNotFound     = errors.New("artifact: not found")
	ErrNotAvailable = errors.New("artifact: not available")
	ErrUnauthorized = errors.New("artifact: unauthorized")
	ErrInvalidInput = errors.New("artifact: invalid input")
	ErrDigest       = errors.New("artifact: digest mismatch")
	ErrSize         = errors.New("artifact: size mismatch")
	ErrTooLarge     = errors.New("artifact: exceeds maximum size")
)

const (
	StatePending   = "pending"
	StateAvailable = "available"
	StateDeleting  = "deleting"
	StateDeleted   = "deleted"
)

const (
	SourceUserUpload     = "user_upload"
	SourceSandboxPublish = "sandbox_publish"
	SourceToolOutput     = "tool_output"
	SourceWorkflow       = "workflow"
)

// Artifact is the canonical immutable reference metadata. Deleted artifacts
// retain this tombstone while their bytes are absent.
type Artifact struct {
	ID                  string     `json:"artifactId"`
	StorageKey          string     `json:"-"`
	SourceType          string     `json:"source"`
	SourceRef           *string    `json:"sourceRef,omitempty"`
	CreatedByIdentityID string     `json:"createdByIdentityId"`
	MIMEType            string     `json:"mimeType"`
	SizeBytes           *int64     `json:"sizeBytes,omitempty"`
	Digest              *string    `json:"digest,omitempty"`
	State               string     `json:"state"`
	RetentionUntil      *time.Time `json:"retentionUntil,omitempty"`
	DeletionReason      *string    `json:"deletionReason,omitempty"`
	CreatedAt           time.Time  `json:"createdAt"`
	AvailableAt         *time.Time `json:"availableAt,omitempty"`
	DeletedAt           *time.Time `json:"deletedAt,omitempty"`
}

// Ref is the canonical wire reference. Digest uses sha256:<lowercase hex>.
type Ref struct {
	ArtifactID string `json:"artifactId"`
	MIMEType   string `json:"mimeType"`
	SizeBytes  int64  `json:"sizeBytes"`
	Digest     string `json:"digest"`
}

func (a Artifact) Ref() (Ref, error) {
	if a.State != StateAvailable || a.SizeBytes == nil || a.Digest == nil {
		return Ref{}, ErrNotAvailable
	}
	return Ref{ArtifactID: a.ID, MIMEType: a.MIMEType, SizeBytes: *a.SizeBytes, Digest: *a.Digest}, nil
}

// Scope is an immutable relationship to durable customer work. Run and attempt
// share one UUID in v1, so only AttemptID is stored.
type Scope struct {
	WorkItemID      string
	AttemptID       string
	NodeExecutionID string
	Role            string
}

// UploadInput is trusted, authorization-derived metadata plus optional caller
// integrity claims. Scope may be empty only for an authorized tenant user
// upload that will be linked when work starts.
type UploadInput struct {
	SourceType          string
	SourceRef           string
	CreatedByIdentityID string
	MIMEType            string
	ExpectedSize        *int64
	ExpectedDigest      string
	RetentionUntil      *time.Time
	Scope               *Scope
}
