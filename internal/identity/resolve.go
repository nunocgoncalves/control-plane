package identity

import (
	"context"
	"errors"
)

// Identity-resolution modes.
const (
	// ModeEnrolled is the S8 default: a linked IdentityMapping is required; an
	// unlinked surface user is denied.
	ModeEnrolled = "enrolled"
	// ModeOpen auto-provisions any workspace member with all capabilities. It is
	// deferred to HOR-313; the token endpoint returns 501 in the skeleton.
	ModeOpen = "open"
)

// ErrOpenModeNotImplemented is returned by Resolve when open mode is configured
// but not yet wired (HOR-313).
var ErrOpenModeNotImplemented = errors.New("identity: open mode is not yet implemented (HOR-313)")

// Resolver maps a surface user (provider + type + external ID) to a platform
// identity, according to the configured mode.
type Resolver struct {
	store *Store
	mode  string
}

// NewResolver builds a resolver for the given mode (enrolled or open).
func NewResolver(store *Store, mode string) *Resolver {
	return &Resolver{store: store, mode: mode}
}

// Resolve resolves a surface user to an identity.
//   - enrolled: a linked IdentityMapping is required; unlinked -> ErrNotFound
//     (denied at the token endpoint).
//   - open: returns ErrOpenModeNotImplemented (deferred, HOR-313).
func (r *Resolver) Resolve(ctx context.Context, provider, bindingType, externalID string) (Identity, error) {
	if r.mode == ModeOpen {
		return Identity{}, ErrOpenModeNotImplemented
	}
	return r.store.ResolveByExternalID(ctx, provider, bindingType, externalID)
}

// Mode returns the configured resolution mode.
func (r *Resolver) Mode() string { return r.mode }
