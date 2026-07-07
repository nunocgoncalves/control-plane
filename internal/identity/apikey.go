package identity

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base32"
	"encoding/hex"
	"fmt"
	"strings"
)

// API key scopes. A key's scope determines what it can authenticate:
//   - admin   - control-plane API management (bootstrap admin)
//   - token   - service accounts calling POST /v1/token (agent-fleet, Path 2)
//   - gateway - end users calling the gateway directly (Path 1); validated by
//     the gateway from its control-plane-synced snapshot, not here.
const (
	ScopeAdmin   = "admin"
	ScopeToken   = "token"
	ScopeGateway = "gateway"
)

const (
	apiKeyPrefix     = "cp-" // long-lived API keys are cp-prefixed (mirrors inference-gateway's ml- prefix)
	randomPartBytes  = 20    // 160 bits of entropy -> 32 base32 chars
	prefixDisplayLen = 12    // chars of the full key retained for display/identification
)

// GenerateAPIKey creates a new random API key. It returns:
//   - full:  the complete key ("cp-" + 32 base32 chars); shown ONCE to the caller
//   - prefix: a short display prefix stored alongside the hash
//   - hash:  the sha256 hex of full, stored for lookup (never store full)
func GenerateAPIKey() (full, prefix, hash string, err error) {
	b := make([]byte, randomPartBytes)
	if _, err := rand.Read(b); err != nil {
		return "", "", "", fmt.Errorf("generating api key: %w", err)
	}
	// base32 (no padding) is URL-safe and unambiguous to type/read aloud.
	enc := base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(b)
	full = apiKeyPrefix + enc
	prefix = full
	if len(prefix) > prefixDisplayLen {
		prefix = prefix[:prefixDisplayLen]
	}
	hash = HashAPIKey(full)
	return full, prefix, hash, nil
}

// HashAPIKey returns the sha256 hex of the full key for storage and lookup.
func HashAPIKey(full string) string {
	sum := sha256.Sum256([]byte(full))
	return hex.EncodeToString(sum[:])
}

// ParseBearer extracts the token from an "Authorization: Bearer <token>"
// header value. Returns the token and true if present, "" and false otherwise.
func ParseBearer(authHeader string) (string, bool) {
	const prefix = "Bearer "
	if len(authHeader) <= len(prefix) || !strings.EqualFold(authHeader[:len(prefix)], prefix) {
		return "", false
	}
	token := strings.TrimSpace(authHeader[len(prefix):])
	if token == "" {
		return "", false
	}
	return token, true
}

// ValidScope reports whether s is a recognized API key scope.
func ValidScope(s string) bool {
	switch s {
	case ScopeAdmin, ScopeToken, ScopeGateway:
		return true
	}
	return false
}
