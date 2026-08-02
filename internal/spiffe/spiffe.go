// Package spiffe implements SPIFFE workload-identity verification for the
// control-plane's mTLS gRPC servers (HOR-392 tool gateway; reused later by
// HOR-249's harness Work server).
//
// The gateway terminates native gRPC over HTTP/2 + mTLS (ARCH-004/010/015).
// The client certificate's URI SAN is the authoritative caller identity; the
// gateway resolves scope from durable state rather than trusting agent-supplied
// scope (ARCH-004). Go's tls stack verifies the certificate chain against the
// configured ClientCAs (RequireAndVerifyClientCert); this package extracts and
// parses the SPIFFE URI SAN into a typed Identity.
//
// Trust domain is `iterabase.local` by default (configurable). Three caller
// classes (ARCH-012):
//
//   - Supervisor (turn-scoped):    spiffe://<td>/pools/<pool-uid>/workers/<pod-name>
//   - Tool runner:                 spiffe://<td>/tool-runners/<namespace>/<runner-id>
//   - Control-plane workflow step: spiffe://<td>/control-plane/workflow-runtime
package spiffe

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/url"
	"strings"
)

// DefaultTrustDomain is the SPIFFE trust domain for the control plane.
const DefaultTrustDomain = "iterabase.local"

// Kind classifies a verified caller by its SPIFFE id path.
type Kind int

const (
	KindUnknown              Kind = iota
	KindSupervisor                // /pools/<pool>/workers/<worker>
	KindRunner                    // /tool-runners/<namespace>/<runner-id>
	KindControlPlaneWorkflow      // /control-plane/workflow-runtime
)

func (k Kind) String() string {
	switch k {
	case KindSupervisor:
		return "supervisor"
	case KindRunner:
		return "runner"
	case KindControlPlaneWorkflow:
		return "control-plane-workflow"
	}
	return "unknown"
}

// Identity is a parsed, trust-domain-validated SPIFFE workload identity.
type Identity struct {
	SPIFFEID    string
	Kind        Kind
	TrustDomain string
	// Supervisor fields.
	PoolUID  string
	WorkerID string // pod name (stable warm-worker slot, verified cert SAN)
	// Runner fields.
	Namespace string
	RunnerID  string
}

// TrustConfig is the mTLS trust configuration for a gRPC server.
type TrustConfig struct {
	TrustDomain string
	ClientCAs   *x509.CertPool
}

// IdentityFromCerts extracts the SPIFFE identity from a peer certificate chain.
// The chain is assumed already verified by tls (RequireAndVerifyClientCert);
// this parses the URI SAN of the leaf cert and validates the trust domain +
// path structure. Returns an error if no URI SAN is present or the id is
// malformed — the caller fails closed (PermissionDenied).
func IdentityFromCerts(certs []*x509.Certificate, trustDomain string) (Identity, error) {
	if trustDomain == "" {
		trustDomain = DefaultTrustDomain
	}
	if len(certs) == 0 {
		return Identity{}, errors.New("spiffe: no peer certificate")
	}
	leaf := certs[0]
	if len(leaf.URIs) == 0 {
		return Identity{}, errors.New("spiffe: peer certificate has no URI SAN")
	}
	// A SPIFFE id is a single URI SAN; use the first.
	id, err := Parse(leaf.URIs[0].String(), trustDomain)
	if err != nil {
		return Identity{}, err
	}
	return id, nil
}

// IdentityFromConnState extracts the identity from a TLS connection state.
func IdentityFromConnState(cs *tls.ConnectionState, trustDomain string) (Identity, error) {
	if cs == nil {
		return Identity{}, errors.New("spiffe: no TLS connection state")
	}
	return IdentityFromCerts(cs.PeerCertificates, trustDomain)
}

// Parse validates a SPIFFE id string against the trust domain and parses its
// path into a typed Identity.
func Parse(spiffeID string, trustDomain string) (Identity, error) {
	if trustDomain == "" {
		trustDomain = DefaultTrustDomain
	}
	u, err := url.Parse(spiffeID)
	if err != nil {
		return Identity{}, fmt.Errorf("spiffe: parse id: %w", err)
	}
	if u.Scheme != "spiffe" {
		return Identity{}, fmt.Errorf("spiffe: id %q is not a spiffe:// URI", spiffeID)
	}
	if u.Host != trustDomain {
		return Identity{}, fmt.Errorf("spiffe: trust domain mismatch: id %q != %q", u.Host, trustDomain)
	}
	id := Identity{SPIFFEID: spiffeID, TrustDomain: u.Host}
	segments := splitPath(u.Path)
	switch {
	case len(segments) == 4 && segments[0] == "pools" && segments[2] == "workers":
		id.Kind = KindSupervisor
		id.PoolUID = segments[1]
		id.WorkerID = segments[3]
	case len(segments) == 3 && segments[0] == "tool-runners":
		id.Kind = KindRunner
		id.Namespace = segments[1]
		id.RunnerID = segments[2]
	case len(segments) == 2 && segments[0] == "control-plane" && segments[1] == "workflow-runtime":
		id.Kind = KindControlPlaneWorkflow
	default:
		return Identity{}, fmt.Errorf("spiffe: unrecognized id path %q", u.Path)
	}
	return id, nil
}

// splitPath splits a URL path into non-empty segments.
func splitPath(p string) []string {
	p = strings.Trim(p, "/")
	if p == "" {
		return nil
	}
	return strings.Split(p, "/")
}

// ServerTLSConfig builds the mTLS server TLS config: HTTP/2-only, require +
// verify client cert against ClientCAs. The gateway stamps the verified
// Identity from the request's TLS conn state in its handler/interceptor.
func ServerTLSConfig(serverCert tls.Certificate, clientCAs *x509.CertPool) *tls.Config {
	return &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    clientCAs,
		MinVersion:   tls.VersionTLS12,
		NextProtos:   []string{"h2"}, // HTTP/2 only — no HTTP/1.1 fallback (ARCH-006)
	}
}
