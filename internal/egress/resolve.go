package egress

import (
	"context"
	"encoding/json"
	"fmt"
)

// Resolve returns the effective tool routes for a scope identity, plus the
// distinct K8s Secret references the AgentSandbox operator (HOR-245) must
// mount into the proxy. It reads egress.effective_routes directly — no
// request-path calls, no caching (the operator calls it once at provisioning
// and on EgressRoute change).
//
// Broad-default (v1): every active identity gets every route. An inactive or
// unknown identity resolves to zero routes (denied) — the view filters to
// active identities, so this falls out naturally. The model route is NOT
// included here (it is platform-wide, not identity-scoped); the operator adds
// it from platform config.
func (s *Store) Resolve(ctx context.Context, scopeIdentityID string) (ResolveResult, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT route_id, upstream_base_url, auth
		FROM egress.effective_routes
		WHERE identity_id = $1
		ORDER BY route_id`, scopeIdentityID)
	if err != nil {
		return ResolveResult{}, fmt.Errorf("query effective_routes: %w", err)
	}
	defer rows.Close()

	var out ResolveResult
	seen := make(map[SecretRef]struct{})
	for rows.Next() {
		var r ResolvedRoute
		var authJSON []byte
		if err := rows.Scan(&r.RouteID, &r.UpstreamBaseURL, &authJSON); err != nil {
			return ResolveResult{}, err
		}
		if err := unmarshalJSON(authJSON, &r.Auth); err != nil {
			return ResolveResult{}, fmt.Errorf("unmarshal auth for %s: %w", r.RouteID, err)
		}
		out.Routes = append(out.Routes, r)

		// Collect the distinct secret refs the operator must mount.
		for _, ref := range r.Auth.secretRefs() {
			if _, ok := seen[ref]; !ok {
				seen[ref] = struct{}{}
				out.SecretRefs = append(out.SecretRefs, ref)
			}
		}
	}
	return out, rows.Err()
}

// secretRefs returns the K8s Secret references referenced by an Auth (one for
// bearer, one for oauthClientCredentials).
func (a Auth) secretRefs() []SecretRef {
	var refs []SecretRef
	if a.SecretRef != nil {
		refs = append(refs, *a.SecretRef)
	}
	if a.ClientSecretRef != nil {
		refs = append(refs, *a.ClientSecretRef)
	}
	return refs
}

// marshalJSON marshals v, defaulting empty to "{}" for non-null JSONB columns.
func marshalJSON(v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return b, nil
}

// unmarshalJSON unmarshals b into v.
func unmarshalJSON(b []byte, v any) error {
	if len(b) == 0 {
		return nil
	}
	return json.Unmarshal(b, v)
}
