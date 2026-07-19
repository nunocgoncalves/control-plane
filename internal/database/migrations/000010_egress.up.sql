-- HOR-244: egress credential source (the data-scope plane).
--
-- `egress.credentials` materializes EgressRoute CRs (Git -> DB bridge),
-- mirroring identity.identities (HOR-242) / permissions.policies (HOR-243) /
-- catalog.backends (HOR-306). An EgressRoute is one upstream the per-sandbox
-- egress proxy may forward to, plus how it injects the real credential
-- (credentials-as-scope: the agent can only reach what its scoped creds
-- allow). The credential *value* never lives here -- only the K8s Secret
-- reference (name+key); the value stays in a K8s Secret, mounted into the
-- proxy by the AgentSandbox operator (HOR-245).
--
-- `egress.effective_routes` IS the data-scope resolution contract: a consumer
-- (the AgentSandbox operator at provisioning, via internal/egress.Resolve)
-- reads it directly -- no request-path calls, no pg_notify (the runtime is
-- internal; EgressRoute changes are detected via a K8s watch on the CR, not
-- Postgres NOTIFY). Contract: identity_id -> set of (route_id, upstream,
-- auth) rows; presence = allowed, absence = denied.
--
-- Broad-default (v1): every active identity gets every route -- the view
-- IGNORES `subject` (stored, not enforced), exactly like
-- permissions.effective_capabilities ignores permissions.policies scopes in
-- v1. Group-scoped narrowing is additive later: the view will UNION
-- subject-restricted rows once a group-membership model lands (SSO/HOR-314);
-- the `subject` column is already here so no reshuffle is needed.

CREATE SCHEMA IF NOT EXISTS egress;

CREATE TABLE egress.credentials (
    id               uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    key              text NOT NULL,                 -- stable natural key; CR "<ns>/<name>"
    name             text NOT NULL,                 -- route-id (the EgressRoute name); tools address /upstreams/<name>/...
    namespace        text NOT NULL,
    upstream_base_url text NOT NULL,                -- the real target the proxy forwards to
    auth             jsonb NOT NULL,                -- {scheme, secretRef|clientSecretRef, ...}; the cred VALUE is never here
    subject          jsonb,                         -- stored, NOT enforced in v1 (broad-default); {kind, key} for group-scoping later
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    deleted_at       timestamptz,                   -- soft delete; row kept for history
    UNIQUE (key)
);

CREATE INDEX idx_egress_credentials_active ON egress.credentials (name) WHERE deleted_at IS NULL;
CREATE INDEX idx_egress_credentials_namespace ON egress.credentials (namespace) WHERE deleted_at IS NULL;

-- updated_at maintenance (mirrors identity/permissions/catalog).
CREATE OR REPLACE FUNCTION egress.set_updated_at() RETURNS trigger
    LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$;

CREATE TRIGGER credentials_updated BEFORE UPDATE ON egress.credentials
    FOR EACH ROW EXECUTE FUNCTION egress.set_updated_at();

-- The data-scope engine: effective routes per active identity. Broad-default
-- grants every route to every active identity (subject ignored). Deepen-phase
-- will narrow by subject + group membership; the contract (columns + consumer
-- resolution) stays unchanged.
CREATE VIEW egress.effective_routes AS
    SELECT i.id                AS identity_id,
           c.name              AS route_id,
           c.upstream_base_url AS upstream_base_url,
           c.auth              AS auth
    FROM egress.credentials c
    CROSS JOIN identity.identities i
    WHERE c.deleted_at IS NULL
      AND i.deleted_at IS NULL;
