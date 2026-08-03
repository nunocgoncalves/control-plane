-- HOR-252: operator-defined workflow model + trigger registration.
--
-- The control-plane workflow definition data layer. An operator deploys one
-- versioned customer workflow from product/client overlay artifacts (REQ-001):
-- its source adapter/trigger, deterministic steps + agent tasks, requested
-- gateway capabilities, customer-facing workflow/persona labels, completion
-- rule, blocker behavior, and value-model reference. Definitions are immutable
-- per version (ARCH-007): publishing a content change creates a new (key,
-- version, digest) row; the same (key, version) with different content is
-- rejected in Go. Re-registering the same digest is idempotent.
--
-- Trigger bindings carry ONLY non-secret routing identifiers (mailbox address,
-- artifact source id). Customer secret VALUES are never embedded here;
-- authenticated source access resolves credentials through the AgentPool's
-- credentialBindings via the gateway (ARCH-008). The Workflow CRD has no
-- secret-value fields by design.
--
-- The definition_key wire format is "<key>:<version>" (stable across schemas).
-- It is stored in:
--   * runtime.workflow_runs.definition_key          (HOR-246, no cross-schema FK
--     by design — resolved in Go, mirroring the existing no-FK-until-later
--     pattern the runtime migration documents)
--   * toolgateway.workflow_pool_bindings.workflow_definition_key (HOR-392, same
--     no-FK pattern).
-- HOR-252 populates workflow_pool_bindings with the workflow's permitted tools
-- (requested capabilities validated against the AgentPool's grants, ARCH-018),
-- and creates a kind=workflow scope identity (HOR-242) runs execute under.
--
-- Mirrors the identity (HOR-242) / permissions (HOR-243) / catalog (HOR-306) /
-- runtime (HOR-246) / toolgateway (HOR-392) stores: pgxpool store +
-- ErrNotFound, soft-delete on operator-owned config tables, set_updated_at
-- triggers, no pg_notify.

CREATE SCHEMA IF NOT EXISTS workflow;

-- ---------------------------------------------------------------------------
-- definitions: immutable versioned workflow definitions (ARCH-007/REQ-001).
--
-- (key, version) is unique; (key, digest) is unique. A content change publishes
-- a NEW row (a new version); old versions remain resolvable for in-flight
-- attempts. validation_status is inspectable (REQ-001 acceptance: "registered
-- with immutable version identity and inspectable validation status"). scope_identity_id
-- is the kind=workflow identity runs execute under (HOR-242).
-- ---------------------------------------------------------------------------
CREATE TABLE workflow.definitions (
    id                uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    key               text NOT NULL,                       -- stable natural key (e.g. "walter/quotation")
    version           text NOT NULL,                       -- immutable version identity component
    digest            text NOT NULL,                       -- canonical spec content digest (immutable identity)
    spec_json         jsonb NOT NULL,                      -- the canonical materialized spec
    validation_status text NOT NULL DEFAULT 'valid' CHECK (validation_status IN ('valid', 'invalid')),
    scope_identity_id uuid NOT NULL REFERENCES identity.identities(id),
    source_type       text NOT NULL CHECK (source_type IN ('graph_email', 'operator_artifact')),
    pool_key          text NOT NULL,                       -- AgentPool natural key "<ns>/<name>" (resolved to pool_id in toolgateway)
    presentation      jsonb NOT NULL DEFAULT '{}'::jsonb,  -- customer-facing labels + persona (REQ-021)
    created_at        timestamptz NOT NULL DEFAULT now(),
    updated_at        timestamptz NOT NULL DEFAULT now(),
    deleted_at        timestamptz,
    UNIQUE (key, version),
    UNIQUE (key, digest)
);

CREATE INDEX idx_definitions_key ON workflow.definitions (key) WHERE deleted_at IS NULL;
CREATE INDEX idx_definitions_scope_identity ON workflow.definitions (scope_identity_id);

-- ---------------------------------------------------------------------------
-- trigger_bindings: non-secret trigger route registrations (ARCH-012/REQ-002).
-- One row per (definition, binding name). config is opaque non-secret JSONB.
-- No secret values are stored; credentials resolve via the AgentPool's
-- credentialBindings through the gateway (ARCH-008).
-- ---------------------------------------------------------------------------
CREATE TABLE workflow.trigger_bindings (
    id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    definition_id uuid NOT NULL REFERENCES workflow.definitions(id) ON DELETE CASCADE,
    name          text NOT NULL,                 -- logical binding name, unique per definition
    source_type   text NOT NULL CHECK (source_type IN ('graph_email', 'operator_artifact')),
    binding_key   text NOT NULL,                 -- non-secret routing identifier (mailbox/artifact source id)
    config        jsonb NOT NULL DEFAULT '{}'::jsonb,  -- opaque non-secret trigger config
    created_at    timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now(),
    deleted_at    timestamptz,
    UNIQUE (definition_id, name)
);

CREATE INDEX idx_trigger_bindings_definition ON workflow.trigger_bindings (definition_id) WHERE deleted_at IS NULL;

-- updated_at maintenance (mirrors identity/catalog/runtime/toolgateway).
CREATE OR REPLACE FUNCTION workflow.set_updated_at() RETURNS trigger
    LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$;

CREATE TRIGGER definitions_updated BEFORE UPDATE ON workflow.definitions
    FOR EACH ROW EXECUTE FUNCTION workflow.set_updated_at();
CREATE TRIGGER trigger_bindings_updated BEFORE UPDATE ON workflow.trigger_bindings
    FOR EACH ROW EXECUTE FUNCTION workflow.set_updated_at();
