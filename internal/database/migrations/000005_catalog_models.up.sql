-- HOR-268: catalog.models + the effective_catalog view. The Model CRD is the
-- offering the gateway (HOR-247) consumes; it references a ModelBackend
-- (HOR-306). The view joins Model -> ModelBackend and exposes only the rows a
-- consumer routes on (available = Model.available AND backend.healthy). Consumers
-- read the view directly + LISTEN on 'catalog_changed' (mirrors HOR-243).
--
-- catalog.models materializes Model CRs (Git -> DB bridge). Per-alias request
-- config (default_params/reasoning_config/transforms/rate_limits) is stored as
-- JSONB, translating the inference-gateway's registry.Model so the gateway
-- (HOR-247) reads it from the catalog instead of its self-managed store.
-- Multiple Models may reference one ModelBackend (aliases, e.g. reasoning on/off).

CREATE TABLE catalog.models (
    id               uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    key              text NOT NULL,                 -- stable natural key; CR "<ns>/<name>"
    namespace        text NOT NULL,
    model_id         text NOT NULL,                 -- client-facing alias clients request
    display_name     text,
    context_length   integer,
    capabilities     jsonb NOT NULL DEFAULT '[]'::jsonb,
    backend_ref      text NOT NULL,                 -- ModelBackend name (same namespace)
    default_params   jsonb NOT NULL DEFAULT '{}'::jsonb,
    reasoning_config jsonb NOT NULL DEFAULT '{}'::jsonb,
    transforms       jsonb NOT NULL DEFAULT '{}'::jsonb,
    rate_limits      jsonb NOT NULL DEFAULT '{}'::jsonb,
    available        boolean NOT NULL DEFAULT false,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    deleted_at       timestamptz,                   -- soft delete; row kept for history
    UNIQUE (key)
);

-- One active alias per (namespace, model_id): two Models can't claim the same
-- client-facing alias in the same namespace.
CREATE UNIQUE INDEX idx_models_model_id ON catalog.models (namespace, model_id) WHERE deleted_at IS NULL;
CREATE INDEX idx_models_backend ON catalog.models (namespace, backend_ref) WHERE deleted_at IS NULL;

-- updated_at maintenance (reuses catalog.set_updated_at() from HOR-306/000004).
CREATE TRIGGER models_updated BEFORE UPDATE ON catalog.models
    FOR EACH ROW EXECUTE FUNCTION catalog.set_updated_at();

-- NOTIFY on catalog.models (reuses catalog.notify_change() from HOR-306/000004).
CREATE TRIGGER models_notify AFTER INSERT OR UPDATE OR DELETE ON catalog.models
    FOR EACH ROW EXECUTE FUNCTION catalog.notify_change();

-- The catalog: Model -> ModelBackend join. backend_model_id is the HuggingFace id
-- vLLM serves under; the gateway rewrites the client's alias -> backend_model_id.
-- available is the conjunction the gateway routes on.
CREATE VIEW catalog.effective_catalog AS
    SELECT m.key               AS model_key,
           m.namespace         AS namespace,
           m.model_id          AS model_id,
           m.display_name      AS display_name,
           m.context_length    AS context_length,
           m.capabilities      AS capabilities,
           m.backend_ref       AS backend_ref,
           b.kind              AS backend_kind,
           b.model             AS backend_model_id,
           b.service_url       AS backend_url,
           m.default_params    AS default_params,
           m.reasoning_config  AS reasoning_config,
           m.transforms        AS transforms,
           m.rate_limits       AS rate_limits,
           (m.available AND b.healthy) AS available
    FROM catalog.models m
    JOIN catalog.backends b
      ON b.name = m.backend_ref AND b.namespace = m.namespace AND b.deleted_at IS NULL
    WHERE m.deleted_at IS NULL;
