-- HOR-306: catalog store (backends). The Model CRD (HOR-268) adds catalog.models
-- + the effective_catalog view; both backends and models notify on the shared
-- 'catalog_changed' channel so consumers (gateway HOR-247, agent-fleet) LISTEN
-- and refresh their local cache — no request-path calls to control-plane.
--
-- catalog.backends materializes ModelBackend CRs (Git -> DB bridge), mirroring
-- identity.identities (HOR-242) and permissions.policies (HOR-243). Contract:
-- key -> (kind, model, service_url, deployed, healthy). The effective_catalog
-- view (HOR-268) joins Model -> ModelBackend and exposes only available rows.

CREATE SCHEMA IF NOT EXISTS catalog;

CREATE TABLE catalog.backends (
    id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    key         text NOT NULL,                 -- stable natural key; CR "<ns>/<name>"
    name        text NOT NULL,                 -- ModelBackend name (Service name)
    namespace   text NOT NULL,
    kind        text NOT NULL CHECK (kind IN ('vLLM', 'SGLang', 'external')),
    model       text,                          -- HuggingFace id (vLLM/SGLang --model)
    service_url text NOT NULL,                 -- in-cluster Service URL / external baseURL
    image       text,
    deployed    boolean NOT NULL DEFAULT false,
    healthy     boolean NOT NULL DEFAULT false,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now(),
    deleted_at  timestamptz,                   -- soft delete; row kept for history
    UNIQUE (key)
);

CREATE INDEX idx_backends_healthy ON catalog.backends (healthy) WHERE deleted_at IS NULL;

-- updated_at maintenance (mirrors identity/permissions).
CREATE OR REPLACE FUNCTION catalog.set_updated_at() RETURNS trigger
    LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$;

CREATE TRIGGER backends_updated BEFORE UPDATE ON catalog.backends
    FOR EACH ROW EXECUTE FUNCTION catalog.set_updated_at();

-- NOTIFY: let consumers (gateway/agent-fleet) LISTEN on 'catalog_changed' and
-- refresh their local cache. HOR-268 adds the same trigger on catalog.models.
-- The payload names the table + key so a consumer may refine its refresh;
-- control-plane's Go code is unaware of this channel (mirrors HOR-243).
CREATE OR REPLACE FUNCTION catalog.notify_change() RETURNS trigger
    LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        PERFORM pg_notify('catalog_changed', json_build_object('table', TG_TABLE_NAME, 'key', OLD.key)::text);
    ELSE
        PERFORM pg_notify('catalog_changed', json_build_object('table', TG_TABLE_NAME, 'key', NEW.key)::text);
    END IF;
    RETURN COALESCE(NEW, OLD);
END;
$$;

CREATE TRIGGER backends_notify AFTER INSERT OR UPDATE OR DELETE ON catalog.backends
    FOR EACH ROW EXECUTE FUNCTION catalog.notify_change();
