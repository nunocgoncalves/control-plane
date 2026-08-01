-- HOR-245 / ARCH-009: reverse 000012_egress_drop — recreate the v1 HOR-244
-- `egress` schema (credentials + effective_routes view + updated_at trigger).
-- This restores the superseded data-scope plane shape; the Go code that
-- populated/consumed it is NOT restored (it was removed under ARCH-009), so the
-- schema stays empty. Reversal exists for migration-level safety only.

CREATE SCHEMA IF NOT EXISTS egress;

CREATE TABLE IF NOT EXISTS egress.credentials (
    id               uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    key              text NOT NULL,                 -- stable natural key; CR "<ns>/<name>"
    name             text NOT NULL,                 -- route-id
    namespace        text NOT NULL,
    upstream_base_url text NOT NULL,
    auth             jsonb NOT NULL,
    subject          jsonb,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    deleted_at       timestamptz,
    UNIQUE (key)
);

CREATE INDEX IF NOT EXISTS idx_egress_credentials_active ON egress.credentials (name) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_egress_credentials_namespace ON egress.credentials (namespace) WHERE deleted_at IS NULL;

CREATE OR REPLACE FUNCTION egress.set_updated_at() RETURNS trigger
    LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS credentials_updated ON egress.credentials;
CREATE TRIGGER credentials_updated BEFORE UPDATE ON egress.credentials
    FOR EACH ROW EXECUTE FUNCTION egress.set_updated_at();

CREATE OR REPLACE VIEW egress.effective_routes AS
    SELECT i.id                AS identity_id,
           c.name              AS route_id,
           c.upstream_base_url AS upstream_base_url,
           c.auth              AS auth
    FROM egress.credentials c
    CROSS JOIN identity.identities i
    WHERE c.deleted_at IS NULL
      AND i.deleted_at IS NULL;
