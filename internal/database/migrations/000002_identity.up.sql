-- HOR-242: identity store.
--
-- Unified `identities` table is the join point for HOR-243 (permissions) and
-- usage (S8). Satellite tables specialize identities:
--   external_mappings - binds a platform identity to an external provider ID
--                       (Slack/Teams). Populated by the IdentityMapping
--                       reconciler (source=external) and, in open mode
--                       (HOR-313, deferred), by auto-provisioning.
--   local_users       - human accounts created via the API/bootstrap
--                       (source=local). API-key only in the skeleton;
--                       password_hash is deferred (S7).
--   api_keys          - long-lived `cp-` keys (sha256 hashed) bound to an
--                       identity, carrying a scope (admin|token|gateway).
--
-- Service accounts are identities (kind=service_account, source=local) with
-- api_keys; they have no satellite table.

CREATE TABLE identity.identities (
    id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    key          text NOT NULL,                 -- stable natural key; CR "<ns>/<name>", local user email, or SA name
    kind         text NOT NULL CHECK (kind IN ('user', 'group', 'service_account', 'workflow')),
    source       text NOT NULL CHECK (source IN ('local', 'external', 'external-auto')),
    display_name text NOT NULL DEFAULT '',
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now(),
    deleted_at   timestamptz,                   -- soft delete (HOR-242 Q9): row kept for usage/history; access revoked via removed bindings
    UNIQUE (key)
);

CREATE TABLE identity.external_mappings (
    id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    identity_id uuid NOT NULL REFERENCES identity.identities(id) ON DELETE CASCADE,
    provider    text NOT NULL CHECK (provider IN ('slack', 'teams')),
    type        text NOT NULL CHECK (type IN ('user', 'group')),
    external_id text NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    -- One external binding resolves to exactly one identity.
    UNIQUE (provider, type, external_id)
);

CREATE TABLE identity.local_users (
    id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    identity_id   uuid NOT NULL UNIQUE REFERENCES identity.identities(id) ON DELETE CASCADE,
    email         text NOT NULL,
    role          text NOT NULL DEFAULT 'user' CHECK (role IN ('admin', 'user')),
    password_hash text,                        -- deferred (S7); API-key only in skeleton
    created_at    timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now(),
    UNIQUE (email)
);

CREATE TABLE identity.api_keys (
    id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    identity_id  uuid NOT NULL REFERENCES identity.identities(id) ON DELETE CASCADE,
    key_hash     text NOT NULL,                 -- sha256 hex of the full key
    prefix       text NOT NULL,                 -- display prefix (first chars) for identification
    name         text NOT NULL DEFAULT '',
    scope        text NOT NULL CHECK (scope IN ('admin', 'token', 'gateway')),
    expires_at   timestamptz,                   -- optional; NULL = no expiry
    last_used_at timestamptz,
    revoked_at   timestamptz,                   -- soft revoke
    created_at   timestamptz NOT NULL DEFAULT now(),
    UNIQUE (key_hash)
);

CREATE INDEX idx_external_mappings_identity ON identity.external_mappings (identity_id);
CREATE INDEX idx_external_mappings_resolve ON identity.external_mappings (provider, type, external_id);
CREATE INDEX idx_api_keys_identity ON identity.api_keys (identity_id);
CREATE INDEX idx_identities_kind_source ON identity.identities (kind, source);

-- updated_at maintenance.
CREATE OR REPLACE FUNCTION identity.set_updated_at() RETURNS trigger
    LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$;

CREATE TRIGGER ident_updated BEFORE UPDATE ON identity.identities
    FOR EACH ROW EXECUTE FUNCTION identity.set_updated_at();
CREATE TRIGGER local_users_updated BEFORE UPDATE ON identity.local_users
    FOR EACH ROW EXECUTE FUNCTION identity.set_updated_at();
