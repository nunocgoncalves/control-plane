-- HOR-243: permissions store.
--
-- `permissions.policies` materializes PermissionPolicy CRs (Git -> DB bridge),
-- mirroring identity.identities (HOR-242). In v1 broad-default the
-- effective_capabilities view IGNORES policy rows: every active identity gets a
-- wildcard ('*','*') capability. Fine-grained scopes (deepen-phase) will union
-- policy-derived, narrowed rows into the view; the contract (columns + consumer
-- wildcard-match) stays unchanged.
--
-- `permissions.effective_capabilities` IS the permission engine: consumers
-- (gateway HOR-247, agent-fleet) read it directly — no request-path calls to
-- control-plane — and own their Redis cache + freshness (LISTEN/NOTIFY).
-- Contract: identity_id -> set of (resource, action) rows; presence = allow,
-- absence = deny. Broad-default: ('*','*') per active identity.

CREATE TABLE permissions.policies (
    id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    key          text NOT NULL,                 -- stable natural key; CR "<ns>/<name>"
    subject_kind text NOT NULL CHECK (subject_kind IN ('user', 'group', 'service_account', 'workflow')),
    subject_key  text NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now(),
    deleted_at   timestamptz,                   -- soft delete; row kept for history
    UNIQUE (key)
);

CREATE INDEX idx_policies_subject ON permissions.policies (subject_kind, subject_key);

-- The engine: effective capabilities per active identity. Broad-default grants
-- all capabilities (wildcard) to every active identity. Deepen-phase will UNION
-- policy-derived, narrowed rows.
CREATE VIEW permissions.effective_capabilities AS
    SELECT i.id     AS identity_id,
           '*'::text AS resource,
           '*'::text AS action
    FROM identity.identities i
    WHERE i.deleted_at IS NULL;

-- updated_at maintenance (mirrors identity.set_updated_at).
CREATE OR REPLACE FUNCTION permissions.set_updated_at() RETURNS trigger
    LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$;

CREATE TRIGGER policies_updated BEFORE UPDATE ON permissions.policies
    FOR EACH ROW EXECUTE FUNCTION permissions.set_updated_at();

-- NOTIFY: let consumers (gateway/agent-fleet) LISTEN on 'permissions_changed'
-- and refresh their local cache. Fires on any change to effective capabilities:
-- policy changes (permissions.policies) AND identity changes (identity.identities,
-- since the view reads active identities — a soft-deleted identity loses its
-- wildcard, i.e. revocation). The payload names the table + key so a consumer
-- may refine its refresh; control-plane's Go code is unaware of this channel.
CREATE OR REPLACE FUNCTION permissions.notify_change() RETURNS trigger
    LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        PERFORM pg_notify('permissions_changed', json_build_object('table', TG_TABLE_NAME, 'key', OLD.key)::text);
    ELSE
        PERFORM pg_notify('permissions_changed', json_build_object('table', TG_TABLE_NAME, 'key', NEW.key)::text);
    END IF;
    RETURN COALESCE(NEW, OLD);
END;
$$;

CREATE TRIGGER policies_notify AFTER INSERT OR UPDATE OR DELETE ON permissions.policies
    FOR EACH ROW EXECUTE FUNCTION permissions.notify_change();

-- Cross-schema: identity changes also affect effective capabilities (the view
-- reads identity.identities), so notify on it too. This is an additive trigger
-- on HOR-242's table; no HOR-242 code change.
CREATE TRIGGER identities_permissions_notify AFTER INSERT OR UPDATE OR DELETE ON identity.identities
    FOR EACH ROW EXECUTE FUNCTION permissions.notify_change();
