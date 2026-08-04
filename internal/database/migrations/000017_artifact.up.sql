-- HOR-399: immutable artifact metadata + durable MinIO lifecycle.
--
-- The installation is the tenant boundary. identity.identities records the
-- actor; work.artifact_links records the immutable work/attempt/node scope.
-- Bytes live under one unique object key in MinIO and are visible only while
-- metadata is available. Cross-store transitions are pending -> available ->
-- deleting -> deleted; object keys and artifact ids are never reused.

CREATE SCHEMA artifact;

CREATE TABLE artifact.artifacts (
    id                    uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    storage_key           text NOT NULL UNIQUE,
    source_type           text NOT NULL CHECK (source_type IN
                              ('user_upload', 'sandbox_publish', 'tool_output', 'workflow')),
    source_ref            text,
    created_by_identity_id uuid NOT NULL REFERENCES identity.identities(id) ON DELETE RESTRICT,
    mime_type             text NOT NULL,
    size_bytes            bigint CHECK (size_bytes IS NULL OR size_bytes >= 0),
    digest                text CHECK (digest IS NULL OR digest ~ '^sha256:[0-9a-f]{64}$'),
    state                 text NOT NULL DEFAULT 'pending' CHECK (state IN
                              ('pending', 'available', 'deleting', 'deleted')),
    retention_until       timestamptz,
    deletion_reason       text,
    deletion_error        text,
    created_at            timestamptz NOT NULL DEFAULT now(),
    available_at          timestamptz,
    deletion_started_at   timestamptz,
    deleted_at            timestamptz,
    CHECK ((state = 'pending' AND size_bytes IS NULL AND digest IS NULL AND available_at IS NULL)
        OR (state IN ('available', 'deleting', 'deleted') AND size_bytes IS NOT NULL
            AND digest IS NOT NULL AND available_at IS NOT NULL)),
    CHECK ((state = 'deleted' AND deleted_at IS NOT NULL)
        OR (state <> 'deleted' AND deleted_at IS NULL)),
    CHECK ((state IN ('deleting', 'deleted') AND deletion_started_at IS NOT NULL)
        OR (state IN ('pending', 'available') AND deletion_started_at IS NULL))
);

CREATE INDEX idx_artifacts_state_created ON artifact.artifacts (state, created_at);
CREATE INDEX idx_artifacts_retention ON artifact.artifacts (retention_until)
    WHERE state = 'available' AND retention_until IS NOT NULL;
CREATE INDEX idx_artifacts_source ON artifact.artifacts (source_type, source_ref)
    WHERE source_ref IS NOT NULL;

-- Enforce immutable identity/source/retention/storage metadata in Postgres,
-- not only in service code. Lifecycle transitions may fill integrity metadata
-- once and update deletion diagnostics; no transition can return to available.
CREATE FUNCTION artifact.enforce_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.id <> OLD.id
       OR NEW.storage_key <> OLD.storage_key
       OR NEW.source_type <> OLD.source_type
       OR NEW.source_ref IS DISTINCT FROM OLD.source_ref
       OR NEW.created_by_identity_id <> OLD.created_by_identity_id
       OR NEW.mime_type <> OLD.mime_type
       OR NEW.retention_until IS DISTINCT FROM OLD.retention_until
       OR NEW.created_at <> OLD.created_at THEN
        RAISE EXCEPTION 'artifact immutable metadata cannot be changed';
    END IF;
    IF OLD.state = 'pending' AND NEW.state <> 'available' THEN
        RAISE EXCEPTION 'pending artifact may transition only to available';
    ELSIF OLD.state = 'available' AND NEW.state <> 'deleting' THEN
        RAISE EXCEPTION 'available artifact may transition only to deleting';
    ELSIF OLD.state = 'deleting' AND NEW.state NOT IN ('deleting', 'deleted') THEN
        RAISE EXCEPTION 'deleting artifact may remain deleting or transition to deleted';
    ELSIF OLD.state = 'deleted' THEN
        RAISE EXCEPTION 'deleted artifact tombstone is immutable';
    END IF;
    IF OLD.state <> 'pending' AND
       (NEW.size_bytes IS DISTINCT FROM OLD.size_bytes OR NEW.digest IS DISTINCT FROM OLD.digest
        OR NEW.available_at IS DISTINCT FROM OLD.available_at) THEN
        RAISE EXCEPTION 'artifact integrity metadata cannot be changed after availability';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER artifact_immutable BEFORE UPDATE ON artifact.artifacts
    FOR EACH ROW EXECUTE FUNCTION artifact.enforce_immutable();

-- HOR-254 deliberately used opaque text until HOR-399 owned the identity. The
-- product path has not shipped, so migrate it to the durable UUID and enforce
-- referential integrity now rather than retain a compatibility representation.
ALTER TABLE work.artifact_links
    ALTER COLUMN artifact_id TYPE uuid USING artifact_id::uuid;
ALTER TABLE work.artifact_links
    ADD CONSTRAINT artifact_links_artifact_fk
    FOREIGN KEY (artifact_id) REFERENCES artifact.artifacts(id) ON DELETE RESTRICT;

-- Runner artifact authorization must be based on the exact input references
-- committed with the invocation, never on a runner-supplied artifact id.
ALTER TABLE toolgateway.invocations
    ADD COLUMN artifact_input_refs jsonb NOT NULL DEFAULT '[]'::jsonb;
