-- HOR-326: per-identity rate limits on PermissionPolicy.
--
-- rate_limits is a nullable jsonb column on permissions.policies: {rpm, tpm}.
-- The effective_rate_limits view resolves subject -> identity (by subject_key =
-- identity.key) and exposes identity_id -> (rpm, tpm), collapsing multiple
-- policies on one identity to the most restrictive (min). Absent row = unlimited.
-- Consumers (gateway HOR-247) read the view directly + LISTEN on
-- permissions_changed (reused; already fires on permissions.policies).

ALTER TABLE permissions.policies ADD COLUMN rate_limits jsonb;

CREATE VIEW permissions.effective_rate_limits AS
    SELECT i.id   AS identity_id,
           MIN((p.rate_limits->>'rpm')::int) AS rpm,
           MIN((p.rate_limits->>'tpm')::int) AS tpm
    FROM permissions.policies p
    JOIN identity.identities i ON i.key = p.subject_key
    WHERE p.deleted_at IS NULL
      AND i.deleted_at IS NULL
      AND p.rate_limits IS NOT NULL
    GROUP BY i.id;
