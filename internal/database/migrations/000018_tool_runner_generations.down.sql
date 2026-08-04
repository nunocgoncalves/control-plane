DROP INDEX IF EXISTS toolgateway.idx_approved_runners_managed;
DROP INDEX IF EXISTS toolgateway.idx_runner_reg_available;

DROP VIEW toolgateway.available_tool_versions;
CREATE VIEW toolgateway.available_tool_versions AS
    SELECT DISTINCT tv.*
    FROM toolgateway.tool_versions tv
    JOIN toolgateway.runner_registrations rr
      ON rr.tool_name = tv.name AND rr.tool_digest = tv.digest
    WHERE rr.active
      AND rr.last_heartbeat_at > now() - interval '30 seconds'
      AND (tv.effect_class = 'read_only' OR tv.consequence_summary_template <> '{}'::jsonb);

CREATE INDEX idx_runner_reg_available
    ON toolgateway.runner_registrations (tool_name, tool_digest)
    WHERE active;

ALTER TABLE toolgateway.approved_runners DROP COLUMN managed_by;
ALTER TABLE toolgateway.runner_registrations DROP COLUMN accepting_new;
