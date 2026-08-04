-- HOR-397: coordinated immutable tool-runner generation draining.

ALTER TABLE toolgateway.runner_registrations
    ADD COLUMN accepting_new boolean NOT NULL DEFAULT true,
    ADD COLUMN retired_at timestamptz;

ALTER TABLE toolgateway.approved_runners
    ADD COLUMN managed_by text NOT NULL DEFAULT 'operator'
        CHECK (managed_by IN ('operator', 'static_config'));

DROP VIEW toolgateway.available_tool_versions;
CREATE VIEW toolgateway.available_tool_versions AS
    SELECT DISTINCT tv.*
    FROM toolgateway.tool_versions tv
    JOIN toolgateway.runner_registrations rr
      ON rr.tool_name = tv.name AND rr.tool_digest = tv.digest
    WHERE rr.active
      AND rr.accepting_new
      AND rr.last_heartbeat_at > now() - interval '30 seconds'
      AND (tv.effect_class = 'read_only' OR tv.consequence_summary_template <> '{}'::jsonb);

DROP INDEX toolgateway.idx_runner_reg_available;
CREATE INDEX idx_runner_reg_available
    ON toolgateway.runner_registrations (tool_name, tool_digest)
    WHERE active AND accepting_new;

CREATE INDEX idx_approved_runners_managed
    ON toolgateway.approved_runners (managed_by)
    WHERE deleted_at IS NULL;
