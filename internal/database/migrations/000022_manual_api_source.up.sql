-- HOR-425: add the authenticated manual_api Workflow source (ARCH-026).
--
-- manual_api is a supported installed Workflow source backed by the existing
-- authenticated POST /v1/work-items endpoint. It is API-only (no trigger
-- binding, public route, upload form, or customer-facing source configuration),
-- so workflow.trigger_bindings rows never carry a manual_api source; both
-- check constraints are widened only so a manual_api definition can be
-- registered. graph_email / operator_artifact remain valid enum values but
-- their ingress adapters are not installed until HOR-356 / HOR-393.

ALTER TABLE workflow.definitions
    DROP CONSTRAINT definitions_source_type_check,
    ADD CONSTRAINT definitions_source_type_check
        CHECK (source_type IN ('graph_email', 'operator_artifact', 'manual_api'));

ALTER TABLE workflow.trigger_bindings
    DROP CONSTRAINT trigger_bindings_source_type_check,
    ADD CONSTRAINT trigger_bindings_source_type_check
        CHECK (source_type IN ('graph_email', 'operator_artifact', 'manual_api'));
