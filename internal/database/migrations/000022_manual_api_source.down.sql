-- HOR-425 down: revert the manual_api Workflow source contract to the pre-HOR-425
-- source_type value set (graph_email, operator_artifact).

ALTER TABLE workflow.definitions
    DROP CONSTRAINT definitions_source_type_check,
    ADD CONSTRAINT definitions_source_type_check
        CHECK (source_type IN ('graph_email', 'operator_artifact'));

ALTER TABLE workflow.trigger_bindings
    DROP CONSTRAINT trigger_bindings_source_type_check,
    ADD CONSTRAINT trigger_bindings_source_type_check
        CHECK (source_type IN ('graph_email', 'operator_artifact'));
