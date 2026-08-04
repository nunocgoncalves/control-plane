-- HOR-399 rollback. Bytes in MinIO are intentionally not removed by a schema
-- rollback; operators must explicitly apply the documented deletion procedure.

ALTER TABLE toolgateway.invocations DROP COLUMN artifact_input_refs;

ALTER TABLE work.artifact_links DROP CONSTRAINT artifact_links_artifact_fk;
ALTER TABLE work.artifact_links
    ALTER COLUMN artifact_id TYPE text USING artifact_id::text;

DROP SCHEMA artifact CASCADE;
