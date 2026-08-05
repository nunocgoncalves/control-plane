-- HOR-396: immutable customer-safe source/workflow/node presentation for the
-- customer Dashboard. Internal trigger context remains private.
ALTER TABLE work.work_items
    ADD COLUMN source_presentation jsonb NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE work.attempts
    ADD COLUMN presentation_snapshot jsonb NOT NULL DEFAULT '{}'::jsonb;

UPDATE work.attempts a
SET presentation_snapshot = d.presentation
FROM workflow.definitions d
WHERE d.id = a.definition_id;

ALTER TABLE runtime.node_executions
    ADD COLUMN business_label jsonb NOT NULL DEFAULT '{}'::jsonb;

-- Existing pre-release graph snapshots did not require labels. Keep their
-- projection empty rather than inventing customer copy; newly created visits
-- always snapshot the validated localized label.
