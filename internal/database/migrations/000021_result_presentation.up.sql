-- Immutable localized outcome/result presentation for customer-safe receipts.
ALTER TABLE runtime.node_executions
    ADD COLUMN result_presentation jsonb NOT NULL DEFAULT '{}'::jsonb;
