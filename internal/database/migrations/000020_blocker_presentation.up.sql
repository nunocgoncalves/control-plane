-- HOR-396: snapshot localized labels for customer-visible blocker outcomes,
-- response fields, and enum options alongside the immutable blocker contract.
ALTER TABLE work.blockers
    ADD COLUMN response_presentation jsonb NOT NULL DEFAULT '{"outcomes":[]}'::jsonb;
