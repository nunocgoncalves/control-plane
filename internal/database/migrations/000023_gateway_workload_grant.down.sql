-- HOR-433: reverse the gateway workload grants. DEFAULT PRIVILEGES are not
-- used here (none were set), matching the 000008 down precedent of leaving
-- them untouched.

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'gateway') THEN
        REVOKE SELECT ON
            toolgateway.pools,
            runtime.turns,
            runtime.run_pool_assignments,
            runtime.workflow_runs,
            runtime.turn_assignments
            FROM gateway;
        REVOKE USAGE ON SCHEMA toolgateway, runtime FROM gateway;
    END IF;
END $$;
