-- HOR-433: grant the `gateway` read-only DB role access to the schemas/tables
-- the inference-gateway workload mTLS auth path reads.
--
-- HOR-334 (000008) granted `gateway` USAGE on `identity, permissions, catalog`
-- only. The workload-store (inference-gateway internal/workload) additionally
-- reads durable worker/pool/turn state for the HOR-398 supervisor workload
-- listener (ARCH-004/010) from the `toolgateway` (HOR-392, 000011) and
-- `runtime` (000009) schemas — neither of which was ever granted to the
-- `gateway` role. On OPO1 every workload model call therefore failed with
--
--   permission denied for schema toolgateway (SQLSTATE 42501)
--
-- surfaced as an infrastructure 503 from resolve-pool, aborting the attempt.
--
-- Least privilege: USAGE on the two schemas + SELECT only on the exact tables
-- internal/workload reads, not broad DEFAULT PRIVILEGES. Mirrors 000008's
-- conditional pattern (grant only if the `gateway` role exists, so `migrate up`
-- never fails where the role is absent in BYO/test setups).

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'gateway') THEN
        GRANT USAGE ON SCHEMA toolgateway, runtime TO gateway;
        GRANT SELECT ON
            toolgateway.pools,
            runtime.turns,
            runtime.run_pool_assignments,
            runtime.workflow_runs,
            runtime.turn_assignments
            TO gateway;
    END IF;
END $$;
