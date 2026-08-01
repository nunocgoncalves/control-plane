-- HOR-245 / ARCH-009: remove the per-sandbox egress proxy data-scope plane.
--
-- ARCH-009 ("Immediate removal of the AgentSandbox egress proxy") retires the
-- EgressRoute CRD/schema/reconciler/resolver, the proxy image/sidecar, the
-- per-sandbox credential mounts, and the harness egressProxyUrl/placeholder-key
-- dependency. Customer-system actions move to the tool gateway (ARCH-008) and
-- private-inference auth to supervisor mTLS (ARCH-010). HOR-244 remains
-- completed history but is architecturally superseded; nothing references the
-- `egress` schema at this point (the agent path is not yet a production-stable
-- integration, so the table carries no live data).
--
-- This migration drops the `egress` schema created by 000010_egress. The down
-- migration recreates the v1 HOR-244 shape for reversibility. The code that
-- populated/consumed these objects is removed in the same ticket.

DROP VIEW IF EXISTS egress.effective_routes;
DROP TRIGGER IF EXISTS credentials_updated ON egress.credentials;
DROP FUNCTION IF EXISTS egress.set_updated_at();
DROP TABLE IF EXISTS egress.credentials;
DROP SCHEMA IF EXISTS egress;
