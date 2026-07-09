-- HOR-326: reverse per-identity rate limits.

DROP VIEW IF EXISTS permissions.effective_rate_limits;
ALTER TABLE permissions.policies DROP COLUMN IF EXISTS rate_limits;
