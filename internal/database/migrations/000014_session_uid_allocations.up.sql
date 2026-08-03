-- HOR-249 / HOR-245: durable collision-free, non-recycling session UID allocator.
--
-- The founder-approved HOR-245 reuse-safety contract makes stable, non-recycled
-- session (uid, gid) identity part of the v1 safety floor: a recycled UID whose
-- prior sandbox still exists (unreaped) lets a new child running that UID read
-- the prior session's 0700 sandbox directory — defeating filesystem isolation.
-- The prior hash-into-9000 scheme allowed two LIVE sessions to collide on the
-- same UID; this table replaces it with a durable allocator that:
--
--   * assigns a unique UID per session (gid = uid), idempotent per session;
--   * never recycles a UID while its sandbox may still exist: a UID is reusable
--     only after it has been released (SessionEnd reaped) AND a bounded grace
--     exceeding max reap latency has elapsed;
--   * fails closed (no dispatch) on exhaustion rather than silently sharing.
--
-- Owner: HOR-249 dispatch (the session-lifecycle owner). The supervisor's
-- fail-closed reap (HOR-245) + this non-recycling grace is the v1 safety floor.
-- A durable reap-ack + retry/fencing contract remains an explicit v1 non-goal.

CREATE TABLE runtime.session_uid_allocations (
    uid          integer PRIMARY KEY,                       -- the allocated UID (== GID), in [base, base+range)
    session_id   text NOT NULL UNIQUE,                      -- one allocation per session (idempotent)
    state        text NOT NULL DEFAULT 'in_use' CHECK (state IN ('in_use', 'freed')),
    allocated_at timestamptz NOT NULL DEFAULT now(),
    freed_at     timestamptz,                               -- set when released; recyclable after freed_at + grace
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now()
);

-- Recycle candidate lookup: freed UIDs whose grace has elapsed.
CREATE INDEX idx_session_uid_alloc_recycle
    ON runtime.session_uid_allocations (state, freed_at)
    WHERE state = 'freed';

-- runtime.set_updated_at() is defined in migration 000009; reuse it.
CREATE TRIGGER session_uid_allocations_updated BEFORE UPDATE ON runtime.session_uid_allocations
    FOR EACH ROW EXECUTE FUNCTION runtime.set_updated_at();
