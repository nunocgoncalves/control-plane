-- HOR-241: initial schema scaffold.
--
-- Creates the four namespace reservations named in the ticket
-- (identity, permissions, usage, ai_data) plus the pgvector extension.
-- No business tables live here: identity -> HOR-242, permissions -> HOR-243,
-- durable turns/events -> HOR-246 (its own schema). usage + ai_data content
-- is post-v1 (S2/S8/S9); the namespaces are reserved up front.

CREATE SCHEMA IF NOT EXISTS identity;
CREATE SCHEMA IF NOT EXISTS permissions;
CREATE SCHEMA IF NOT EXISTS usage;
CREATE SCHEMA IF NOT EXISTS ai_data;

-- pgvector: used by memory/KB and tool-registry semantic search (post-v1
-- consumers). Requires a pgvector-enabled Postgres image (forge bundled
-- Postgres must ship it; tests use pgvector/pgvector).
CREATE EXTENSION IF NOT EXISTS vector;
