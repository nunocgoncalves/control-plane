-- HOR-241: roll back the initial schema scaffold.

DROP EXTENSION IF EXISTS vector;
DROP SCHEMA IF EXISTS ai_data;
DROP SCHEMA IF EXISTS usage;
DROP SCHEMA IF EXISTS permissions;
DROP SCHEMA IF EXISTS identity;
