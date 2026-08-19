-- 001_init.sql
-- Base schema for the log ingestion service.
--
-- Design notes:
--  * Table is RANGE-partitioned by day on `timestamp`, so retention becomes an
--    O(1) partition DROP rather than a heavy DELETE.
--  * `level` uses a small ENUM (~4 bytes) instead of TEXT to save on row
--    width across millions of rows.
--  * `attributes` is JSONB where every value is coerced to a text string at
--    ingest time. This keeps `attr.<key>=value` semantics ("compared as
--    strings") trivially correct and lets a single GIN(jsonb_path_ops) index
--    answer any attribute-equality filter.
--  * `id` comes from a shared sequence at the partitioned-table level and is
--    unique across partitions, so `(timestamp DESC, id DESC)` is a stable,
--    deterministic sort key.

CREATE EXTENSION IF NOT EXISTS pg_trgm;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'log_level') THEN
    CREATE TYPE log_level AS ENUM ('debug', 'info', 'warn', 'error');
  END IF;
END$$;

CREATE TABLE IF NOT EXISTS logs (
  id           BIGINT      NOT NULL,
  "timestamp"  TIMESTAMPTZ NOT NULL,
  ingested_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  level        log_level   NOT NULL,
  service      TEXT        NOT NULL,
  message      TEXT        NOT NULL,
  attributes   JSONB       NOT NULL DEFAULT '{}'::jsonb
) PARTITION BY RANGE ("timestamp");

CREATE SEQUENCE IF NOT EXISTS logs_id_seq AS BIGINT START WITH 1 CACHE 1000;

-- Make id auto-populate from the shared sequence unless the caller supplies one.
ALTER TABLE logs
  ALTER COLUMN id SET DEFAULT nextval('logs_id_seq');

-- Safety valve: a default partition catches any log whose timestamp does not
-- fall inside an existing daily partition (e.g. someone ingests a timestamp far
-- in the past). The retention worker keeps this small.
CREATE TABLE IF NOT EXISTS logs_default PARTITION OF logs DEFAULT;
