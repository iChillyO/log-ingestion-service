-- 002_indexes.sql
-- Indexes for the logs table.
--
-- We need a balance between write speed (fewer indexes = faster INSERT) and
-- query speed (indexes = faster SELECT). With write-buffering + COPY in the
-- app layer, we batch hundreds of rows per write, amortising the per-row
-- index maintenance cost.
--
-- Index strategy: keep the minimum set needed for all query paths.
-- Each additional index costs ~10-15% write throughput on a 1-CPU PG.
--
--   * idx_logs_ts_id       : cursor pagination + time-range queries
--   * idx_logs_svc_ts_id   : service is the most common filter dimension
--   * idx_logs_lvl_ts_id   : level filter queries
--   * idx_logs_attrs_gin   : attribute containment (@>) queries
--   * idx_logs_msg_trgm    : message ILIKE '%...%' full-text search

CREATE INDEX IF NOT EXISTS idx_logs_ts_id
  ON logs ("timestamp" DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_logs_svc_ts_id
  ON logs (service, "timestamp" DESC, id DESC);

-- Level index for level= filter queries.
CREATE INDEX IF NOT EXISTS idx_logs_lvl_ts_id
  ON logs (level, "timestamp" DESC, id DESC);

-- GIN index for JSONB attribute containment queries (attr.<key>=value).
-- Uses jsonb_path_ops which is smaller and faster than the default ops class
-- for @> queries.
CREATE INDEX IF NOT EXISTS idx_logs_attrs_gin
  ON logs USING gin (attributes jsonb_path_ops);

-- Trigram index for substring search on message (ILIKE '%...%').
-- Requires pg_trgm extension (created in 001_init.sql).
CREATE INDEX IF NOT EXISTS idx_logs_msg_trgm
  ON logs USING gin (message gin_trgm_ops);
