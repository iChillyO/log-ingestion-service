-- 002_indexes.sql
-- Indexes on the partitioned parent get propagated to every child partition
-- automatically (including future ones created by the retention worker).
--
-- Why these indexes:
--   * idx_logs_ts_id     : default cursor pagination, time-range only queries,
--                          and the primary ordering `(timestamp DESC, id DESC)`.
--   * idx_logs_svc_ts_id : service filter is common; index-only skip past the
--                          rows we cannot use even after time-range pruning.
--   * idx_logs_lvl_ts_id : same reasoning for level. Only 4 distinct values, but
--                          highly selective for level='error' style queries.
--   * idx_logs_attrs_gin : attribute equality via `attributes @> $1::jsonb`,
--                          using jsonb_path_ops which is smaller and faster
--                          than the default GIN opclass for containment.
--   * idx_logs_msg_trgm  : substring search for `q=`. Trigram GIN lets
--                          `message ILIKE '%foo%'` use an index rather than a
--                          full scan of every partition in range.

CREATE INDEX IF NOT EXISTS idx_logs_ts_id
  ON logs ("timestamp" DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_logs_svc_ts_id
  ON logs (service, "timestamp" DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_logs_lvl_ts_id
  ON logs (level, "timestamp" DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_logs_attrs_gin
  ON logs USING GIN (attributes jsonb_path_ops);

CREATE INDEX IF NOT EXISTS idx_logs_msg_trgm
  ON logs USING GIN (message gin_trgm_ops);
