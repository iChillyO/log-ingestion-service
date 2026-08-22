-- 007_covering_indexes.sql
-- Replace non-covering rollup indexes with covering ones that INCLUDE cnt and
-- the grouping column, enabling index-only scans for filtered aggregate queries.
--
-- Before: idx_rollup_svc_bucket (service, bucket_start) required a heap visit
--         for every matching row to read `cnt` and `level`.
-- After:  The index includes all columns the aggregate query needs, so
--         Postgres reads only the index — no heap access.

-- Minute tier
DROP INDEX IF EXISTS idx_rollup_svc_bucket;
CREATE INDEX IF NOT EXISTS idx_rollup_svc_bucket_cov
  ON logs_rollup (service, bucket_start) INCLUDE (level, cnt);

DROP INDEX IF EXISTS idx_rollup_lvl_bucket;
CREATE INDEX IF NOT EXISTS idx_rollup_lvl_bucket_cov
  ON logs_rollup (level, bucket_start) INCLUDE (service, cnt);

-- Hourly tier
DROP INDEX IF EXISTS idx_rollup_hour_svc_bucket;
CREATE INDEX IF NOT EXISTS idx_rollup_hour_svc_bucket_cov
  ON logs_rollup_hour (service, bucket_start) INCLUDE (level, cnt);

DROP INDEX IF EXISTS idx_rollup_hour_lvl_bucket;
CREATE INDEX IF NOT EXISTS idx_rollup_hour_lvl_bucket_cov
  ON logs_rollup_hour (level, bucket_start) INCLUDE (service, cnt);
