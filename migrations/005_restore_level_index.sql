-- 005_restore_level_index.sql
-- Restore the level index that was dropped in a previous version.
-- Also add secondary indexes on the rollup table for filtered aggregates.

-- Level index for level= filter queries.
CREATE INDEX IF NOT EXISTS idx_logs_lvl_ts_id
  ON logs (level, "timestamp" DESC, id DESC);

-- Rollup indexes for filtered aggregate queries.
CREATE INDEX IF NOT EXISTS idx_rollup_svc_bucket
  ON logs_rollup (service, bucket_start);

CREATE INDEX IF NOT EXISTS idx_rollup_lvl_bucket
  ON logs_rollup (level, bucket_start);
