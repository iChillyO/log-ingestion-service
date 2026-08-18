-- 004_logs_rollup.sql
-- Minute-level pre-aggregation for GET /logs/aggregate.
--
-- Why this exists:
--   The primary aggregation query is date_bin(timestamp) + GROUP BY service/level
--   over a time window. Scanning the raw `logs` heap under concurrent ingest
--   saturates the single Postgres vCPU (mixed-load p95 was ~1.5s at 1M rows).
--   A rollup of (minute, service, level) -> count is tiny — a 1-hour window
--   with 6 services and 4 levels is a few thousand rows — so the same query
--   becomes an index range scan + SUM.
--
-- Consistency:
--   insertMany upserts this table in the same statement as the raw INSERT
--   (CTE + ON CONFLICT). Aggregates that only filter on time/service/level
--   therefore match raw counts with no background lag.
--   Queries that use `q` or `attr.*` still scan `logs` because those
--   dimensions are not in the rollup.
--
-- Alignment:
--   bucket_start is date_bin(1 minute, timestamp, epoch), i.e. UTC-minute
--   floors. Larger request buckets (5m/1h/1d) are derived by date_bin'ing
--   these minute rows. Unaligned since/until edges (partial minutes) are
--   filled from `logs` so counts stay exact.

CREATE TABLE IF NOT EXISTS logs_rollup (
  bucket_start TIMESTAMPTZ NOT NULL,
  service      TEXT        NOT NULL,
  level        log_level   NOT NULL,
  cnt          BIGINT      NOT NULL CHECK (cnt >= 0),
  PRIMARY KEY (bucket_start, service, level)
);

-- Backfill existing rows so a running database stays consistent after migrate.
INSERT INTO logs_rollup (bucket_start, service, level, cnt)
SELECT date_bin(INTERVAL '1 minute', "timestamp", TIMESTAMPTZ 'epoch'),
       service,
       level,
       COUNT(*)
FROM logs
GROUP BY 1, 2, 3
ON CONFLICT (bucket_start, service, level)
DO UPDATE SET cnt = EXCLUDED.cnt;
