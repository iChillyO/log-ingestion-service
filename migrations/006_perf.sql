-- 006_perf.sql
-- Performance work driven by three measured problems.
--
-- 1. EVERY BACKFILLED ROW WAS LANDING IN `logs_default`.
--    `ensure_log_partition` was only ever called for [yesterday .. +2 days],
--    so a dataset that spans a month had ~97% of its rows in the DEFAULT
--    partition. That is bad twice over:
--      * queries lose partition pruning and scan one giant heap;
--      * `CREATE TABLE ... PARTITION OF` must take ACCESS EXCLUSIVE on the
--        parent AND full-scan the default partition to prove no row conflicts.
--        With ~1M rows sitting in `logs_default`, every retention sweep stalled
--        ingestion for seconds.
--    Fix: `ensure_log_partitions(from, to)` materialises the whole retention
--    window in one round trip, and the service does that before reporting
--    healthy, while `logs_default` is still empty (so the scan is free).
--
-- 2. `ingested_at` was written on every row and read by nothing.
--    8 bytes/row plus a now() call per row inside COPY. Dropped.
--
-- 3. A month-wide aggregate had to SUM the whole minute rollup.
--    30 days x 1440 minutes x (services x levels) is ~1M rollup rows - the
--    rollup was as big as the raw table, so `bucket=1h` over a month was no
--    cheaper than scanning `logs`. Fix: a second, hourly tier.
--    30 days x 720 hours x 24 combos is ~17k rows.

-- ---------------------------------------------------------------------------
-- 1. Bulk partition management
-- ---------------------------------------------------------------------------

-- Partition bounds are anchored to UTC midnight explicitly. Relying on the
-- session TimeZone here would silently shift every bound if PGTZ ever changed.
CREATE OR REPLACE FUNCTION ensure_log_partitions(from_date DATE, to_date DATE)
RETURNS INT
LANGUAGE plpgsql
AS $$
DECLARE
  d         DATE := from_date;
  part_name TEXT;
  created   INT  := 0;
BEGIN
  -- Serialise against concurrent sweeps so two callers cannot race on the
  -- same CREATE TABLE and fail with duplicate_table.
  PERFORM pg_advisory_xact_lock(hashtext('logs_partition_maint'));

  WHILE d <= to_date LOOP
    part_name := format('logs_p_%s', to_char(d, 'YYYYMMDD'));
    IF NOT EXISTS (
      SELECT 1
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE c.relname = part_name
        AND n.nspname = current_schema()
    ) THEN
      EXECUTE format(
        'CREATE TABLE %I PARTITION OF logs FOR VALUES FROM (%L) TO (%L)',
        part_name,
        (d::timestamp AT TIME ZONE 'UTC'),
        ((d + 1)::timestamp AT TIME ZONE 'UTC')
      );
      created := created + 1;
    END IF;
    d := d + 1;
  END LOOP;

  RETURN created;
END;
$$;

-- Keep the single-day helper as a thin wrapper so existing callers/tests work.
CREATE OR REPLACE FUNCTION ensure_log_partition(day_start DATE) RETURNS TEXT
LANGUAGE plpgsql
AS $$
BEGIN
  PERFORM ensure_log_partitions(day_start, day_start);
  RETURN format('logs_p_%s', to_char(day_start, 'YYYYMMDD'));
END;
$$;

-- ---------------------------------------------------------------------------
-- 2. Drop the write-only column
-- ---------------------------------------------------------------------------

ALTER TABLE logs DROP COLUMN IF EXISTS ingested_at;

-- ---------------------------------------------------------------------------
-- 3. Hourly rollup tier
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS logs_rollup_hour (
  bucket_start TIMESTAMPTZ NOT NULL,
  service      TEXT        NOT NULL,
  level        log_level   NOT NULL,
  cnt          BIGINT      NOT NULL CHECK (cnt >= 0),
  PRIMARY KEY (bucket_start, service, level)
);

CREATE INDEX IF NOT EXISTS idx_rollup_hour_svc_bucket
  ON logs_rollup_hour (service, bucket_start);

CREATE INDEX IF NOT EXISTS idx_rollup_hour_lvl_bucket
  ON logs_rollup_hour (level, bucket_start);

-- Backfill from the minute tier (cheaper than re-scanning `logs`, and the two
-- tiers stay consistent by construction because hours nest inside minutes).
INSERT INTO logs_rollup_hour (bucket_start, service, level, cnt)
SELECT date_bin(INTERVAL '1 hour', bucket_start, TIMESTAMPTZ 'epoch'),
       service,
       level,
       SUM(cnt)
FROM logs_rollup
GROUP BY 1, 2, 3
ON CONFLICT (bucket_start, service, level)
DO UPDATE SET cnt = EXCLUDED.cnt;

-- ---------------------------------------------------------------------------
-- 4. Rollup storage tuning
-- ---------------------------------------------------------------------------
-- The rollup tables are the opposite workload to `logs`: a small, bounded set
-- of rows updated over and over. `cnt` is not indexed, so every upsert can be
-- a HOT update - but only if the page has free space. Leaving 30% headroom
-- keeps updates in-page, which means no index maintenance and no new heap
-- pages per flush. The hourly tier is hit ~60x more often per row, so it gets
-- more headroom still.
ALTER TABLE logs_rollup
  SET (fillfactor = 70, autovacuum_vacuum_scale_factor = 0.05);
ALTER TABLE logs_rollup_hour
  SET (fillfactor = 60, autovacuum_vacuum_scale_factor = 0.05);

-- ---------------------------------------------------------------------------
-- 5. Planner statistics
-- ---------------------------------------------------------------------------
-- `service` is the highest-cardinality filter column, and its estimate decides
-- whether the planner walks idx_logs_svc_ts_id in order (cheap under LIMIT) or
-- builds a bitmap and sorts it (expensive). Raising the target on the parent
-- improves the partitioned-table-level statistics that drive that choice.
ALTER TABLE logs ALTER COLUMN service SET STATISTICS 500;

-- A larger sequence cache means fewer nextval() round trips inside COPY. Ids
-- only need to be unique and roughly increasing, so burning a block on restart
-- is fine.
ALTER SEQUENCE logs_id_seq CACHE 10000;
