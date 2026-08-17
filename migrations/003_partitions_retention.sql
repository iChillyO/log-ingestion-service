-- 003_partitions_retention.sql
-- Helper functions used by the application's retention worker.
--
-- ensure_log_partition(day_start)
--   Creates the daily partition covering [day_start, day_start + 1 day) if it
--   does not already exist. Idempotent and safe to call concurrently thanks to
--   the advisory lock.
--
-- drop_log_partitions_before(cutoff)
--   Drops every daily partition whose upper bound is strictly less than or
--   equal to cutoff. This is the fast retention path: dropping a partition is
--   metadata-only and takes no ACCESS EXCLUSIVE lock on the parent table for
--   any noticeable amount of time.

CREATE OR REPLACE FUNCTION ensure_log_partition(day_start DATE) RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
  part_name TEXT := format('logs_p_%s', to_char(day_start, 'YYYYMMDD'));
  from_ts   TIMESTAMPTZ := day_start::TIMESTAMPTZ;
  to_ts     TIMESTAMPTZ := (day_start + INTERVAL '1 day')::TIMESTAMPTZ;
BEGIN
  -- Serialise partition creation to avoid duplicate-name races between the
  -- retention worker sweeps and any ad-hoc invocations.
  PERFORM pg_advisory_xact_lock(hashtext('logs_partition_maint'));

  IF NOT EXISTS (
    SELECT 1 FROM pg_class WHERE relname = part_name
  ) THEN
    EXECUTE format(
      'CREATE TABLE %I PARTITION OF logs FOR VALUES FROM (%L) TO (%L)',
      part_name, from_ts, to_ts
    );
  END IF;

  RETURN part_name;
END;
$$;

CREATE OR REPLACE FUNCTION drop_log_partitions_before(cutoff TIMESTAMPTZ)
RETURNS SETOF TEXT
LANGUAGE plpgsql
AS $$
DECLARE
  rec RECORD;
BEGIN
  PERFORM pg_advisory_xact_lock(hashtext('logs_partition_maint'));

  FOR rec IN
    SELECT c.oid::regclass::text AS part_name,
           pg_get_expr(c.relpartbound, c.oid) AS bound
    FROM pg_inherits i
    JOIN pg_class c ON c.oid = i.inhrelid
    WHERE i.inhparent = 'logs'::regclass
      AND c.relname <> 'logs_default'
  LOOP
    -- bound looks like: FOR VALUES FROM ('2026-07-20 00:00:00+00') TO ('2026-07-21 00:00:00+00')
    -- We use a regex to pull out the TO bound.
    DECLARE
      to_txt TEXT;
      to_ts  TIMESTAMPTZ;
    BEGIN
      to_txt := substring(rec.bound FROM 'TO \(''([^'']+)''\)');
      IF to_txt IS NULL THEN
        CONTINUE;
      END IF;
      to_ts := to_txt::TIMESTAMPTZ;
      IF to_ts <= cutoff THEN
        EXECUTE format('DROP TABLE IF EXISTS %s', rec.part_name);
        RETURN NEXT rec.part_name;
      END IF;
    END;
  END LOOP;
END;
$$;
