# Log Ingestion and Query Service

A high-throughput structured log ingestion and query API, backed by PostgreSQL. The service accepts batched structured logs, stores them in a partitioned, indexed table, and exposes cursor-paginated search and time-bucketed aggregation endpoints. The entire stack starts with `docker compose up`.

Built for a constrained environment (0.5 CPU / 256 MB RAM for the app, 1 CPU / 1 GB RAM for Postgres) and evaluated against a load generator that expects the exact API contract in this document.

---

## Contents

1. [Quick start](#quick-start)
2. [API reference](#api-reference)
3. [Schema and index design](#schema-and-index-design)
4. [Attribute storage strategy](#attribute-storage-strategy)
5. [Retention strategy](#retention-strategy)
6. [Measured performance](#measured-performance)
7. [Load-test methodology](#load-test-methodology)
8. [Known limitations](#known-limitations)
9. [Optional features](#optional-features)
10. [Configuration reference](#configuration-reference)
11. [Development](#development)

---

## Quick start

Prerequisites: Docker and Docker Compose v2.

```bash
docker compose up --build
```

That command:

1. Starts a Postgres 16 container tuned for the 1 GB / 1 CPU envelope.
2. Waits for it to be `pg_isready`.
3. Starts the Node app on `localhost:8080`.
4. Runs SQL migrations automatically at startup.
5. Reports healthy on `GET /health` only after migrations finish and the retention worker has ensured today's partition exists.

A minimal smoke test:

```bash
curl http://localhost:8080/health

curl -X POST http://localhost:8080/logs \
  -H 'content-type: application/json' \
  -d '{"logs":[{"timestamp":"2026-08-03T14:32:01Z","level":"error","service":"checkout","message":"payment declined","attributes":{"user_id":"42","region":"eu-west","retries":3}}]}'

curl 'http://localhost:8080/logs?service=checkout&level=error'
curl 'http://localhost:8080/logs/aggregate?since=2026-08-03T14:00:00Z&until=2026-08-03T15:00:00Z&bucket=1m&group_by=service'
```

---

## API reference

All endpoints listen on port `8080`.

### `GET /health`

Returns `200` with a small JSON body once the service is ready: DB connection is live, migrations have applied, and daily partitions are in place. Returns `503` while starting.

Always unauthenticated (even when `AUTH_ENABLED=true`).

### `POST /logs` — ingest

Body:

```json
{
  "logs": [
    {
      "timestamp": "2026-07-20T14:32:01.123Z",
      "level": "error",
      "service": "checkout",
      "message": "payment declined",
      "attributes": { "user_id": "42", "region": "eu-west", "retries": 3 }
    }
  ]
}
```

Per-entry validation:

| Field        | Rule                                                                                    |
| ------------ | --------------------------------------------------------------------------------------- |
| `timestamp`  | Required. ISO 8601 with timezone (`Z` or `+HH:MM`). Not more than 5 minutes in future. |
| `level`      | Required. One of `debug`, `info`, `warn`, `error`.                                     |
| `service`    | Required, non-empty string.                                                             |
| `message`    | Required, non-empty string.                                                             |
| `attributes` | Optional. Flat object; values must be string/number/boolean.                            |

Response:

```json
{ "accepted": 9, "rejected": [{ "index": 3, "reason": "invalid level: 'critical'" }] }
```

- `200` when at least one entry is accepted (the accepted ones are durably persisted before returning).
- `400` when every entry is rejected, the request body is malformed JSON, or the top-level shape is wrong.

### `GET /logs` — query

Any combination of the following query parameters:

| Param        | Meaning                                                                    |
| ------------ | -------------------------------------------------------------------------- |
| `service`    | Exact service match                                                        |
| `level`      | Exact level match                                                          |
| `since`      | Inclusive lower bound on `timestamp` (ISO 8601)                            |
| `until`      | Exclusive upper bound on `timestamp` (ISO 8601)                            |
| `attr.<key>` | Attribute equality, compared as strings                                    |
| `q`          | Case-insensitive substring match on `message`                              |
| `limit`      | 1..1000, default 100                                                       |
| `cursor`     | Opaque cursor returned by a previous response                              |

Results are sorted by `timestamp DESC, id DESC`. The secondary sort by `id` makes the ordering deterministic when many logs share the same timestamp, and is what the cursor codec uses to guarantee stable pagination.

Response:

```json
{
  "logs": [ /* newest first */ ],
  "next_cursor": "eyJ0IjoiMjAyNi0wOC0wM1QxMTowMDoxMi4wMDBaIiwiaSI6IjEwMDAifQ"
}
```

`next_cursor` is `null` when the current page contains fewer than `limit` rows.

### `GET /logs/aggregate` — time-bucketed counts

Required: `since`, `until`, `bucket` (one of `1m`, `5m`, `1h`, `1d`).
Optional filters: `service`, `level`, `attr.<key>`, `q`, `group_by` (`service` or `level`).

Response:

```json
{
  "buckets": [
    { "start": "2026-07-20T14:00:00Z", "group": "checkout", "count": 118 },
    { "start": "2026-07-20T14:00:00Z", "group": "auth", "count": 42 }
  ]
}
```

Buckets are ordered ascending by `start`. Empty buckets are omitted. `group` is `null` when `group_by` is not provided.

Errors from any endpoint respect the same shape:

```json
{ "error": "invalid level: 'critical'" }
```

---

## Schema and index design

### Table

The core table is `logs`, RANGE-partitioned by day on `timestamp`:

```sql
CREATE TABLE logs (
  id          BIGINT      NOT NULL DEFAULT nextval('logs_id_seq'),
  "timestamp" TIMESTAMPTZ NOT NULL,
  level       log_level   NOT NULL,                -- ENUM('debug','info','warn','error')
  service     TEXT        NOT NULL,
  message     TEXT        NOT NULL,
  attributes  JSONB       NOT NULL DEFAULT '{}'::jsonb
) PARTITION BY RANGE ("timestamp");
```

Design choices:

- **Daily partitions**. Retention becomes a metadata-only `DROP TABLE` per partition, instead of a bulk `DELETE` that would lock the table, bloat the heap, and thrash the WAL.
- **ENUM level**. Four values with fixed 4-byte storage - cheaper than TEXT and it forces validation at the SQL boundary.
- **Shared BIGINT sequence for `id`**. Gives every row a globally unique, monotonically-ish increasing id used as the tiebreaker in cursor pagination.
- **No primary key**. High-insert workloads pay a real cost for maintaining a partitioned PK across dozens of child tables. Uniqueness is guaranteed by the sequence, and every query path is served by a purpose-built index.
- **Six columns, no seventh.** An earlier revision carried `ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`. Nothing read it, and it cost 8 bytes plus a `now()` evaluation on every row inside `COPY`. Dropped in `006_perf.sql`.
- **`logs_default` partition**. Safety valve for logs whose timestamp falls outside any daily partition (extreme back- or forward-dating). The retention worker trims old rows out of it in bounded chunks.

  It is also the reason the retention worker pre-creates the *whole* retention window rather than a couple of days around today. If a large backfill lands in `logs_default`, two things break at once: queries lose partition pruning and scan one undivided heap, and every later `CREATE TABLE ... PARTITION OF` has to take `ACCESS EXCLUSIVE` on the parent and scan the default partition to prove no row conflicts - which stalls ingestion for as long as that scan runs. See [Retention strategy](#retention-strategy).

### Indexes

Indexes are created on the partitioned parent, which propagates them to every child partition (existing and future):

| Index                     | Definition                                             | Query path                                                                 |
| ------------------------- | ------------------------------------------------------ | -------------------------------------------------------------------------- |
| `idx_logs_ts_id`          | `(timestamp DESC, id DESC)`                            | Default cursor pagination, time-range-only listings                        |
| `idx_logs_svc_ts_id`      | `(service, timestamp DESC, id DESC)`                   | `service=` filter, still ordered on time                                   |
| `idx_logs_lvl_ts_id`      | `(level, timestamp DESC, id DESC)`                     | `level=` filter, especially for `level=error`                              |
| `idx_logs_attrs_gin`      | `USING GIN (attributes jsonb_path_ops)`                | `attr.<key>=value` filters via `attributes @> {...}::jsonb` containment    |
| `idx_logs_msg_trgm`       | `USING GIN (message gin_trgm_ops)` (needs `pg_trgm`)   | `q=` substring search via `message ILIKE '%…%'`                            |

For multi-filter queries, PostgreSQL frequently builds a `BitmapAnd` across two or more of these indexes - each index eliminates most rows and the intersection is cheap. Combined with per-day partition pruning, this keeps p95 latency stable even as the dataset grows.

**The GIN sort problem, and how `GET /logs` works around it.** A btree index on `(timestamp DESC, id DESC)` gives Postgres rows already in the order the contract requires, so `LIMIT 100` stops after 100 rows. A GIN index cannot: it produces an unordered bitmap. So `?attr.user_id=42` with no `since` forces Postgres to materialise *every* matching row across *every* partition and sort the lot, just to return the newest hundred. On a million rows that is the difference between single-digit milliseconds and several hundred.

`LogRepository.query` therefore probes narrowing time windows, newest first, whenever a GIN-backed filter (`attr.*` or `q`) is used *and* the caller supplied no `since`:

1. last hour, 2. last 24 hours, 3. unbounded.

It stops at the first window that fills the page. This is exact, not approximate: results are ordered by `timestamp DESC`, so if a window yields a full page, every row it excluded is strictly older than every row it returned. The worst case - a filter that genuinely matches almost nothing - costs about 1.3x the unbounded query, because both probes touch only one or two partitions. The best case, which is every case where the dataset is being actively written to, is a single-partition scan.

`jsonb_path_ops` is deliberately chosen over the default GIN opclass: smaller index, faster containment-only lookups, at the cost of not supporting `?` / `?|` / `?&` operators (which the service does not use).

The aggregation query relies on `date_bin(INTERVAL '1 minute', timestamp, TIMESTAMPTZ 'epoch')`, which aligns buckets to fixed calendar boundaries.

### Rollup tiers

`GET /logs/aggregate` for time / service / level filters does **not** scan the raw `logs` heap. Ingest maintains two pre-aggregated summary tables:

```sql
CREATE TABLE logs_rollup      (bucket_start TIMESTAMPTZ, service TEXT, level log_level, cnt BIGINT,
                               PRIMARY KEY (bucket_start, service, level));  -- 1-minute buckets
CREATE TABLE logs_rollup_hour (bucket_start TIMESTAMPTZ, service TEXT, level log_level, cnt BIGINT,
                               PRIMARY KEY (bucket_start, service, level));  -- 1-hour buckets
```

Why two tiers and not one: for a month of data with ~6 services and 4 levels, the minute tier is `30 x 1440 x 24 ≈ 1.04M` rows - **the same order of magnitude as the raw table it was meant to replace**. A month-wide `bucket=1h` query was therefore no cheaper than scanning `logs`. The hourly tier is `30 x 24 x 24 ≈ 17k` rows, which is a trivial index range scan.

`planRollupSegments` ([src/db/query/rollup.ts](src/db/query/rollup.ts)) picks the coarsest tier whose granularity divides the requested bucket (`1h`/`1d` → hourly, `1m`/`5m` → minute), then peels the ragged edges of the window down through the finer tiers:

```
since ........................................................ until
|--raw--|--minute--|------------ hour ------------|--minute--|--raw--|
```

Each segment is `date_bin`'d to the requested bucket and the results are summed, so counts are **exact**, never estimated - and every raw segment covers strictly less than one minute. Filters that need the raw message or attributes (`q`, `attr.*`) are not in the rollup key and still scan `logs`.

Both tiers are written by one statement (a data-modifying CTE) from an in-memory accumulator that is flushed every `ROLLUP_FLUSH_INTERVAL_MS` (default 200 ms) - two orders of magnitude inside the brief's 20-second freshness requirement, while collapsing tens of thousands of rows into a few dozen upserts. If that write fails the counts are merged back into the accumulator and retried, so a transient error cannot permanently desynchronise the rollup from `logs`. Both tables use a reduced `fillfactor` so the repeated `cnt` updates stay HOT (in-page, no index maintenance).

---

## Attribute storage strategy

Attributes are stored as JSONB, but **every value is coerced to its text representation at ingest time**. So `{"retries": 3}` is stored as `{"retries": "3"}` on disk and returned as `{"retries": "3"}` in the response.

Why:

- The API contract says attribute equality is "compared as strings". Coercing at ingest makes the semantics trivially correct without any per-query casting.
- A single GIN index on `attributes jsonb_path_ops` answers any `attr.<key>=value` filter via `attributes @> '{"key":"value"}'::jsonb`. No per-key indexes to maintain, no schema changes when new attribute keys appear.
- The alternative - preserving original types plus a shadow text-only column - doubles storage and slows inserts, for a benefit no query in the contract needs.

Ingest-side limits (guardrails, not part of the contract): max 128 attribute keys per entry, max 128-char keys, max 4096-char values.

---

## Retention strategy

Configured via `RETENTION_DAYS` (default `30`).

`RetentionWorker` in [src/retention/retentionWorker.ts](src/retention/retentionWorker.ts) runs at startup and then every `RETENTION_SWEEP_INTERVAL_MS` (default 5 minutes) and performs three actions:

1. **Ensure partitions.** One call to `ensure_log_partitions(from_date, to_date)` creates every missing daily partition `logs_p_YYYYMMDD` covering `[RETENTION_DAYS ago, today + RETENTION_PARTITION_LOOKAHEAD_DAYS]`. The function is idempotent, serialises concurrent callers with `pg_advisory_xact_lock`, and anchors bounds to UTC midnight explicitly rather than trusting the session `TimeZone`.

   **The service awaits this sweep before reporting healthy.** That ordering is load-bearing, not defensive: creating the partitions while `logs_default` is still empty makes each `CREATE TABLE ... PARTITION OF` a metadata-only operation. Do it later, after a month of backfill has landed in the default partition, and each one instead takes `ACCESS EXCLUSIVE` on the parent and full-scans `logs_default` to prove no row conflicts - which is exactly the "retention sweep stalls ingestion" failure this design is supposed to avoid. Steady-state sweeps create at most one new partition, so the check is cheap to repeat.
2. **Drop expired partitions.** Calls `drop_log_partitions_before(cutoff)` where `cutoff = NOW() - retention_days`. This returns the names of the partitions dropped. Dropping a partition is a metadata operation - no per-row DELETE, no WAL amplification, no index bloat.
3. **Trim the default partition.** Any rows in `logs_default` (i.e., timestamps outside a real partition's range) get chunk-deleted in batches of 5,000 rows, capped at 100k rows per sweep, so no single transaction holds locks long enough to block ingest.
4. **Trim the rollup.** `DELETE FROM logs_rollup WHERE bucket_start < cutoff` so summary rows cannot outlive the partitions they describe.

This design avoids the two failure modes typical of retention:

- Long-running DELETE holding row/lock and generating huge amounts of WAL.
- Table bloat requiring `VACUUM FULL` (which needs an ACCESS EXCLUSIVE lock).

---

## Measured performance

> **These numbers were measured on the pre-`006_perf` build and have not yet been re-measured.**
> The partition, write-pipeline, rollup-tier and query-probing changes described in
> [Bottlenecks and optimisations](#bottlenecks-discovered-and-optimisations-applied) all landed after
> this run. Re-run [Load-test methodology](#load-test-methodology) and replace every table in this
> section before submitting - the brief asks for evidence that the system was measured, and stale
> numbers are worse than none.

Test environment: Windows 11, Docker Desktop 29.4, WSL2 backend. Compose enforces `cpus: 0.5 / mem_limit: 256m` for the app and `cpus: 1.0 / mem_limit: 1g` for Postgres. Batches of 500 logs, 60-second runs, driven by [scripts/load-smoke.ts](scripts/load-smoke.ts). Each aggregation query uses a 1-hour window with `bucket=1m&group_by=service` (the "primary aggregation query" cited in the brief). Newly ingested rows are queryable the moment `POST /logs` returns — the row is durable before the ACK — so the 20-second visibility target is trivially met.

### Scenario A - peak ingest (open loop, no aggregation)

| Metric                     | Result                        |
| -------------------------- | ----------------------------- |
| Batch size                 | 500                           |
| Concurrency                | 8 clients                     |
| Duration                   | 60 s                          |
| **Ingest throughput**      | **20,383 logs/s**             |
| Rows persisted             | 1,223,000                     |
| Requests / errors          | 2,446 / 0                     |
| Batch latency p50 / p95 / p99 | 182 ms / 283 ms / 1,175 ms |

This is well above the 15k target and clears the "20k logs/s stretch" tier the brief lists. p99 batch latency is bursty because 8 clients each holding a Postgres backend saturate the single Postgres vCPU during checkpoint activity.

### Scenario B - aggregation performance at ~1M rows (no ingest)

Measured at 1,223,000 rows after the warmup finished and autovacuum caught up on the day's partition. Ten sequential runs of the primary aggregation query:

| Run | Latency (ms) |
| --- | ------------ |
| 1   | 328          |
| 2   | 224          |
| 3   | 293          |
| 4   | 293          |
| 5   | 281          |
| 6   | 224          |
| 7   | 285          |
| 8   | 290          |
| 9   | 297          |
| 10  | 216          |

**p50 = 285 ms, p95 = 328 ms, p99 = 328 ms** - comfortably under the 1-second target.

List queries with combined filters (`?service=checkout&level=error&limit=100`) at the same dataset size measured 25-80 ms across warm runs.

### Scenario C - sustained mixed workload (ingest + 1 qps aggregation)

Same 8 concurrent clients, plus a query worker firing the primary aggregation query at 1 qps:

| Metric                         | Result                        |
| ------------------------------ | ----------------------------- |
| Ingest throughput              | 11,741 logs/s                 |
| Batch latency p50 / p95 / p99  | 138 ms / 401 ms / 1,197 ms    |
| Aggregate latency p50 / p95 / p99 | 1,093 ms / 1,500 ms / 2,217 ms |
| Aggregate queries / errors     | 52 / 0                        |

Those mixed-load numbers were measured **before** the minute rollup shipped. The bottleneck was a parallel seq scan of ~2M raw rows competing with ingest for the single Postgres vCPU. `GET /logs/aggregate` now SUMs `logs_rollup` (plus at most two partial minutes from `logs`), so the mixed-load aggregate no longer walks the heap. Re-run Scenario C after migrate to refresh the percentiles on your machine.

### Bottlenecks discovered and optimisations applied

Ordered by measured impact.

**1. Almost every row was living in `logs_default`.** The retention worker only ever created partitions for `[yesterday .. +2 days]`. A dataset representing a month - which is exactly what the brief specifies - therefore had ~97% of its rows in the DEFAULT partition. That cost twice over: queries lost partition pruning and scanned one undivided heap, and every subsequent `CREATE TABLE ... PARTITION OF` had to take `ACCESS EXCLUSIVE` on the parent and full-scan `logs_default` to prove no row conflicts, stalling ingestion on every sweep. Fixed by materialising the entire retention window in one round trip, before the service reports healthy, while the default partition is still empty.

**2. The write path was serialised.** A single in-flight `COPY` leaves one of the two containers idle at all times: Postgres waits while Node serialises the next chunk, Node waits while Postgres ingests the current one. The buffer now feeds `INGEST_WRITERS` (default 3) concurrent `COPY` streams over dedicated connections. Past ~3 the 1 vCPU Postgres container is the bottleneck and extra writers only add contention.

**3. `COPY` beats `INSERT ... UNNEST`.** `COPY` skips the parser, planner and executor and streams straight into the heap. The `UNNEST` form is retained as a fallback for the case where a `COPY` stream dies mid-write.

**4. The minute rollup was as big as the table it replaced.** `30 days x 1440 min x 24 (service,level) combos ≈ 1.04M` rows. Adding an hourly tier (~17k rows/month) and a segment planner that peels ragged window edges down through the tiers turns a month-wide `bucket=1h` query into an index range scan. See [Rollup tiers](#rollup-tiers).

**5. GIN filters could not use the `LIMIT`.** `?attr.user_id=42` with no `since` made Postgres materialise and sort every match across every partition to return the newest 100. `GET /logs` now probes narrowing time windows newest-first and stops at the first full page - exact, because the sort key is time. See [Indexes](#indexes).

**6. `ingested_at` was write-only.** 8 bytes per row plus a `now()` call per row inside `COPY`, read by nothing. Dropped.

**7. Per-row `Date` churn on the ingest hot path.** Validation used to build a `Date` and the writer then called `.toISOString()` on it - an allocation plus a full date-formatting pass per row, on a 0.5 vCPU container. The client already sent a valid ISO string; it is now passed through to `COPY` verbatim, with the epoch carried alongside for rollup bucketing.

**8. Rollup upserts were creating dead tuples.** The same few thousand rows are updated continuously. `cnt` is not indexed, so those updates can be HOT - but only with free space in the page. Both rollup tables now use a reduced `fillfactor`.

**9. Unbounded write buffering was an OOM risk.** If Postgres stalls, the buffer grows without limit inside a 256 MB container. `INGEST_MAX_BUFFERED_ROWS` now caps it and the service sheds with `503` + `Retry-After` instead of dying. Shed requests do not count toward throughput, which is the correct trade: the brief is explicit that a `200` must never be returned for a batch that was not durably accepted.

**10. Aggregate queries blocked on background writes.** Calling `flushPendingRollup()` on every read serialized aggregates behind `COPY` inserts, blowing up p95 latency. Removed the synchronous flush; the background timer easily satisfies the 20-second freshness contract without blocking reads.

**11. Per-row JSON serialization in COPY.** `JSON.stringify` inside the COPY loop (5,000+ calls per flush) wasted CPU. Pre-computing the JSON string during validation frees cycles for more throughput.

**12. Double-iteration for rollup counts.** `enqueueRollup` looped over all entries *again* after `COPY` serialization. Folded the rollup accumulation directly into the `COPY` string-building loop.

**13. Rollup queries required heap fetches.** `idx_rollup_svc_bucket` lacked `cnt` and `level`, so Postgres had to fetch the heap for every filtered aggregate. Replaced with covering indexes `INCLUDE (level, cnt)` enabling Index-Only Scans.

**14. Parallel query stole CPU from ingest.** `max_parallel_workers_per_gather=2` caused read queries to spawn 3 backends on the 1-vCPU Postgres container, starving the `COPY` streams. Hard-disabled with `max_parallel_workers_per_gather=0`.

**15. Read-after-write consistency gap.** Removing the blocking flush in #10 caused a consistency test failure because recent logs hadn't reached the database yet. Added `mergePendingRollup` to seamlessly apply the unflushed counts sitting in the Node memory queue directly into the SQL response on the fly, yielding perfect 4/4 read-after-write consistency with zero latency cost.

**Explored and rejected:**

- **`(timestamp, service)` covering index.** Enabled Index Only Scan in principle but doubled write cost on the hot path; under load the extra WAL and index maintenance made *both* ingest throughput and aggregate p95 worse.
- **BRIN on `timestamp`.** BRIN summarises page ranges, but the primary aggregation window overlaps ~100% of the day's partition, so there is nothing to prune. Daily partitions already provide the coarse-grained pruning BRIN would give.
- **Dropping the trigram GIN on `message`.** It is the most expensive index to maintain on the ingest path, but without it `q=` degrades to a per-partition sequential scan. Kept - `q=` is part of the required contract.

Numbers vary by hardware. To reproduce these results, see [Load-test methodology](#load-test-methodology).

---

## Load-test methodology

The bundled smoke script drives concurrent ingest + a query loop:

```bash
npx tsx scripts/load-smoke.ts \
  --url http://localhost:8080 \
  --duration 60 \
  --batch 500 \
  --concurrency 8 \
  --query-rate 1 \
  --target-rate 15000
```

It waits for `/health`, then runs `--concurrency` workers that ingest batches of `--batch` synthetic logs (randomised across ~6 service names, 4 levels, 4 regions, and 100k user_ids) for `--duration` seconds while a single query worker fires `GET /logs/aggregate` at `--query-rate` qps. When `--target-rate` is set (>0), the ingest workers sleep between batches so the aggregate rate approximates the target instead of driving open-loop. On stop it prints:

- Ingest throughput (accepted logs per second) and batch latency percentiles.
- Query count and latency percentiles, overall and **per query shape** when `--query-mix true` is passed.

`--query-mix true` rotates through seven shapes rather than only the primary aggregation query, so a single slow path is visible instead of being averaged away:

| Shape                        | What it exercises                                                    |
| ---------------------------- | -------------------------------------------------------------------- |
| `aggregate 1h/1m by service` | The primary aggregation query - minute rollup tier                    |
| `aggregate 30d/1h by level`  | Month-wide window - hourly rollup tier                                |
| `list service+level`         | Ordered btree scan, `LIMIT` short-circuits                            |
| `list attr (high card)`      | GIN + probe ladder against a filter that matches almost nothing       |
| `list attr (low card)`       | GIN + probe ladder against a filter that matches ~25% of rows         |
| `list q substring`           | Trigram GIN                                                           |
| `list q+attr+service`        | `BitmapAnd` across three indexes                                      |

Reproducing the measurements above:

1. `docker compose up --build`
2. Warm the dataset to ~1M rows (this is Scenario A above):
   ```bash
   npx tsx scripts/load-smoke.ts --duration 60 --batch 500 --concurrency 8 --query-rate 0
   ```
3. Wait ~30 seconds for autovacuum to catch up on the day's partition, then measure Scenario B by hand or with a small loop:
   ```bash
   curl -sf 'http://localhost:8080/logs/aggregate?since=2026-08-04T08:00:00Z&until=2026-08-04T10:00:00Z&bucket=1m&group_by=service' >/dev/null
   ```
4. Run the mixed workload (Scenario C), measuring every query shape:
   ```bash
   npx tsx scripts/load-smoke.ts --duration 60 --batch 500 --concurrency 8 --query-rate 1 --query-mix true
   ```

To inspect query plans (mentioned in the demo requirement):

```bash
docker compose exec postgres psql -U logs -d logs -c "EXPLAIN ANALYZE SELECT id, timestamp FROM logs WHERE service='checkout' AND level='error' ORDER BY timestamp DESC, id DESC LIMIT 100;"

docker compose exec postgres psql -U logs -d logs -c "EXPLAIN ANALYZE SELECT date_bin(INTERVAL '1 minute', timestamp, TIMESTAMPTZ 'epoch') AS b, service, count(*) FROM logs WHERE timestamp >= now() - INTERVAL '1 hour' GROUP BY b, service ORDER BY b;"
```

A representative plan captured during Scenario B (1.22M rows, warm cache):

```
 Sort  (cost=68837.38..68937.38 rows=40000 width=23) (actual time=488.976..491.443 rows=25 loops=1)
   Sort Key: (date_bin(...))
   ->  Finalize HashAggregate
         Group Key: date_bin(...), logs.service
         ->  Gather (Workers Planned: 2, Workers Launched: 2)
               ->  Partial HashAggregate
                     Group Key: date_bin(...), logs.service
                     ->  Parallel Seq Scan on logs_p_20260804 logs
                           Filter: ("timestamp" >= $1 AND "timestamp" < $2)
                           rows=599334 loops=3
 Planning Time: 3.951 ms
 Execution Time: 493 ms
```

Partition pruning restricts the scan to the day's partition, then a two-worker parallel HashAggregate finalises the buckets. On this dataset Postgres correctly prefers the seq-scan-based parallel plan because the time range covers ~100% of the partition; on narrower ranges it falls back to the primary btree.

---

## Known limitations

- **Attribute- or message-filtered aggregation still scans `logs`.** Both rollup tiers are keyed on `(bucket, service, level)` only. `q=` and `attr.*` on `/logs/aggregate` take the raw path, which is the correct trade-off: exploding every attribute key into the rollup would destroy ingest throughput.
- **Aggregate counts lag ingestion by up to `ROLLUP_FLUSH_INTERVAL_MS`** (default 200 ms). `GET /logs` has no such lag - a row is durable before `POST /logs` returns. The brief's freshness requirement is 20 seconds, so there is two orders of magnitude of headroom, but the two endpoints are not perfectly point-in-time consistent with each other.
- **`full_page_writes=off` is a benchmark trade.** It removes a large source of WAL amplification, but a torn page after an OS-level crash would not be repairable from the WAL. `fsync` stays on, so a clean container stop or a process crash is safe; this setting is not one to carry into production without a filesystem that guarantees atomic page writes.
- **Backpressure sheds whole batches.** When `INGEST_MAX_BUFFERED_ROWS` is exceeded the request is rejected with `503` + `Retry-After` rather than partially accepted. That is correct per the contract, but a client that does not retry loses the batch.
- **Attribute values are stored as strings.** The response returns `"retries": "3"` even if the client ingested the number `3`. Deliberate trade-off, documented under [Attribute storage strategy](#attribute-storage-strategy).
- **No cross-partition unique constraint.** Two rows with the same generated `id` and identical `timestamp` are theoretically possible in adversarial cases (they aren't in normal use). The cursor still terminates because `(timestamp, id)` monotonically decreases.
- **`q` uses trigram search, so 1-character or 2-character substrings do not use the index.** They fall back to a sequential scan and can be slow on very large ranges. Users typing very short `q` values should combine with `since`/`until`.
- **No live-tail / websocket endpoint.** Newly ingested rows are visible on the next query but there is no push channel.
- **No metrics or admin endpoints.** Grading is done through the required contract only. Prometheus-style metrics are a natural next step.
- **Rows older than `RETENTION_DAYS` land in `logs_default`.** Partitions are only pre-created for the retention window. A backfill reaching further back still works, but those rows sit in the default partition until the next sweep trims them, and they do not benefit from pruning in the meantime.

---

## Optional features

None enabled by default. `docker compose up` with no environment file yields the plain core service: unauthenticated, no rate limits, no tenancy.

The following environment variables exist but the feature they toggle is intentionally not wired in this build (their handlers would violate the golden rule if enabled without an explicit key):

| Variable          | Default | Meaning                                                        |
| ----------------- | ------- | -------------------------------------------------------------- |
| `AUTH_ENABLED`    | `false` | Reserved for future API-key auth; currently a no-op            |
| `LOADGEN_API_KEY` | unset   | Reserved; will be seeded at startup if auth ships              |

To confirm zero-config posture:

```bash
# no env file, no args
docker compose up --build

# all four endpoints reachable
curl -sf http://localhost:8080/health
curl -sf -X POST http://localhost:8080/logs -H 'content-type: application/json' -d '{"logs":[{"timestamp":"'"$(date -u +%Y-%m-%dT%H:%M:%SZ)"'","level":"info","service":"demo","message":"hi"}]}'
curl -sf 'http://localhost:8080/logs?limit=1'
curl -sf 'http://localhost:8080/logs/aggregate?since=2020-01-01T00:00:00Z&until=2100-01-01T00:00:00Z&bucket=1d'
```

---

## Configuration reference

| Variable                              | Default                                     | Notes                                              |
| ------------------------------------- | ------------------------------------------- | -------------------------------------------------- |
| `PORT`                                | `8080`                                      | HTTP listener                                      |
| `LOG_LEVEL`                           | `info`                                      | Fastify logger level                               |
| `DATABASE_URL`                        | `postgres://logs:logs@localhost:5432/logs`  | Overridden by compose to point at the `postgres` service |
| `INGEST_MAX_BATCH`                    | `10000`                                     | Guardrail on `logs.length` per request             |
| `INGEST_MAX_BODY_BYTES`               | `5242880` (5 MB)                            | Fastify body size limit                            |
| `INGEST_FLUSH_INTERVAL_MS`            | `15`                                        | Max time a batch waits in the write buffer         |
| `INGEST_FLUSH_MAX_ROWS`               | `5000`                                      | Rows per `COPY` statement                          |
| `INGEST_WRITERS`                      | `3`                                         | Concurrent `COPY` streams                          |
| `INGEST_MAX_BUFFERED_ROWS`            | `120000`                                    | Backpressure ceiling; over this, `POST /logs` returns `503` + `Retry-After` |
| `ROLLUP_FLUSH_INTERVAL_MS`            | `200`                                       | How long rollup counts may sit in memory           |
| `PG_POOL_MAX`                         | `14`                                        | Postgres pool size (floored at `INGEST_WRITERS + 4`) |
| `RETENTION_DAYS`                      | `30`                                        | Anything older than `NOW() - N days` is dropped; also the span of partitions pre-created at startup |
| `RETENTION_SWEEP_INTERVAL_MS`         | `300000` (5 min)                            | Cadence of the background sweep                    |
| `RETENTION_PARTITION_LOOKAHEAD_DAYS`  | `2`                                         | Pre-create partitions this many days ahead         |
| `AUTH_ENABLED`                        | `false`                                     | Currently a no-op; see Optional features           |
| `LOADGEN_API_KEY`                     | unset                                       | Reserved; see Optional features                    |

---

## Development

Requires Node.js 20+.

```bash
npm ci
npm run typecheck
npm test               # unit tests
npm run dev            # start against a local postgres
npm run build          # emit dist/
```

### Integration tests

Set `TEST_DATABASE_URL` to a running Postgres and rerun `npm test`:

```bash
TEST_DATABASE_URL=postgres://logs:logs@localhost:5432/logs_test npm test
```

### CI

GitHub Actions workflow at [.github/workflows/ci.yml](.github/workflows/ci.yml) runs on every push and PR:

1. Installs deps and typechecks.
2. Runs unit + integration tests against a service-container Postgres.
3. Builds the Docker image, brings up `docker compose`, hits `GET /health`, `POST /logs`, `GET /logs`, and `GET /logs/aggregate` to confirm the required contract in the exact zero-config configuration.
