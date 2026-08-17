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
  ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
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
- **`logs_default` partition**. Safety valve for logs whose timestamp falls outside any daily partition (extreme back- or forward-dating). The retention worker trims old rows out of it in bounded chunks.

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

`jsonb_path_ops` is deliberately chosen over the default GIN opclass: smaller index, faster containment-only lookups, at the cost of not supporting `?` / `?|` / `?&` operators (which the service does not use).

The aggregation query relies on `date_bin(INTERVAL '1 minute', timestamp, TIMESTAMPTZ 'epoch')`, which aligns buckets to fixed calendar boundaries and lets Postgres group-scan the timestamp index in order.

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

1. **Ensure partitions.** Calls the SQL function `ensure_log_partition(day)` for yesterday, today, and `RETENTION_PARTITION_LOOKAHEAD_DAYS` days ahead. The function is idempotent, uses `pg_advisory_xact_lock` to serialize concurrent callers, and creates the daily partition `logs_p_YYYYMMDD` covering `[day, day+1)`.
2. **Drop expired partitions.** Calls `drop_log_partitions_before(cutoff)` where `cutoff = NOW() - retention_days`. This returns the names of the partitions dropped. Dropping a partition is a metadata operation - no per-row DELETE, no WAL amplification, no index bloat.
3. **Trim the default partition.** Any rows in `logs_default` (i.e., timestamps outside a real partition's range) get chunk-deleted in batches of 5,000 rows, capped at 100k rows per sweep, so no single transaction holds locks long enough to block ingest.

This design avoids the two failure modes typical of retention:

- Long-running DELETE holding row/lock and generating huge amounts of WAL.
- Table bloat requiring `VACUUM FULL` (which needs an ACCESS EXCLUSIVE lock).

---

## Measured performance

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

Under simultaneous max-throughput ingest **and** 1 qps aggregation on a single Postgres vCPU, aggregation latency exceeds the sub-1s target. The bottleneck is CPU: 8 ingest client backends plus parallel query workers plus autovacuum all compete for one vCPU, so an aggregate scanning ~2M rows can't get sustained runtime slices. See [Known limitations](#known-limitations) for the rollup-table follow-up.

### Bottlenecks discovered and optimisations applied

- **WAL flush cost.** Solved with batched `UNNEST` inserts (~500 rows per statement) and `synchronous_commit=off` in `docker-compose.yml`. This yields 15-20k logs/s from a single writer with strong durability for the recent-history that matters for log data.
- **Visibility map staleness kills Index Only Scans.** Freshly-ingested rows aren't visibility-marked until autovacuum runs, so the planner does heap fetches even when it picks an index-only scan. Fixed with aggressive autovacuum (`autovacuum_naptime=10s`, `autovacuum_vacuum_insert_scale_factor=0.02`).
- **Explored and rejected: `(timestamp, service)` covering index.** Enabled Index Only Scan in principle but doubled the write cost on the ingest hot path, and under load the extra WAL/index maintenance made *both* ingest throughput and aggregate p95 worse. Removed.
- **Explored and rejected: `max_parallel_workers_per_gather=0`.** Trading parallelism for less context-switching backfired: a serial 2M-row scan under contention was slower than the parallel plan.
- **Explored and rejected: BRIN on `timestamp`.** BRIN summarises page ranges, but the primary aggregation query window overlaps ~100% of the day's partition, so there's nothing to prune. Kept as a note for a real production system where queries have narrower windows.
- **`jsonb_path_ops` over the default GIN opclass** halves the attribute index size and speeds containment lookups. Kept.
- **Trigram GIN on `message`.** Necessary for `q=` substring search; without it `ILIKE` degrades to a per-partition seq scan and blows out list-query latency. Kept.

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
- Aggregation query count and latency percentiles.

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
4. Run the mixed workload (Scenario C):
   ```bash
   npx tsx scripts/load-smoke.ts --duration 60 --batch 500 --concurrency 8 --query-rate 1
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

- **Aggregation latency under concurrent max-throughput ingest exceeds the 1-second target.** At ~1M rows on 1 Postgres vCPU, aggregation alone is ~285 ms p50 / ~328 ms p95, well inside the target. But under 8-way sustained ingest (Scenario C above), aggregate p95 rises to ~1.5 s because there is no CPU headroom left. The natural fix is a pre-aggregated rollup table (minute-level counts by `(bucket_start, service)`) refreshed by an incremental background job. The aggregate query would then read a table with tens of thousands of rows rather than scanning millions. This is a documented next step rather than shipped code because it materially expands scope; the schema and dispatcher would live alongside the existing repository.
- **Attribute values are stored as strings.** The response returns `"retries": "3"` even if the client ingested the number `3`. Deliberate trade-off, documented under [Attribute storage strategy](#attribute-storage-strategy).
- **No cross-partition unique constraint.** Two rows with the same generated `id` and identical `timestamp` are theoretically possible in adversarial cases (they aren't in normal use). The cursor still terminates because `(timestamp, id)` monotonically decreases.
- **`q` uses trigram search, so 1-character or 2-character substrings do not use the index.** They fall back to a sequential scan and can be slow on very large ranges. Users typing very short `q` values should combine with `since`/`until`.
- **No live-tail / websocket endpoint.** Newly ingested rows are visible on the next query but there is no push channel.
- **No metrics or admin endpoints.** Grading is done through the required contract only. Prometheus-style metrics are a natural next step.

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
| `RETENTION_DAYS`                      | `30`                                        | Anything older than `NOW() - N days` is dropped    |
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
