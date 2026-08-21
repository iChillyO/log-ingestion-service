// Persistence layer for logs. All SQL construction happens here (and in
// buildLogFilters). HTTP handlers should never see raw SQL.

import type { Pool, PoolClient } from "pg";
import { from as copyFrom } from "pg-copy-streams";
import { buildWhereClause, type LogFilters } from "../query/buildLogFilters";
import { canUseRollup, planRollupSegments, type RollupSource } from "../query/rollup";
import type { Cursor } from "../../domain/cursor";
import type { LogLevel, ValidatedLogEntry } from "../../domain/logSchemas";

export interface StoredLog {
  id: string;
  timestamp: Date;
  level: LogLevel;
  service: string;
  message: string;
  attributes: Record<string, string>;
}

export type BucketSize = "1m" | "5m" | "1h" | "1d";
export type GroupBy = "service" | "level";

const BUCKET_TO_INTERVAL: Record<BucketSize, string> = {
  "1m": "1 minute",
  "5m": "5 minutes",
  "1h": "1 hour",
  "1d": "1 day",
};

const GROUP_TO_COLUMN: Record<GroupBy, string> = {
  service: "service",
  level: "level::text",
};

const SOURCE_TO_TABLE: Record<RollupSource, string> = {
  hour: "logs_rollup_hour",
  minute: "logs_rollup",
  raw: "logs",
};

export interface AggregateOptions {
  filters: LogFilters;
  since: Date;
  until: Date;
  bucket: BucketSize;
  groupBy: GroupBy | null;
}

export interface AggregateBucket {
  start: Date;
  group: string | null;
  count: number;
}

export interface QueryOptions {
  filters: LogFilters;
  limit: number;
  cursor: Cursor | null;
}

/**
 * Thrown when the write buffer is already holding more rows than we are
 * willing to keep in a 256 MB container. The HTTP layer turns this into a
 * `503 + Retry-After`: shedding load is honest, silently 200-ing a batch we
 * have not persisted is not.
 */
export class IngestOverloadedError extends Error {
  readonly retryAfterSeconds = 1;
  constructor(message = "ingest buffer is full, retry shortly") {
    super(message);
    this.name = "IngestOverloadedError";
  }
}

export interface LogRepositoryOptions {
  /** Max time a batch waits in the buffer before being written. */
  flushIntervalMs?: number;
  /** Rows per COPY statement. */
  flushMaxRows?: number;
  /** Concurrent COPY streams. */
  writers?: number;
  /** Hard ceiling on buffered + in-flight rows before we shed load. */
  maxBufferedRows?: number;
  /** How long rollup counts may sit in memory before being written. */
  rollupFlushIntervalMs?: number;
}

// ---------------------------------------------------------------------------
// Write buffer configuration.
//
// Two separate wins are stacked here:
//
//  1. Coalescing. Many small HTTP batches (30-500 rows each) become a few
//     large COPY statements. Each COPY has fixed parse/plan/WAL-flush overhead
//     that is paid once instead of once per request.
//
//  2. Pipelining. A single serialised writer leaves one of the two containers
//     idle at all times: Postgres waits while Node serialises the next chunk,
//     Node waits while Postgres ingests the current one. Running a few COPY
//     streams concurrently keeps both busy. Beyond ~3 the 1 vCPU Postgres
//     container becomes the bottleneck and extra writers only add contention.
//
// Safety: the HTTP handler awaits `repo.insertMany(...)`, which does not
// resolve until the rows are committed, so 200 is never returned for data that
// is not persisted.
// ---------------------------------------------------------------------------

const DEFAULT_FLUSH_INTERVAL_MS = 15;
const DEFAULT_FLUSH_MAX_ROWS = 5_000;
const DEFAULT_WRITERS = 3;
const DEFAULT_MAX_BUFFERED_ROWS = 120_000;
const DEFAULT_ROLLUP_FLUSH_MS = 200;

/**
 * Time windows tried, newest first, for `GET /logs` queries that filter on an
 * attribute or on message text but give no `since`.
 *
 * Those filters are answered by GIN indexes, which produce an unordered bitmap.
 * Without a time bound Postgres has to materialise every matching row across
 * every partition and sort it just to return the newest 100. Probing a narrow
 * window first means the common case (matches exist recently) touches one or
 * two partitions. Correctness is preserved because results are ordered by
 * timestamp DESC: if a window yields a full page, every row we excluded is
 * strictly older than every row we returned.
 *
 * Two probes, then the unbounded query. Worst case (a genuinely sparse filter)
 * is ~1.3x the cost of going unbounded immediately, because both probes touch
 * only one or two partitions. Best case - which is the case whenever the
 * dataset is being actively written to - is a single-partition scan.
 */
const PROBE_SPANS_MS = [60 * 60_000, 24 * 60 * 60_000];

interface BufferEntry {
  entries: ValidatedLogEntry[];
  resolve: () => void;
  reject: (err: Error) => void;
}

// COPY TEXT format escaping: backslash, tab, newline, carriage return.
// NUL never reaches here - validation rejects entries containing one, because
// PostgreSQL cannot store U+0000 in a text column at all.
const NEEDS_ESCAPE = /[\\\t\n\r]/;
function escapeCopyValue(s: string): string {
  if (!NEEDS_ESCAPE.test(s)) return s;
  return s
    .replace(/\\/g, "\\\\")
    .replace(/\t/g, "\\t")
    .replace(/\n/g, "\\n")
    .replace(/\r/g, "\\r");
}

const COPY_SQL = `COPY logs ("timestamp", level, service, message, attributes) FROM STDIN WITH (FORMAT text)`;

export class LogRepository {
  private readonly flushIntervalMs: number;
  private readonly flushMaxRows: number;
  private readonly maxWriters: number;
  private readonly maxBufferedRows: number;
  private readonly rollupFlushIntervalMs: number;

  // Write buffer. `bufferHead` is an index rather than Array.shift() so that
  // draining thousands of queued batches stays O(n) instead of O(n^2).
  private buffer: BufferEntry[] = [];
  private bufferHead = 0;
  private bufferedRows = 0;
  private inFlightRows = 0;
  private flushTimer: ReturnType<typeof setTimeout> | null = null;
  private activeWriters = 0;
  private draining: Array<() => void> = [];

  // Dedicated connections for COPY writes, kept checked out so we never pay
  // pool checkout + backend startup on the hot path.
  private idleCopyClients: PoolClient[] = [];

  // Rollup accumulator. Flushes are chained onto `rollupInFlight` so at most
  // one runs at a time and callers (shutdown, tests) can await the tail.
  private rollupQueue = new Map<string, number>();
  private rollupTimer: ReturnType<typeof setTimeout> | null = null;
  private rollupInFlight: Promise<void> = Promise.resolve();

  constructor(
    private readonly pool: Pool,
    options: LogRepositoryOptions = {},
  ) {
    this.flushIntervalMs = options.flushIntervalMs ?? DEFAULT_FLUSH_INTERVAL_MS;
    this.flushMaxRows = options.flushMaxRows ?? DEFAULT_FLUSH_MAX_ROWS;
    this.maxWriters = Math.max(1, options.writers ?? DEFAULT_WRITERS);
    this.maxBufferedRows = options.maxBufferedRows ?? DEFAULT_MAX_BUFFERED_ROWS;
    this.rollupFlushIntervalMs = options.rollupFlushIntervalMs ?? DEFAULT_ROLLUP_FLUSH_MS;
  }

  // =========================================================================
  // Write path
  // =========================================================================

  /**
   * Buffer `entries` and resolve once they are committed. Rejects with
   * `IngestOverloadedError` if we are already holding more than we can afford
   * to keep in memory.
   */
  async insertMany(entries: ValidatedLogEntry[]): Promise<void> {
    if (entries.length === 0) return;

    if (this.bufferedRows + this.inFlightRows + entries.length > this.maxBufferedRows) {
      throw new IngestOverloadedError();
    }

    return new Promise<void>((resolve, reject) => {
      this.buffer.push({ entries, resolve, reject });
      this.bufferedRows += entries.length;

      if (this.bufferedRows >= this.flushMaxRows) {
        this.clearFlushTimer();
        this.pump();
      } else if (!this.flushTimer) {
        this.flushTimer = setTimeout(() => {
          this.flushTimer = null;
          this.pump();
        }, this.flushIntervalMs);
      }
    });
  }

  private clearFlushTimer(): void {
    if (this.flushTimer) {
      clearTimeout(this.flushTimer);
      this.flushTimer = null;
    }
  }

  private get queuedBatches(): number {
    return this.buffer.length - this.bufferHead;
  }

  /** Start writers until either the buffer is empty or all slots are busy. */
  private pump(): void {
    while (this.queuedBatches > 0 && this.activeWriters < this.maxWriters) {
      const batch = this.takeBatch();
      this.activeWriters += 1;
      void this.writeBatch(batch).finally(() => {
        this.activeWriters -= 1;
        if (this.queuedBatches > 0) {
          this.pump();
        } else if (this.activeWriters === 0) {
          const waiters = this.draining;
          this.draining = [];
          for (const done of waiters) done();
        }
      });
    }
  }

  /** Pull whole caller-batches off the front of the buffer, up to flushMaxRows. */
  private takeBatch(): BufferEntry[] {
    const batch: BufferEntry[] = [];
    let rows = 0;
    while (this.bufferHead < this.buffer.length) {
      const next = this.buffer[this.bufferHead]!;
      if (rows > 0 && rows + next.entries.length > this.flushMaxRows) break;
      this.bufferHead += 1;
      batch.push(next);
      rows += next.entries.length;
      if (rows >= this.flushMaxRows) break;
    }
    if (this.bufferHead === this.buffer.length) {
      this.buffer = [];
      this.bufferHead = 0;
    }
    this.bufferedRows -= rows;
    this.inFlightRows += rows;
    return batch;
  }

  private async writeBatch(batch: BufferEntry[]): Promise<void> {
    let total = 0;
    for (const b of batch) total += b.entries.length;

    const allEntries: ValidatedLogEntry[] = new Array(total);
    let idx = 0;
    for (const b of batch) {
      for (const e of b.entries) allEntries[idx++] = e;
    }

    try {
      try {
        await this.insertViaCopy(allEntries);
      } catch {
        // COPY can fail for reasons that a plain INSERT survives (a broken
        // connection mid-stream, most often). Retry once on the pool before
        // giving up on the batch.
        await this.insertDirect(allEntries);
      }
      this.enqueueRollup(allEntries);
      for (const b of batch) b.resolve();
    } catch (err) {
      const error = err instanceof Error ? err : new Error(String(err));
      for (const b of batch) b.reject(error);
    } finally {
      this.inFlightRows -= total;
    }
  }

  private async acquireCopyClient(): Promise<PoolClient> {
    const pooled = this.idleCopyClients.pop();
    if (pooled) return pooled;
    const client = await this.pool.connect();
    client.on("error", () => {
      // Drop it from the idle set; the next acquire opens a fresh one.
      this.idleCopyClients = this.idleCopyClients.filter((c) => c !== client);
    });
    return client;
  }

  private releaseCopyClient(client: PoolClient, broken: boolean): void {
    if (broken) {
      try {
        client.release(true);
      } catch {
        /* already gone */
      }
      return;
    }
    if (this.idleCopyClients.length < this.maxWriters) {
      this.idleCopyClients.push(client);
    } else {
      client.release();
    }
  }

  /**
   * COPY is 2-5x cheaper than a parameterised INSERT for bulk writes: it skips
   * the parser, planner and executor entirely and streams straight into the
   * heap.
   */
  private async insertViaCopy(entries: ValidatedLogEntry[]): Promise<void> {
    if (entries.length === 0) return;

    const client = await this.acquireCopyClient();
    let broken = false;
    try {
      const parts: string[] = new Array(entries.length);
      for (let i = 0; i < entries.length; i++) {
        const e = entries[i]!;
        parts[i] =
          `${e.tsText}\t${e.level}\t${escapeCopyValue(e.service)}\t` +
          `${escapeCopyValue(e.message)}\t${escapeCopyValue(JSON.stringify(e.attributes))}`;
      }
      const data = parts.join("\n") + "\n";

      const copyStream = client.query(copyFrom(COPY_SQL));
      await new Promise<void>((resolve, reject) => {
        copyStream.on("error", reject);
        copyStream.on("finish", () => resolve());
        // A single write+end beats piping a Readable: no stream machinery, one
        // syscall-sized buffer.
        copyStream.end(data);
      });
    } catch (err) {
      broken = true;
      throw err;
    } finally {
      this.releaseCopyClient(client, broken);
    }
  }

  /** Fallback INSERT via UNNEST, used only when COPY fails. */
  private async insertDirect(entries: ValidatedLogEntry[]): Promise<void> {
    if (entries.length === 0) return;

    const len = entries.length;
    const timestamps: string[] = new Array(len);
    const levels: string[] = new Array(len);
    const services: string[] = new Array(len);
    const messages: string[] = new Array(len);
    const attributes: string[] = new Array(len);

    for (let i = 0; i < len; i++) {
      const e = entries[i]!;
      timestamps[i] = e.tsText;
      levels[i] = e.level;
      services[i] = e.service;
      messages[i] = e.message;
      attributes[i] = JSON.stringify(e.attributes);
    }

    await this.pool.query(
      `INSERT INTO logs ("timestamp", level, service, message, attributes)
       SELECT ts::timestamptz, lvl::log_level, svc, msg, attrs::jsonb
       FROM UNNEST($1::text[], $2::text[], $3::text[], $4::text[], $5::text[])
         AS t(ts, lvl, svc, msg, attrs)`,
      [timestamps, levels, services, messages, attributes],
    );
  }

  // -----------------------------------------------------------------------
  // Rollup maintenance.
  //
  // Counts are accumulated in memory per (minute, service, level) and written
  // every `rollupFlushIntervalMs`. Both tiers are updated in one round trip.
  // The delay is two orders of magnitude below the 20s freshness requirement,
  // and it collapses tens of thousands of rows into a few dozen upserts.
  // -----------------------------------------------------------------------

  private enqueueRollup(entries: ValidatedLogEntry[]): void {
    const q = this.rollupQueue;
    for (const e of entries) {
      const bucketMs = Math.floor(e.tsMs / 60_000) * 60_000;
      const key = `${bucketMs}\t${e.service}\t${e.level}`;
      q.set(key, (q.get(key) ?? 0) + 1);
    }
    this.scheduleRollupFlush();
  }

  private scheduleRollupFlush(): void {
    if (this.rollupTimer) return;
    this.rollupTimer = setTimeout(() => {
      this.rollupTimer = null;
      void this.flushRollup();
    }, this.rollupFlushIntervalMs);
    this.rollupTimer.unref?.();
  }

  /** Serialised flush. The returned promise resolves when this flush is done. */
  private flushRollup(): Promise<void> {
    this.rollupInFlight = this.rollupInFlight.then(() => this.doFlushRollup());
    return this.rollupInFlight;
  }

  private async doFlushRollup(): Promise<void> {
    if (this.rollupQueue.size === 0) return;

    const pending = this.rollupQueue;
    this.rollupQueue = new Map();

    const buckets: string[] = [];
    const services: string[] = [];
    const levels: string[] = [];
    const counts: number[] = [];
    for (const [key, cnt] of pending) {
      const tab1 = key.indexOf("\t");
      const tab2 = key.lastIndexOf("\t");
      buckets.push(new Date(Number(key.slice(0, tab1))).toISOString());
      services.push(key.slice(tab1 + 1, tab2));
      levels.push(key.slice(tab2 + 1));
      counts.push(cnt);
    }

    try {
      await this.pool.query(
        `WITH src AS (
           SELECT b::timestamptz AS bucket_start, s AS service, l::log_level AS level, c AS cnt
           FROM UNNEST($1::text[], $2::text[], $3::text[], $4::bigint[]) AS t(b, s, l, c)
         ), minute_tier AS (
           INSERT INTO logs_rollup (bucket_start, service, level, cnt)
           SELECT bucket_start, service, level, cnt FROM src
           ON CONFLICT (bucket_start, service, level)
           DO UPDATE SET cnt = logs_rollup.cnt + EXCLUDED.cnt
           RETURNING 1
         )
         INSERT INTO logs_rollup_hour (bucket_start, service, level, cnt)
         SELECT date_bin(INTERVAL '1 hour', bucket_start, TIMESTAMPTZ 'epoch'),
                service, level, SUM(cnt)
         FROM src
         GROUP BY 1, 2, 3
         ON CONFLICT (bucket_start, service, level)
         DO UPDATE SET cnt = logs_rollup_hour.cnt + EXCLUDED.cnt`,
        [buckets, services, levels, counts],
      );
    } catch {
      // Merge the counts back so a transient failure does not permanently
      // desynchronise the rollup from `logs`, and try again on the next tick.
      for (const [key, cnt] of pending) {
        this.rollupQueue.set(key, (this.rollupQueue.get(key) ?? 0) + cnt);
      }
      this.scheduleRollupFlush();
    }
  }

  /** Write any accumulated rollup counts now, and wait for them to land. */
  async flushPendingRollup(): Promise<void> {
    if (this.rollupTimer) {
      clearTimeout(this.rollupTimer);
      this.rollupTimer = null;
    }
    await this.flushRollup();
  }

  /** Drain everything. Called on graceful shutdown and by tests. */
  async flushPendingWrites(): Promise<void> {
    this.clearFlushTimer();
    if (this.queuedBatches > 0 || this.activeWriters > 0) {
      await new Promise<void>((resolve) => {
        this.draining.push(resolve);
        this.pump();
      });
    }
    await this.flushPendingRollup();

    for (const client of this.idleCopyClients) {
      try {
        client.release();
      } catch {
        /* already gone */
      }
    }
    this.idleCopyClients = [];
  }

  // =========================================================================
  // Read path
  // =========================================================================

  async query({ filters, limit, cursor }: QueryOptions): Promise<StoredLog[]> {
    const usesGinFilter = filters.q !== undefined || Object.keys(filters.attributes).length > 0;

    // Caller already bounded the scan, or the filters are all btree-indexed:
    // one query, planner picks an ordered index scan.
    if (!usesGinFilter || filters.since !== undefined) {
      return this.runQuery(filters, limit, cursor);
    }

    const anchorMs = cursor
      ? Date.parse(cursor.t)
      : (filters.until?.getTime() ?? Date.now());

    for (const spanMs of PROBE_SPANS_MS) {
      const probe: LogFilters = { ...filters, since: new Date(anchorMs - spanMs) };
      const rows = await this.runQuery(probe, limit, cursor);
      if (rows.length >= limit) return rows;
    }

    // Sparse match set: fall through to the unbounded scan.
    return this.runQuery(filters, limit, cursor);
  }

  private async runQuery(
    filters: LogFilters,
    limit: number,
    cursor: Cursor | null,
  ): Promise<StoredLog[]> {
    const built = buildWhereClause(filters, { cursor });
    const sql = `
      SELECT id::text          AS id,
             "timestamp"       AS timestamp,
             level::text       AS level,
             service,
             message,
             attributes
      FROM logs
      ${built.clause}
      ORDER BY "timestamp" DESC, id DESC
      LIMIT $${built.params.length + 1}
    `;
    const params = [...built.params, limit];
    const { rows } = await this.pool.query<{
      id: string;
      timestamp: Date;
      level: LogLevel;
      service: string;
      message: string;
      attributes: Record<string, string> | null;
    }>(sql, params);
    return rows.map((r) => ({
      id: r.id,
      timestamp: r.timestamp,
      level: r.level,
      service: r.service,
      message: r.message,
      attributes: r.attributes ?? {},
    }));
  }

  // -----------------------------------------------------------------------
  // Aggregation
  // -----------------------------------------------------------------------

  async aggregate(opts: AggregateOptions): Promise<AggregateBucket[]> {
    if (canUseRollup(opts.filters)) {
      return this.aggregateFromRollup(opts);
    }
    return this.aggregateFromRaw(opts);
  }

  /**
   * Fast path. The window is split into rollup-tier segments (see
   * `planRollupSegments`) and each segment is re-binned to the requested
   * bucket size, then summed. A month at `bucket=1h` reads ~17k hourly rows
   * instead of ~1M raw rows.
   */
  private async aggregateFromRollup(opts: AggregateOptions): Promise<AggregateBucket[]> {
    const segments = planRollupSegments(opts.since, opts.until, opts.bucket);
    if (segments.length === 0) return [];

    const interval = BUCKET_TO_INTERVAL[opts.bucket];
    const groupCol = opts.groupBy ? GROUP_TO_COLUMN[opts.groupBy] : null;
    const params: unknown[] = [];

    // Shared filter params come first so every UNION branch can reference the
    // same placeholders.
    let filterSql = "";
    if (opts.filters.service !== undefined) {
      params.push(opts.filters.service);
      filterSql += ` AND service = $${params.length}`;
    }
    if (opts.filters.level !== undefined) {
      params.push(opts.filters.level);
      filterSql += ` AND level = $${params.length}::log_level`;
    }

    const selectExtra = groupCol ? `, ${groupCol} AS grp` : "";
    const innerGroup = groupCol ? `, ${groupCol}` : "";

    const branches = segments.map((seg) => {
      params.push(seg.from);
      const fromIdx = params.length;
      params.push(seg.to);
      const toIdx = params.length;

      const table = SOURCE_TO_TABLE[seg.source];
      const tsCol = seg.source === "raw" ? `"timestamp"` : "bucket_start";
      const cntExpr = seg.source === "raw" ? "COUNT(*)::bigint" : "SUM(cnt)::bigint";

      return `SELECT date_bin(INTERVAL '${interval}', ${tsCol}, TIMESTAMPTZ 'epoch') AS bucket_start${selectExtra},
                     ${cntExpr} AS cnt
              FROM ${table}
              WHERE ${tsCol} >= $${fromIdx} AND ${tsCol} < $${toIdx}${filterSql}
              GROUP BY 1${innerGroup}`;
    });

    const outerGroup = groupCol ? ", grp" : "";
    const sql = `
      WITH parts AS (
        ${branches.join("\n        UNION ALL\n        ")}
      )
      SELECT bucket_start${outerGroup}, SUM(cnt)::bigint AS cnt
      FROM parts
      GROUP BY bucket_start${outerGroup}
      ORDER BY bucket_start ASC${outerGroup}
    `;

    return this.mapAggregateRows(opts.groupBy, sql, params);
  }

  /** Slow path: `q` / `attr.*` filters are not in the rollup, so scan `logs`. */
  private async aggregateFromRaw(opts: AggregateOptions): Promise<AggregateBucket[]> {
    const built = buildWhereClause({ ...opts.filters, since: opts.since, until: opts.until });
    const interval = BUCKET_TO_INTERVAL[opts.bucket];
    const groupCol = opts.groupBy ? GROUP_TO_COLUMN[opts.groupBy] : null;

    // date_bin aligns buckets to fixed epoch-anchored boundaries, so buckets
    // are stable and equal-width regardless of the query window. Much cheaper
    // than date_trunc plus arithmetic per row.
    const bucketExpr = `date_bin(INTERVAL '${interval}', "timestamp", TIMESTAMPTZ 'epoch') AS bucket_start`;

    const selectExtra = groupCol ? `, ${groupCol} AS grp` : "";
    const groupExtra = groupCol ? `, ${groupCol}` : "";
    const orderExtra = groupCol ? ", grp" : "";

    const sql = `
      SELECT ${bucketExpr}${selectExtra}, COUNT(*)::bigint AS cnt
      FROM logs
      ${built.clause}
      GROUP BY bucket_start${groupExtra}
      ORDER BY bucket_start ASC${orderExtra}
    `;

    return this.mapAggregateRows(opts.groupBy, sql, built.params);
  }

  private async mapAggregateRows(
    groupBy: GroupBy | null,
    sql: string,
    params: unknown[],
  ): Promise<AggregateBucket[]> {
    const { rows } = await this.pool.query<{
      bucket_start: Date;
      grp?: string | null;
      cnt: string;
    }>(sql, params);

    return rows.map((r) => ({
      start: r.bucket_start,
      group: groupBy ? (r.grp ?? null) : null,
      count: Number(r.cnt),
    }));
  }
}
