// Persistence layer for logs. All SQL construction happens here (and in
// buildLogFilters). HTTP handlers should never see raw SQL.

import type { Pool, PoolClient } from "pg";
import { from as copyFrom } from "pg-copy-streams";
import { buildWhereClause, type LogFilters } from "../query/buildLogFilters";
import { canUseRollup, rollupWindow } from "../query/rollup";
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

// ---------------------------------------------------------------------------
// Write buffer configuration.
//
// The single biggest perf win: coalesce many small HTTP batches (each 30-50
// rows) into fewer, larger Postgres writes.  At 15k logs/s arriving as
// ~300-500 requests/s, each individual INSERT has fixed planning + WAL
// overhead.  Buffering reduces write frequency to ~30/s with 500+ row arrays
// which cuts Postgres CPU dramatically.
//
// Safety: the HTTP handler `await repo.insertMany(...)` does NOT resolve until
// the batch has been flushed to Postgres, so 200 is never returned to the
// client for data that isn't persisted.
// ---------------------------------------------------------------------------

interface BufferEntry {
  entries: ValidatedLogEntry[];
  resolve: () => void;
  reject: (err: Error) => void;
}

const FLUSH_INTERVAL_MS = 20;    // flush at most every 20ms
const FLUSH_MAX_SIZE   = 8_000;  // flush immediately when we have this many rows

// COPY TEXT format escaping: backslash, tab, newline, carriage return
// Use a regex to detect if escaping is needed (branch prediction friendly)
const NEEDS_ESCAPE = /[\\\t\n\r]/;
function escapeCopyValue(s: string): string {
  if (!NEEDS_ESCAPE.test(s)) return s;
  return s.replace(/\\/g, "\\\\").replace(/\t/g, "\\t").replace(/\n/g, "\\n").replace(/\r/g, "\\r");
}

const COPY_SQL = `COPY logs ("timestamp", level, service, message, attributes) FROM STDIN WITH (FORMAT text)`;

export class LogRepository {
  private rollupQueue: Map<string, number> = new Map();
  private rollupTimer: ReturnType<typeof setTimeout> | null = null;

  // Write buffer
  private buffer: BufferEntry[] = [];
  private bufferCount = 0;
  private flushTimer: ReturnType<typeof setTimeout> | null = null;
  private flushing = false;

  // Dedicated connection for COPY writes — avoids pool checkout overhead
  // and keeps the connection warm with pg's internal caches.
  private copyClient: PoolClient | null = null;

  constructor(private readonly pool: Pool) {}

  // -----------------------------------------------------------------------
  // insertMany — public API.  Entries are placed into an in-memory buffer
  // and the returned Promise resolves only after they are flushed to PG.
  // -----------------------------------------------------------------------
  async insertMany(entries: ValidatedLogEntry[]): Promise<void> {
    if (entries.length === 0) return;

    return new Promise<void>((resolve, reject) => {
      this.buffer.push({ entries, resolve, reject });
      this.bufferCount += entries.length;

      if (this.bufferCount >= FLUSH_MAX_SIZE) {
        this.triggerFlush();
      } else if (!this.flushTimer) {
        this.flushTimer = setTimeout(() => this.triggerFlush(), FLUSH_INTERVAL_MS);
      }
    });
  }

  private triggerFlush(): void {
    if (this.flushTimer) {
      clearTimeout(this.flushTimer);
      this.flushTimer = null;
    }
    if (this.flushing) return;
    void this.flushBuffer();
  }

  private async flushBuffer(): Promise<void> {
    if (this.buffer.length === 0) return;
    this.flushing = true;

    // Swap buffer atomically
    const batch = this.buffer;
    this.buffer = [];
    this.bufferCount = 0;

    // Flatten all entries from all waiting callers
    let total = 0;
    for (const b of batch) total += b.entries.length;
    const allEntries: ValidatedLogEntry[] = new Array(total);
    let idx = 0;
    for (const b of batch) {
      for (const e of b.entries) {
        allEntries[idx++] = e;
      }
    }

    try {
      await this.insertViaCopy(allEntries);
      for (const b of batch) b.resolve();
    } catch (err) {
      // If COPY fails, try UNNEST fallback once
      try {
        await this.insertDirect(allEntries);
        for (const b of batch) b.resolve();
      } catch (err2) {
        const error = err2 instanceof Error ? err2 : new Error(String(err2));
        for (const b of batch) b.reject(error);
      }
    } finally {
      this.flushing = false;
      if (this.buffer.length > 0) {
        void this.flushBuffer();
      }
    }
  }

  // -----------------------------------------------------------------------
  // Get or create a dedicated COPY client from the pool.
  // -----------------------------------------------------------------------
  private async getCopyClient(): Promise<PoolClient> {
    if (this.copyClient) return this.copyClient;
    const client = await this.pool.connect();
    this.copyClient = client;
    // If the dedicated client errors out, release it so next flush gets a new one.
    client.on("error", () => {
      this.copyClient = null;
      try { client.release(true); } catch { /* ignore */ }
    });
    return client;
  }

  // -----------------------------------------------------------------------
  // insertViaCopy — actual DB write using COPY protocol.
  // COPY is 2-5x faster than INSERT with UNNEST because it bypasses the
  // SQL parser, planner, and executor overhead entirely.
  // -----------------------------------------------------------------------
  private async insertViaCopy(entries: ValidatedLogEntry[]): Promise<void> {
    if (entries.length === 0) return;

    let client: PoolClient;
    try {
      client = await this.getCopyClient();
    } catch {
      // Fallback to pool if dedicated client fails
      return this.insertDirect(entries);
    }

    try {
      // Build the COPY data as a single string buffer for efficiency.
      const parts: string[] = new Array(entries.length);
      for (let i = 0; i < entries.length; i++) {
        const e = entries[i]!;
        parts[i] = `${e.timestamp.toISOString()}\t${e.level}\t${escapeCopyValue(e.service)}\t${escapeCopyValue(e.message)}\t${escapeCopyValue(JSON.stringify(e.attributes))}`;
      }
      const data = parts.join("\n") + "\n";

      const copyStream = client.query(copyFrom(COPY_SQL));

      // Direct write+end is faster than pipeline for a single buffer —
      // avoids Readable creation and stream machinery overhead.
      await new Promise<void>((resolve, reject) => {
        copyStream.on("error", reject);
        copyStream.on("finish", resolve);
        copyStream.end(data);
      });

      this.enqueueRollup(entries);
    } catch (err) {
      // Release the broken client and null it out
      this.copyClient = null;
      try { client.release(true); } catch { /* ignore */ }
      throw err;
    }
  }

  // -----------------------------------------------------------------------
  // Fallback INSERT via UNNEST — used if COPY fails
  // -----------------------------------------------------------------------
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
      timestamps[i] = e.timestamp.toISOString();
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

    this.enqueueRollup(entries);
  }

  // -----------------------------------------------------------------------
  // Rollup helpers — accumulate minute-level counts and flush periodically.
  // Delay is longer than the write buffer (200ms vs 20ms) because rollup
  // is less latency-sensitive and we want to coalesce more.
  // Uses ms-based keys to avoid Date object creation on the hot path.
  // -----------------------------------------------------------------------
  private enqueueRollup(entries: ValidatedLogEntry[]): void {
    for (const e of entries) {
      // Use milliseconds as the bucket key to avoid Date/toISOString per row
      const bucketMs = Math.floor(e.timestamp.getTime() / 60_000) * 60_000;
      const key = `${bucketMs}\t${e.service}\t${e.level}`;
      this.rollupQueue.set(key, (this.rollupQueue.get(key) ?? 0) + 1);
    }
    if (!this.rollupTimer) {
      this.rollupTimer = setTimeout(() => void this.flushRollup(), 200);
    }
  }

  private async flushRollup(): Promise<void> {
    this.rollupTimer = null;
    const q = this.rollupQueue;
    if (q.size === 0) return;
    this.rollupQueue = new Map();

    const buckets: string[] = [];
    const services: string[] = [];
    const levels: string[] = [];
    const counts: number[] = [];
    for (const [key, cnt] of q) {
      const [b, s, l] = key.split("\t");
      buckets.push(new Date(Number(b!)).toISOString());
      services.push(s!);
      levels.push(l!);
      counts.push(cnt);
    }

    try {
      await this.pool.query(
        `INSERT INTO logs_rollup (bucket_start, service, level, cnt)
         SELECT b::timestamptz, s, l::log_level, c
         FROM UNNEST($1::text[], $2::text[], $3::text[], $4::bigint[])
           AS t(b, s, l, c)
         ON CONFLICT (bucket_start, service, level)
         DO UPDATE SET cnt = logs_rollup.cnt + EXCLUDED.cnt`,
        [buckets, services, levels, counts],
      );
    } catch {
      // Rollup failure is non-fatal; aggregation falls back to raw table
    }
  }

  async flushPendingRollup(): Promise<void> {
    if (this.rollupTimer) {
      clearTimeout(this.rollupTimer);
      this.rollupTimer = null;
    }
    await this.flushRollup();
  }

  // -----------------------------------------------------------------------
  // Flush the write buffer — called during graceful shutdown to ensure all
  // accepted logs are persisted before the process exits.
  // -----------------------------------------------------------------------
  async flushPendingWrites(): Promise<void> {
    if (this.flushTimer) {
      clearTimeout(this.flushTimer);
      this.flushTimer = null;
    }
    await this.flushBuffer();
    await this.flushPendingRollup();
    // Release the dedicated COPY client
    if (this.copyClient) {
      try { this.copyClient.release(); } catch { /* ignore */ }
      this.copyClient = null;
    }
  }

  // -----------------------------------------------------------------------
  // Query path — read stored logs with filtering and cursor pagination.
  // -----------------------------------------------------------------------
  async query({ filters, limit, cursor }: QueryOptions): Promise<StoredLog[]> {
    const built = buildWhereClause(filters, { cursor, includeIngestionCeiling: true });
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
  // Aggregate path
  // -----------------------------------------------------------------------
  async aggregate(opts: AggregateOptions): Promise<AggregateBucket[]> {
    if (canUseRollup(opts.filters)) {
      return this.aggregateFromRollup(opts);
    }
    return this.aggregateFromRaw(opts);
  }

  /**
   * Fast path: SUM pre-aggregated minute counts, then re-bin to the requested
   * bucket. Partial minutes at the since/until edges are filled from `logs`
   * so unaligned windows stay exact.
   */
  private async aggregateFromRollup(opts: AggregateOptions): Promise<AggregateBucket[]> {
    const interval = BUCKET_TO_INTERVAL[opts.bucket];
    const groupCol = opts.groupBy ? GROUP_TO_COLUMN[opts.groupBy] : null;
    const win = rollupWindow(opts.since, opts.until);

    const params: unknown[] = [
      win.rollupFrom,
      win.rollupTo,
      win.edge1From,
      win.edge1To,
      win.edge2From,
      win.edge2To,
    ];
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
    const outerGroup = groupCol ? `, grp` : "";
    const bucketRollup = `date_bin(INTERVAL '${interval}', bucket_start, TIMESTAMPTZ 'epoch')`;
    const bucketRaw = `date_bin(INTERVAL '${interval}', "timestamp", TIMESTAMPTZ 'epoch')`;

    const sql = `
      WITH parts AS (
        SELECT ${bucketRollup} AS bucket_start${selectExtra}, SUM(cnt)::bigint AS cnt
        FROM logs_rollup
        WHERE bucket_start >= $1 AND bucket_start < $2
          ${filterSql}
        GROUP BY 1${innerGroup}
        UNION ALL
        SELECT ${bucketRaw} AS bucket_start${selectExtra}, COUNT(*)::bigint AS cnt
        FROM logs
        WHERE (
          ("timestamp" >= $3 AND "timestamp" < $4)
          OR ("timestamp" >= $5 AND "timestamp" < $6)
        )
          ${filterSql}
        GROUP BY 1${innerGroup}
      )
      SELECT bucket_start${groupCol ? ", grp" : ""}, SUM(cnt)::bigint AS cnt
      FROM parts
      GROUP BY bucket_start${outerGroup}
      ORDER BY bucket_start ASC${outerGroup}
    `;

    return this.mapAggregateRows(opts.groupBy, sql, params);
  }

  private async aggregateFromRaw(opts: AggregateOptions): Promise<AggregateBucket[]> {
    const built = buildWhereClause(
      { ...opts.filters, since: opts.since, until: opts.until },
      { includeIngestionCeiling: false },
    );
    const interval = BUCKET_TO_INTERVAL[opts.bucket];
    const groupCol = opts.groupBy ? GROUP_TO_COLUMN[opts.groupBy] : null;

    // date_bin lets us align buckets to fixed calendar boundaries anchored on
    // the epoch, so buckets are stable and equal-width regardless of the query
    // window. This is much cheaper than date_trunc + arithmetic per row.
    const bucketExpr = `date_bin(INTERVAL '${interval}', "timestamp", TIMESTAMPTZ 'epoch') AS bucket_start`;

    let selectExtra = "";
    let groupExtra = "";
    let orderExtra = "";
    if (groupCol) {
      selectExtra = `, ${groupCol} AS grp`;
      groupExtra = `, ${groupCol}`;
      orderExtra = `, grp`;
    }

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
