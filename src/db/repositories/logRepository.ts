// Persistence layer for logs. All SQL construction happens here (and in
// buildLogFilters). HTTP handlers should never see raw SQL.

import type { Pool } from "pg";
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

export class LogRepository {
  constructor(private readonly pool: Pool) {}

  /**
   * Batch-insert validated log entries in a single round trip using UNNEST.
   * Inserting one row per statement would collapse throughput; UNNEST lets us
   * ship hundreds of rows with a fixed 6-parameter query shape that Postgres
   * can prepare and cache.
   */
  async insertMany(entries: ValidatedLogEntry[]): Promise<void> {
    if (entries.length === 0) return;

    const timestamps: string[] = new Array(entries.length);
    const levels: string[] = new Array(entries.length);
    const services: string[] = new Array(entries.length);
    const messages: string[] = new Array(entries.length);
    const attributes: string[] = new Array(entries.length);

    for (let i = 0; i < entries.length; i++) {
      const e = entries[i]!;
      timestamps[i] = e.timestamp.toISOString();
      levels[i] = e.level;
      services[i] = e.service;
      messages[i] = e.message;
      attributes[i] = JSON.stringify(e.attributes);
    }

    // One statement, one round trip: insert raw rows and increment the
    // minute rollup from RETURNING. The CTE is a single transaction, so a
    // 200 from POST /logs means both the heap and the rollup committed.
    await this.pool.query(
      `WITH ins AS (
         INSERT INTO logs ("timestamp", level, service, message, attributes)
         SELECT ts::timestamptz, lvl::log_level, svc, msg, attrs::jsonb
         FROM UNNEST($1::text[], $2::text[], $3::text[], $4::text[], $5::text[])
           AS t(ts, lvl, svc, msg, attrs)
         RETURNING "timestamp", level, service
       )
       INSERT INTO logs_rollup (bucket_start, service, level, cnt)
       SELECT date_bin(INTERVAL '1 minute', "timestamp", TIMESTAMPTZ 'epoch'),
              service,
              level,
              COUNT(*)
       FROM ins
       GROUP BY 1, 2, 3
       ON CONFLICT (bucket_start, service, level)
       DO UPDATE SET cnt = logs_rollup.cnt + EXCLUDED.cnt`,
      [timestamps, levels, services, messages, attributes],
    );
  }

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
