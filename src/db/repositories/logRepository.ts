// Persistence layer for logs. All SQL construction happens here (and in
// buildLogFilters). HTTP handlers should never see raw SQL.

import type { Pool } from "pg";
import { buildWhereClause, type LogFilters } from "../query/buildLogFilters";
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

    await this.pool.query(
      `INSERT INTO logs ("timestamp", level, service, message, attributes)
       SELECT ts::timestamptz, lvl::log_level, svc, msg, attrs::jsonb
       FROM UNNEST($1::text[], $2::text[], $3::text[], $4::text[], $5::text[])
         AS t(ts, lvl, svc, msg, attrs)`,
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

    const { rows } = await this.pool.query<{
      bucket_start: Date;
      grp?: string | null;
      cnt: string;
    }>(sql, built.params);

    return rows.map((r) => ({
      start: r.bucket_start,
      group: opts.groupBy ? (r.grp ?? null) : null,
      count: Number(r.cnt),
    }));
  }
}
