// Helpers to parse and validate query-string parameters shared between
// `GET /logs` and `GET /logs/aggregate`.

import { BadRequestError } from "./errors";
import { decodeCursor, type Cursor } from "../domain/cursor";
import { LOG_LEVELS, type LogLevel } from "../domain/logSchemas";
import type { BucketSize, GroupBy } from "../db/repositories/logRepository";
import type { LogFilters } from "../db/query/buildLogFilters";

const LEVEL_SET: ReadonlySet<string> = new Set(LOG_LEVELS);
const BUCKET_SET: ReadonlySet<string> = new Set(["1m", "5m", "1h", "1d"]);
const GROUP_SET: ReadonlySet<string> = new Set(["service", "level"]);

function first(value: unknown): string | undefined {
  if (value === undefined || value === null) return undefined;
  if (Array.isArray(value)) {
    // If a param appears twice, take the last non-empty occurrence rather
    // than silently ignoring the client's intent.
    for (let i = value.length - 1; i >= 0; i--) {
      const v = value[i];
      if (typeof v === "string" && v.length > 0) return v;
    }
    return undefined;
  }
  return typeof value === "string" ? value : String(value);
}

function parseIsoTimestamp(raw: string, name: string): Date {
  const ms = Date.parse(raw);
  if (!Number.isFinite(ms)) throw new BadRequestError(`invalid ${name}: '${raw}'`);
  return new Date(ms);
}

function parsePositiveInt(raw: string, name: string, min: number, max: number): number {
  if (!/^\d+$/.test(raw)) throw new BadRequestError(`invalid ${name}: '${raw}'`);
  const n = Number.parseInt(raw, 10);
  if (n < min || n > max) throw new BadRequestError(`${name} must be between ${min} and ${max}`);
  return n;
}

export interface ParsedFilters extends LogFilters {}

export function parseCommonFilters(query: Record<string, unknown>): ParsedFilters {
  const filters: ParsedFilters = { attributes: {} };

  const service = first(query.service);
  if (service !== undefined) {
    if (service.length === 0) throw new BadRequestError("service must be non-empty");
    filters.service = service;
  }

  const level = first(query.level);
  if (level !== undefined) {
    if (!LEVEL_SET.has(level)) throw new BadRequestError(`invalid level: '${level}'`);
    filters.level = level as LogLevel;
  }

  const since = first(query.since);
  if (since !== undefined) filters.since = parseIsoTimestamp(since, "since");

  const until = first(query.until);
  if (until !== undefined) filters.until = parseIsoTimestamp(until, "until");

  if (filters.since && filters.until && filters.until.getTime() < filters.since.getTime()) {
    throw new BadRequestError("until must be greater than or equal to since");
  }

  const q = first(query.q);
  if (q !== undefined) {
    if (q.length === 0) throw new BadRequestError("q must be non-empty");
    if (q.length > 512) throw new BadRequestError("q exceeds maximum length");
    filters.q = q;
  }

  // attr.<key>=<value> - one filter per key.
  for (const key of Object.keys(query)) {
    if (!key.startsWith("attr.")) continue;
    const attrKey = key.slice("attr.".length);
    if (attrKey.length === 0) throw new BadRequestError(`invalid attribute filter: '${key}'`);
    const value = first(query[key]);
    if (value === undefined) throw new BadRequestError(`missing value for '${key}'`);
    filters.attributes[attrKey] = value;
  }

  return filters;
}

export interface ParsedListParams {
  filters: ParsedFilters;
  limit: number;
  cursor: Cursor | null;
}

export function parseListParams(query: Record<string, unknown>): ParsedListParams {
  const filters = parseCommonFilters(query);

  const rawLimit = first(query.limit);
  const limit = rawLimit === undefined ? 100 : parsePositiveInt(rawLimit, "limit", 1, 1000);

  const rawCursor = first(query.cursor);
  let cursor: Cursor | null = null;
  if (rawCursor !== undefined) {
    cursor = decodeCursor(rawCursor);
    if (cursor === null) throw new BadRequestError("invalid cursor");
  }

  return { filters, limit, cursor };
}

export interface ParsedAggregateParams {
  filters: ParsedFilters;
  since: Date;
  until: Date;
  bucket: BucketSize;
  groupBy: GroupBy | null;
}

export function parseAggregateParams(query: Record<string, unknown>): ParsedAggregateParams {
  const filters = parseCommonFilters(query);

  if (!filters.since) throw new BadRequestError("since is required");
  if (!filters.until) throw new BadRequestError("until is required");

  const bucketRaw = first(query.bucket);
  if (bucketRaw === undefined) throw new BadRequestError("bucket is required");
  if (!BUCKET_SET.has(bucketRaw)) throw new BadRequestError(`invalid bucket: '${bucketRaw}'`);
  const bucket = bucketRaw as BucketSize;

  let groupBy: GroupBy | null = null;
  const groupRaw = first(query.group_by);
  if (groupRaw !== undefined) {
    if (!GROUP_SET.has(groupRaw)) throw new BadRequestError(`invalid group_by: '${groupRaw}'`);
    groupBy = groupRaw as GroupBy;
  }

  // Cap the aggregation window relative to bucket size to keep p95 latency low.
  // We do not fail closed here; we simply refuse absurdly large windows that
  // would materialise millions of buckets.
  const windowMs = filters.until!.getTime() - filters.since!.getTime();
  const maxBuckets = 100_000;
  const bucketMs: Record<BucketSize, number> = {
    "1m": 60_000,
    "5m": 5 * 60_000,
    "1h": 60 * 60_000,
    "1d": 24 * 60 * 60_000,
  };
  if (windowMs / bucketMs[bucket] > maxBuckets) {
    throw new BadRequestError(`aggregation window produces too many buckets for bucket=${bucket}`);
  }

  return {
    filters,
    since: filters.since!,
    until: filters.until!,
    bucket,
    groupBy,
  };
}
