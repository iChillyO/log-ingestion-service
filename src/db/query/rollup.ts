// Helpers for the minute-level logs_rollup fast path.
//
// Rollup rows are UTC-minute floors (date_bin 1 minute from epoch). A query
// window that is not minute-aligned has leftover seconds at each edge; those
// must be read from `logs` or counts would be wrong.

import type { LogFilters } from "./buildLogFilters";

export const ROLLUP_MINUTE_MS = 60_000;

export interface RollupWindow {
  rollupFrom: Date;
  rollupTo: Date;
  edge1From: Date;
  edge1To: Date;
  edge2From: Date;
  edge2To: Date;
}

export function canUseRollup(filters: LogFilters): boolean {
  return !filters.q && Object.keys(filters.attributes).length === 0;
}

export function rollupWindow(since: Date, until: Date): RollupWindow {
  const sinceMs = since.getTime();
  const untilMs = until.getTime();
  const alignedSince = Math.ceil(sinceMs / ROLLUP_MINUTE_MS) * ROLLUP_MINUTE_MS;
  const alignedUntil = Math.floor(untilMs / ROLLUP_MINUTE_MS) * ROLLUP_MINUTE_MS;

  if (alignedSince < alignedUntil) {
    return {
      rollupFrom: new Date(alignedSince),
      rollupTo: new Date(alignedUntil),
      edge1From: since,
      edge1To: sinceMs < alignedSince ? new Date(alignedSince) : since,
      edge2From: untilMs > alignedUntil ? new Date(alignedUntil) : until,
      edge2To: until,
    };
  }

  return {
    rollupFrom: until,
    rollupTo: until,
    edge1From: since,
    edge1To: until,
    edge2From: until,
    edge2To: until,
  };
}
