// Planning for the pre-aggregated rollup tiers behind `GET /logs/aggregate`.
//
// There are three sources of counts, from cheapest to most expensive:
//
//   logs_rollup_hour  (hour, service, level) -> cnt   ~17k rows / month
//   logs_rollup       (min,  service, level) -> cnt   ~1M   rows / month
//   logs                                              ~1M   rows / month
//
// A request window almost never lines up with a tier boundary, so we split it
// into segments: the largest possible aligned core read from the coarsest tier,
// then progressively finer tiers for the leftover edges, and finally `logs`
// for the sub-minute remainder. Counts are exact - no tier is an estimate.
//
//   since ........................................................ until
//   |--raw--|--minute--|------------ hour ------------|--minute--|--raw--|

import type { LogFilters } from "./buildLogFilters";

export const ROLLUP_MINUTE_MS = 60_000;
export const ROLLUP_HOUR_MS = 3_600_000;

export type RollupSource = "hour" | "minute" | "raw";

export interface RollupSegment {
  source: RollupSource;
  from: Date;
  to: Date;
}

/**
 * The rollup tiers are keyed on (bucket, service, level) only. Anything that
 * filters on message text or attributes has to go to `logs`.
 */
export function canUseRollup(filters: LogFilters): boolean {
  return !filters.q && Object.keys(filters.attributes).length === 0;
}

/**
 * Coarsest tier whose granularity divides the requested bucket. Using a tier
 * that does not divide the bucket would misattribute counts at bucket edges.
 */
export function coarsestTierFor(bucket: "1m" | "5m" | "1h" | "1d"): RollupSource {
  return bucket === "1h" || bucket === "1d" ? "hour" : "minute";
}

/**
 * Split [since, until) into tier segments, coarsest first.
 *
 * The recursion is only two levels deep (hour -> minute -> raw), so the worst
 * case is 1 hour segment + 2 minute segments + 4 raw segments, and every raw
 * segment covers strictly less than one minute.
 */
export function planRollupSegments(
  since: Date,
  until: Date,
  bucket: "1m" | "5m" | "1h" | "1d",
): RollupSegment[] {
  const tiers: Array<{ source: RollupSource; granularityMs: number }> =
    coarsestTierFor(bucket) === "hour"
      ? [
          { source: "hour", granularityMs: ROLLUP_HOUR_MS },
          { source: "minute", granularityMs: ROLLUP_MINUTE_MS },
        ]
      : [{ source: "minute", granularityMs: ROLLUP_MINUTE_MS }];

  const segments: RollupSegment[] = [];
  split(since.getTime(), until.getTime(), tiers, 0, segments);
  return segments;
}

function split(
  fromMs: number,
  toMs: number,
  tiers: Array<{ source: RollupSource; granularityMs: number }>,
  tierIndex: number,
  out: RollupSegment[],
): void {
  if (fromMs >= toMs) return;

  const tier = tiers[tierIndex];
  if (!tier) {
    out.push({ source: "raw", from: new Date(fromMs), to: new Date(toMs) });
    return;
  }

  const g = tier.granularityMs;
  const alignedFrom = Math.ceil(fromMs / g) * g;
  const alignedTo = Math.floor(toMs / g) * g;

  if (alignedFrom >= alignedTo) {
    // The window does not contain a single whole bucket at this granularity;
    // hand the whole thing to the next finer tier.
    split(fromMs, toMs, tiers, tierIndex + 1, out);
    return;
  }

  split(fromMs, alignedFrom, tiers, tierIndex + 1, out);
  out.push({ source: tier.source, from: new Date(alignedFrom), to: new Date(alignedTo) });
  split(alignedTo, toMs, tiers, tierIndex + 1, out);
}

// ---------------------------------------------------------------------------
// Legacy single-tier helper, kept because it is the clearest expression of the
// alignment rule and is unit-tested independently of the segment planner.
// ---------------------------------------------------------------------------

export interface RollupWindow {
  rollupFrom: Date;
  rollupTo: Date;
  edge1From: Date;
  edge1To: Date;
  edge2From: Date;
  edge2To: Date;
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
