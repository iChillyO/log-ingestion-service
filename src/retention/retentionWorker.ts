// Background retention worker.
//
// Runs on an interval and:
//   1. Ensures a daily partition exists for every day inside the retention
//      window, plus `partitionLookaheadDays` into the future.
//
//      Covering the WHOLE window (not just today +/- a day) is the single
//      most important thing this worker does. A backfilled dataset spanning a
//      month would otherwise land almost entirely in `logs_default`, which
//      costs twice:
//        * queries lose partition pruning and scan one undivided heap;
//        * `CREATE TABLE ... PARTITION OF` has to take ACCESS EXCLUSIVE on the
//          parent and scan the default partition to prove no row conflicts, so
//          every later sweep stalls ingestion for as long as that scan takes.
//      Creating the window up front, while `logs_default` is still empty,
//      makes both problems disappear.
//   2. Drops partitions whose upper bound is <= `now - retentionDays`. This is
//      metadata-only and takes negligible time compared to a bulk DELETE.
//   3. As a safety net, bulk-deletes any rows that ended up in the DEFAULT
//      partition (out-of-range timestamps) and are now past the cutoff.
//      Chunked to avoid long-held locks.
//   4. Deletes expired minute-rollup rows so the summary table cannot outlive
//      the partitions it describes.

import type { Pool } from "pg";

export interface RetentionOptions {
  retentionDays: number;
  sweepIntervalMs: number;
  partitionLookaheadDays: number;
  log?: (msg: string, meta?: Record<string, unknown>) => void;
}

export class RetentionWorker {
  private timer: NodeJS.Timeout | null = null;
  private running = false;
  private readonly log: (msg: string, meta?: Record<string, unknown>) => void;

  constructor(
    private readonly pool: Pool,
    private readonly opts: RetentionOptions,
  ) {
    this.log = opts.log ?? (() => {});
  }

  async runOnce(): Promise<void> {
    if (this.running) return;
    this.running = true;
    try {
      await this.ensurePartitions();
      await this.dropExpiredPartitions();
      await this.trimDefaultPartition();
      await this.trimRollup();
    } catch (err) {
      this.log("retention sweep failed", { error: (err as Error).message });
    } finally {
      this.running = false;
    }
  }

  /**
   * Begin periodic sweeps. The caller is expected to `await runOnce()` first,
   * before reporting the service healthy, so this only schedules the interval.
   */
  start(): void {
    if (this.timer) return;
    this.timer = setInterval(() => {
      void this.runOnce();
    }, this.opts.sweepIntervalMs);
    // Do not block process exit on the interval.
    this.timer.unref?.();
  }

  stop(): void {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  private async ensurePartitions(): Promise<void> {
    const now = new Date();
    const utcDay = (offset: number): string =>
      new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() + offset))
        .toISOString()
        .slice(0, 10);

    // One round trip creates every missing day in the window. Days that
    // already exist are skipped inside the function, so steady-state sweeps
    // create at most one new partition.
    const from = utcDay(-this.opts.retentionDays);
    const to = utcDay(this.opts.partitionLookaheadDays);
    const { rows } = await this.pool.query<{ ensure_log_partitions: number }>(
      "SELECT ensure_log_partitions($1::date, $2::date)",
      [from, to],
    );
    const created = rows[0]?.ensure_log_partitions ?? 0;
    if (created > 0) {
      this.log("created partitions", { count: created, from, to });
    }
  }

  private async dropExpiredPartitions(): Promise<void> {
    const cutoff = new Date(Date.now() - this.opts.retentionDays * 24 * 60 * 60 * 1000);
    const { rows } = await this.pool.query<{ drop_log_partitions_before: string }>(
      "SELECT drop_log_partitions_before($1::timestamptz)",
      [cutoff.toISOString()],
    );
    if (rows.length > 0) {
      this.log("dropped partitions", { count: rows.length });
    }
  }

  private async trimDefaultPartition(): Promise<void> {
    const cutoff = new Date(Date.now() - this.opts.retentionDays * 24 * 60 * 60 * 1000);
    // Delete in bounded chunks to avoid a long-running transaction that could
    // block other DDL (partition creation) or hold locks during ingest bursts.
    const CHUNK = 5000;
    for (let i = 0; i < 20; i++) {
      const result = await this.pool.query(
        `DELETE FROM logs_default
         WHERE ctid IN (
           SELECT ctid FROM logs_default WHERE "timestamp" < $1 LIMIT ${CHUNK}
         )`,
        [cutoff.toISOString()],
      );
      if ((result.rowCount ?? 0) < CHUNK) break;
    }
  }

  private async trimRollup(): Promise<void> {
    const cutoff = new Date(Date.now() - this.opts.retentionDays * 24 * 60 * 60 * 1000);
    await this.pool.query("DELETE FROM logs_rollup WHERE bucket_start < $1", [cutoff.toISOString()]);
  }
}
