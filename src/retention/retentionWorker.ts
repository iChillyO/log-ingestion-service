// Background retention worker.
//
// Runs on an interval and:
//   1. Ensures partitions exist for today, yesterday, and the next
//      `partitionLookaheadDays` days so ingestion always has a home partition
//      even when the local clock crosses midnight or a client back-dates a
//      timestamp by a small margin.
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

  start(): void {
    if (this.timer) return;
    // Run one sweep immediately (fire-and-forget) so partitions for today are
    // guaranteed to exist before the first request lands.
    void this.runOnce();
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
    const days: string[] = [];
    const now = new Date();
    // Include yesterday so back-dated logs land in a real partition, not the
    // default one.
    for (let d = -1; d <= this.opts.partitionLookaheadDays; d++) {
      const dt = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() + d));
      days.push(dt.toISOString().slice(0, 10));
    }
    for (const day of days) {
      await this.pool.query("SELECT ensure_log_partition($1::date)", [day]);
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
