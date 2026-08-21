import { loadConfig } from "./config/env";
import { createPool } from "./db/pool";
import { runMigrations } from "./db/migrator";
import { RetentionWorker } from "./retention/retentionWorker";
import { buildApp } from "./app";
import { LogRepository } from "./db/repositories/logRepository";

async function main(): Promise<void> {
  const config = loadConfig();
  const pool = createPool(config);
  const repo = new LogRepository(pool, {
    flushIntervalMs: config.ingestFlushIntervalMs,
    flushMaxRows: config.ingestFlushMaxRows,
    writers: config.ingestWriters,
    maxBufferedRows: config.ingestMaxBufferedRows,
    rollupFlushIntervalMs: config.rollupFlushIntervalMs,
  });

  let ready = false;
  const app = buildApp({ config, pool, repo, isReady: () => ready });

  // Start listening BEFORE migrations complete so /health can report
  // "starting" (503) rather than the socket being closed. The contract only
  // requires 200 once fully ready.
  await app.listen({ host: "0.0.0.0", port: config.port });
  app.log.info(`listening on :${config.port}`);

  try {
    await waitForDatabase(pool, app.log);
    await runMigrations(pool, (msg) => app.log.info(msg));
  } catch (err) {
    app.log.error({ err }, "startup failed");
    await app.close();
    process.exit(1);
  }

  const retention = new RetentionWorker(pool, {
    retentionDays: config.retentionDays,
    sweepIntervalMs: config.retentionSweepIntervalMs,
    partitionLookaheadDays: config.retentionPartitionLookaheadDays,
    log: (msg, meta) => app.log.info(meta ?? {}, msg),
  });

  // Await the first sweep instead of firing it off in the background. It
  // materialises the whole retention window of daily partitions, and doing
  // that while `logs_default` is empty is the difference between an instant
  // metadata-only CREATE TABLE and one that has to scan the default partition
  // under ACCESS EXCLUSIVE. We must not report healthy - and start taking
  // writes - until the partitions those writes belong in exist.
  try {
    await retention.runOnce();
  } catch (err) {
    app.log.error({ err }, "initial retention sweep failed");
    await app.close();
    process.exit(1);
  }
  retention.start();

  // Warm up pool connections and run ANALYZE for optimal query plans.
  // This ensures the planner has accurate statistics from the start.
  try {
    await warmupPool(pool, config);
  } catch (err) {
    app.log.warn({ err }, "pool warmup failed (non-fatal)");
  }

  ready = true;
  app.log.info("service is ready");

  const shutdown = async (signal: string) => {
    app.log.info(`received ${signal}, shutting down`);
    ready = false;
    retention.stop();
    try {
      // Flush any buffered writes before closing connections
      await repo.flushPendingWrites();
    } catch (err) {
      app.log.warn({ err }, "error flushing pending writes");
    }
    try {
      await app.close();
    } catch (err) {
      app.log.warn({ err }, "error closing fastify");
    }
    try {
      await pool.end();
    } catch (err) {
      app.log.warn({ err }, "error closing pg pool");
    }
    process.exit(0);
  };
  process.on("SIGINT", () => void shutdown("SIGINT"));
  process.on("SIGTERM", () => void shutdown("SIGTERM"));
}

async function waitForDatabase(pool: import("pg").Pool, log: { info: (msg: string) => void }): Promise<void> {
  const deadline = Date.now() + 60_000;
  let lastError: unknown = null;
  while (Date.now() < deadline) {
    try {
      const client = await pool.connect();
      try {
        await client.query("SELECT 1");
      } finally {
        client.release();
      }
      return;
    } catch (err) {
      lastError = err;
      log.info("waiting for database...");
      await new Promise((r) => setTimeout(r, 1000));
    }
  }
  throw new Error(`database not reachable within 60s: ${(lastError as Error)?.message}`);
}

async function warmupPool(pool: import("pg").Pool, config: import("./config/env").AppConfig): Promise<void> {
  // Pre-create pool connections in parallel
  const minConns = Math.min(config.ingestWriters + 2, config.pgPoolMax);
  const warmupPromises: Promise<void>[] = [];
  for (let i = 0; i < minConns; i++) {
    warmupPromises.push(
      pool.connect().then((client) => {
        client.release();
      })
    );
  }
  await Promise.all(warmupPromises);

  // Run ANALYZE on key tables so the planner has fresh statistics
  await pool.query("ANALYZE logs");
  await pool.query("ANALYZE logs_rollup");
  await pool.query("ANALYZE logs_rollup_hour");
}

main().catch((err) => {
  // eslint-disable-next-line no-console
  console.error("fatal error starting service", err);
  process.exit(1);
});
