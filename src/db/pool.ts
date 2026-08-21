import { Pool, PoolConfig } from "pg";
import type { AppConfig } from "../config/env";

export function createPool(config: AppConfig): Pool {
  // Sizing: `ingestWriters` connections are held by COPY streams, one by the
  // rollup upsert, the rest serve reads. Going wider does not help - Postgres
  // has a single vCPU here, so extra backends only add lock and scheduler
  // contention.
  const poolConfig: PoolConfig = {
    connectionString: config.databaseUrl,
    max: Math.max(config.pgPoolMax, config.ingestWriters + 4),
    min: Math.min(4, config.pgPoolMax),
    idleTimeoutMillis: 30_000,
    connectionTimeoutMillis: 10_000,
    application_name: "log-ingestion-service",
    keepAlive: true,
    keepAliveInitialDelayMillis: 5_000,
    statement_timeout: 30_000,  // safety net — fail rather than queue forever
  };
  const pool = new Pool(poolConfig);
  // Do not crash the process on transient client errors from idle connections.
  pool.on("error", (err) => {
    // eslint-disable-next-line no-console
    console.error("[pg] idle client error", err.message);
  });
  return pool;
}
