import { Pool, PoolConfig } from "pg";
import type { AppConfig } from "../config/env";

export function createPool(config: AppConfig): Pool {
  // With write-buffering the app issues fewer concurrent queries (large
  // batched INSERTs + occasional reads).  A smaller pool reduces lock
  // contention inside Postgres on the 1-CPU container.
  const poolConfig: PoolConfig = {
    connectionString: config.databaseUrl,
    max: 12,
    min: 2,
    idleTimeoutMillis: 10_000,
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
