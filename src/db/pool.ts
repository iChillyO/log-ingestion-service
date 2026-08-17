import { Pool, PoolConfig } from "pg";
import type { AppConfig } from "../config/env";

export function createPool(config: AppConfig): Pool {
  // Keep pool bounded - the 0.5 CPU / 256 MB app container cannot benefit
  // from many parallel Postgres sessions, and Postgres itself is 1 CPU.
  const poolConfig: PoolConfig = {
    connectionString: config.databaseUrl,
    max: 16,
    min: 2,
    idleTimeoutMillis: 30_000,
    connectionTimeoutMillis: 10_000,
    application_name: "log-ingestion-service",
    keepAlive: true,
  };
  const pool = new Pool(poolConfig);
  // Do not crash the process on transient client errors from idle connections.
  pool.on("error", (err) => {
    // eslint-disable-next-line no-console
    console.error("[pg] idle client error", err.message);
  });
  return pool;
}
