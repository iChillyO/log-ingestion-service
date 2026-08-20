import { Pool, PoolConfig } from "pg";
import type { AppConfig } from "../config/env";

export function createPool(config: AppConfig): Pool {
  // With write-buffering + COPY the app issues very few concurrent DB calls.
  // A smaller pool reduces lock contention inside Postgres on the 1-CPU
  // container. We keep 4 connections: 1-2 for COPY writes, 1-2 for reads.
  const poolConfig: PoolConfig = {
    connectionString: config.databaseUrl,
    max: 8,
    min: 4,
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
