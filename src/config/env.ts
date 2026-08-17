import "dotenv/config";

export interface AppConfig {
  port: number;
  logLevel: string;
  databaseUrl: string;
  ingestMaxBatch: number;
  ingestMaxBodyBytes: number;
  retentionDays: number;
  retentionSweepIntervalMs: number;
  retentionPartitionLookaheadDays: number;
  authEnabled: boolean;
  loadgenApiKey: string | null;
}

function readInt(name: string, fallback: number, min = 1, max = Number.MAX_SAFE_INTEGER): number {
  const raw = process.env[name];
  if (raw === undefined || raw === "") return fallback;
  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed) || parsed < min || parsed > max) {
    throw new Error(`Invalid ${name}=${raw}: expected integer between ${min} and ${max}`);
  }
  return parsed;
}

function readBool(name: string, fallback: boolean): boolean {
  const raw = process.env[name];
  if (raw === undefined || raw === "") return fallback;
  return /^(1|true|yes|on)$/i.test(raw);
}

export function loadConfig(): AppConfig {
  const databaseUrl = process.env.DATABASE_URL ?? "postgres://logs:logs@localhost:5432/logs";
  return {
    port: readInt("PORT", 8080, 1, 65535),
    logLevel: process.env.LOG_LEVEL ?? "info",
    databaseUrl,
    // Guardrails, not part of the required contract - the API accepts any batch size
    // but we cap raw JSON body size to protect the 256 MB container from OOM.
    ingestMaxBatch: readInt("INGEST_MAX_BATCH", 10_000, 1, 100_000),
    ingestMaxBodyBytes: readInt("INGEST_MAX_BODY_BYTES", 5 * 1024 * 1024, 1024, 128 * 1024 * 1024),
    retentionDays: readInt("RETENTION_DAYS", 30, 1, 3650),
    retentionSweepIntervalMs: readInt("RETENTION_SWEEP_INTERVAL_MS", 5 * 60 * 1000, 10_000, 24 * 60 * 60 * 1000),
    retentionPartitionLookaheadDays: readInt("RETENTION_PARTITION_LOOKAHEAD_DAYS", 2, 1, 30),
    authEnabled: readBool("AUTH_ENABLED", false),
    loadgenApiKey: process.env.LOADGEN_API_KEY?.trim() || null,
  };
}
