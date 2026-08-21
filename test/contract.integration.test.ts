// End-to-end contract test.
//
// Skipped unless TEST_DATABASE_URL is set. In CI we boot a fresh postgres
// service and point this test at it, giving us confidence that the required
// contract works end-to-end (HTTP -> validation -> SQL -> response shape).

import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { Pool } from "pg";
import type { FastifyInstance } from "fastify";
import { runMigrations } from "../src/db/migrator";
import { buildApp } from "../src/app";
import { loadConfig } from "../src/config/env";
import { RetentionWorker } from "../src/retention/retentionWorker";
import { LogRepository } from "../src/db/repositories/logRepository";

const dbUrl = process.env.TEST_DATABASE_URL;
const suite = dbUrl ? describe : describe.skip;

suite("contract (integration)", () => {
  let pool: Pool;
  let app: FastifyInstance;
  let repo: LogRepository;

  beforeAll(async () => {
    process.env.DATABASE_URL = dbUrl!;
    const config = loadConfig();
    pool = new Pool({ connectionString: dbUrl });
    // Fresh schema per run so tests are deterministic.
    await pool.query(`DROP TABLE IF EXISTS logs CASCADE`);
    await pool.query(`DROP TABLE IF EXISTS logs_rollup CASCADE`);
    await pool.query(`DROP TABLE IF EXISTS logs_rollup_hour CASCADE`);
    await pool.query(`DROP TABLE IF EXISTS schema_migrations CASCADE`);
    await pool.query(`DROP TYPE IF EXISTS log_level CASCADE`);
    await pool.query(`DROP SEQUENCE IF EXISTS logs_id_seq CASCADE`);
    await pool.query(`DROP FUNCTION IF EXISTS ensure_log_partition(DATE)`);
    await pool.query(`DROP FUNCTION IF EXISTS ensure_log_partitions(DATE, DATE)`);
    await pool.query(`DROP FUNCTION IF EXISTS drop_log_partitions_before(TIMESTAMPTZ)`);
    await runMigrations(pool);
    const retention = new RetentionWorker(pool, {
      retentionDays: config.retentionDays,
      sweepIntervalMs: 3_600_000,
      partitionLookaheadDays: config.retentionPartitionLookaheadDays,
    });
    await retention.runOnce();
    repo = new LogRepository(pool);
    app = buildApp({ config, pool, repo, isReady: () => true });
    await app.ready();
  });

  afterAll(async () => {
    await app?.close();
    await pool?.end();
  });

  it("GET /health returns 200 with a body", async () => {
    const res = await app.inject({ method: "GET", url: "/health" });
    expect(res.statusCode).toBe(200);
    expect(res.json()).toEqual({ status: "ok" });
  });

  it("POST /logs accepts a batch with mixed valid/invalid entries", async () => {
    const now = new Date().toISOString();
    const res = await app.inject({
      method: "POST",
      url: "/logs",
      payload: {
        logs: [
          { timestamp: now, level: "info", service: "auth", message: "login ok", attributes: { user_id: "1" } },
          { timestamp: now, level: "critical", service: "auth", message: "bad" },
          { timestamp: now, level: "error", service: "checkout", message: "payment declined", attributes: { user_id: "42", region: "eu-west", retries: 3 } },
        ],
      },
    });
    expect(res.statusCode).toBe(200);
    const body = res.json() as { accepted: number; rejected: Array<{ index: number; reason: string }> };
    expect(body.accepted).toBe(2);
    expect(body.rejected).toHaveLength(1);
    expect(body.rejected[0]!.index).toBe(1);
  });

  it("POST /logs rejects malformed JSON with 400", async () => {
    const res = await app.inject({
      method: "POST",
      url: "/logs",
      headers: { "content-type": "application/json" },
      payload: "{not-json",
    });
    expect(res.statusCode).toBe(400);
    expect(res.json()).toHaveProperty("error");
  });

  it("POST /logs returns 400 when top-level shape is wrong", async () => {
    const res = await app.inject({
      method: "POST",
      url: "/logs",
      payload: { entries: [] },
    });
    expect(res.statusCode).toBe(400);
  });

  it("POST /logs returns 400 when all entries invalid", async () => {
    const res = await app.inject({
      method: "POST",
      url: "/logs",
      payload: { logs: [{ level: "bogus" }] },
    });
    expect(res.statusCode).toBe(400);
    const body = res.json() as { accepted: number; rejected: unknown[] };
    expect(body.accepted).toBe(0);
    expect(body.rejected).toHaveLength(1);
  });

  it("GET /logs returns rows sorted by timestamp DESC with deterministic ordering", async () => {
    // Ingest 5 entries with the same timestamp to prove the tiebreaker works.
    const ts = new Date().toISOString();
    const res = await app.inject({
      method: "POST",
      url: "/logs",
      payload: {
        logs: Array.from({ length: 5 }, (_, i) => ({
          timestamp: ts,
          level: "info",
          service: "auth",
          message: `same-ts-${i}`,
          attributes: { seq: String(i) },
        })),
      },
    });
    expect(res.statusCode).toBe(200);

    const list = await app.inject({ method: "GET", url: `/logs?service=auth&limit=2` });
    expect(list.statusCode).toBe(200);
    const body = list.json() as { logs: Array<{ id: string; timestamp: string }>; next_cursor: string | null };
    expect(body.logs).toHaveLength(2);
    expect(BigInt(body.logs[0]!.id)).toBeGreaterThan(BigInt(body.logs[1]!.id));
    expect(body.next_cursor).toBeTypeOf("string");

    const next = await app.inject({ method: "GET", url: `/logs?service=auth&limit=2&cursor=${encodeURIComponent(body.next_cursor!)}` });
    const nextBody = next.json() as { logs: Array<{ id: string }>; next_cursor: string | null };
    expect(nextBody.logs.length).toBeGreaterThan(0);
    // Second page must not include the last row of the first page.
    const firstIds = new Set(body.logs.map((l) => l.id));
    for (const row of nextBody.logs) expect(firstIds.has(row.id)).toBe(false);
  });

  it("GET /logs filters by attributes and q", async () => {
    const ts = new Date().toISOString();
    await app.inject({
      method: "POST",
      url: "/logs",
      payload: {
        logs: [
          { timestamp: ts, level: "error", service: "checkout", message: "payment declined", attributes: { region: "eu-west", user_id: "999" } },
          { timestamp: ts, level: "error", service: "checkout", message: "payment succeeded", attributes: { region: "us-east", user_id: "999" } },
        ],
      },
    });
    const filtered = await app.inject({ method: "GET", url: `/logs?attr.region=eu-west&q=declined` });
    expect(filtered.statusCode).toBe(200);
    const body = filtered.json() as { logs: Array<{ message: string; attributes: Record<string, string> }> };
    expect(body.logs.length).toBeGreaterThanOrEqual(1);
    for (const row of body.logs) {
      expect(row.message.toLowerCase()).toContain("declined");
      expect(row.attributes.region).toBe("eu-west");
    }
  });

  it("GET /logs returns 400 on invalid params", async () => {
    for (const url of [
      "/logs?limit=0",
      "/logs?limit=abc",
      "/logs?level=critical",
      "/logs?since=not-a-date",
      "/logs?cursor=bogus",
      "/logs?since=2026-01-02T00:00:00Z&until=2026-01-01T00:00:00Z",
    ]) {
      const res = await app.inject({ method: "GET", url });
      expect(res.statusCode, url).toBe(400);
      expect(res.json()).toHaveProperty("error");
    }
  });

  it("GET /logs/aggregate returns ascending buckets with counts", async () => {
    const base = new Date();
    base.setUTCSeconds(0, 0);
    const ts1 = new Date(base.getTime() - 90_000).toISOString();
    const ts2 = new Date(base.getTime() - 30_000).toISOString();
    await app.inject({
      method: "POST",
      url: "/logs",
      payload: {
        logs: [
          { timestamp: ts1, level: "info", service: "auth", message: "a" },
          { timestamp: ts1, level: "info", service: "auth", message: "b" },
          { timestamp: ts2, level: "info", service: "checkout", message: "c" },
        ],
      },
    });
    // The rollup is written on a short timer; force it so the assertion below
    // is deterministic instead of racing that timer.
    await repo.flushPendingRollup();

    const since = new Date(base.getTime() - 300_000).toISOString();
    const until = new Date(base.getTime() + 60_000).toISOString();
    const res = await app.inject({
      method: "GET",
      url: `/logs/aggregate?since=${encodeURIComponent(since)}&until=${encodeURIComponent(until)}&bucket=1m&group_by=service`,
    });
    expect(res.statusCode).toBe(200);
    const body = res.json() as { buckets: Array<{ start: string; group: string | null; count: number }> };
    expect(body.buckets.length).toBeGreaterThan(0);
    // Ordered ascending
    for (let i = 1; i < body.buckets.length; i++) {
      expect(Date.parse(body.buckets[i]!.start)).toBeGreaterThanOrEqual(Date.parse(body.buckets[i - 1]!.start));
    }
    // Groups should include only the seeded services
    const groups = new Set(body.buckets.map((b) => b.group));
    for (const g of groups) expect(["auth", "checkout"]).toContain(g);
  });

  it("GET /logs/aggregate with no group_by has null group", async () => {
    await repo.flushPendingRollup();
    const since = new Date(Date.now() - 3_600_000).toISOString();
    const until = new Date(Date.now() + 60_000).toISOString();
    const res = await app.inject({
      method: "GET",
      url: `/logs/aggregate?since=${encodeURIComponent(since)}&until=${encodeURIComponent(until)}&bucket=5m`,
    });
    expect(res.statusCode).toBe(200);
    const body = res.json() as { buckets: Array<{ group: string | null }> };
    for (const b of body.buckets) expect(b.group).toBeNull();
  });

  it("GET /logs/aggregate rollup path matches exact unaligned counts", async () => {
    const base = new Date("2026-03-01T10:00:00.000Z");
    await app.inject({
      method: "POST",
      url: "/logs",
      payload: {
        logs: [
          { timestamp: "2026-03-01T10:00:15.000Z", level: "info", service: "auth", message: "a" },
          { timestamp: "2026-03-01T10:00:15.000Z", level: "info", service: "auth", message: "b" },
          { timestamp: "2026-03-01T10:00:15.000Z", level: "info", service: "auth", message: "c" },
          { timestamp: "2026-03-01T10:01:00.000Z", level: "error", service: "auth", message: "d" },
          { timestamp: "2026-03-01T10:01:00.000Z", level: "error", service: "auth", message: "e" },
          { timestamp: "2026-03-01T10:01:45.000Z", level: "info", service: "checkout", message: "f" },
        ],
      },
    });

    await repo.flushPendingRollup();

    const since = new Date(base.getTime() + 10_000).toISOString();
    const until = new Date(base.getTime() + 120_000).toISOString();
    const res = await app.inject({
      method: "GET",
      url: `/logs/aggregate?since=${encodeURIComponent(since)}&until=${encodeURIComponent(until)}&bucket=1m&group_by=service`,
    });
    expect(res.statusCode).toBe(200);
    const body = res.json() as { buckets: Array<{ start: string; group: string | null; count: number }> };
    const byKey = new Map(body.buckets.map((b) => [`${b.start}|${b.group}`, b.count]));
    expect(byKey.get("2026-03-01T10:00:00Z|auth")).toBe(3);
    expect(byKey.get("2026-03-01T10:01:00Z|auth")).toBe(2);
    expect(byKey.get("2026-03-01T10:01:00Z|checkout")).toBe(1);

    const byLevel = await app.inject({
      method: "GET",
      url: `/logs/aggregate?since=${encodeURIComponent("2026-03-01T10:00:00.000Z")}&until=${encodeURIComponent("2026-03-01T10:02:00.000Z")}&bucket=1m&group_by=level`,
    });
    const levelBody = byLevel.json() as { buckets: Array<{ start: string; group: string | null; count: number }> };
    const levelKey = new Map(levelBody.buckets.map((b) => [`${b.start}|${b.group}`, b.count]));
    expect(levelKey.get("2026-03-01T10:00:00Z|info")).toBe(3);
    expect(levelKey.get("2026-03-01T10:01:00Z|error")).toBe(2);
    expect(levelKey.get("2026-03-01T10:01:00Z|info")).toBe(1);

    const filtered = await app.inject({
      method: "GET",
      url: `/logs/aggregate?since=${encodeURIComponent("2026-03-01T10:00:00.000Z")}&until=${encodeURIComponent("2026-03-01T10:02:00.000Z")}&bucket=1m&service=checkout`,
    });
    const filteredBody = filtered.json() as { buckets: Array<{ count: number; group: string | null }> };
    expect(filteredBody.buckets).toEqual([{ start: "2026-03-01T10:01:00Z", group: null, count: 1 }]);
  });

  it("GET /logs/aggregate rejects invalid params", async () => {
    for (const url of [
      "/logs/aggregate",
      "/logs/aggregate?since=2026-01-01T00:00:00Z",
      "/logs/aggregate?since=2026-01-01T00:00:00Z&until=2026-01-02T00:00:00Z",
      "/logs/aggregate?since=2026-01-01T00:00:00Z&until=2026-01-02T00:00:00Z&bucket=99m",
      "/logs/aggregate?since=2026-01-02T00:00:00Z&until=2026-01-01T00:00:00Z&bucket=1m",
    ]) {
      const res = await app.inject({ method: "GET", url });
      expect(res.statusCode, url).toBe(400);
    }
  });
});
