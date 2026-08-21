// Simple load smoke test used to measure ingest throughput and query latency
// against a running service. Not a benchmark suite - the goal is to produce
// the numbers we cite in the README with a repeatable command.
//
// Usage (in one shell, once docker compose is up):
//   npx tsx scripts/load-smoke.ts \
//     --url http://localhost:8080 \
//     --duration 60 \
//     --batch 500 \
//     --concurrency 8 \
//     --query-rate 1 \
//     --query-mix true

interface Args {
  url: string;
  durationSec: number;
  batchSize: number;
  concurrency: number;
  queryRatePerSec: number;
  targetRatePerSec: number; // 0 = drive at max speed (open loop)
  services: string[];
  queryMix: boolean; // rotate through every query shape, not just the aggregate
}

function parseArgs(): Args {
  const opts: Record<string, string> = {};
  for (let i = 2; i < process.argv.length; i++) {
    const a = process.argv[i]!;
    if (a.startsWith("--")) {
      const key = a.slice(2);
      const value = process.argv[i + 1];
      if (value && !value.startsWith("--")) {
        opts[key] = value;
        i++;
      } else {
        opts[key] = "true";
      }
    }
  }
  return {
    url: opts.url ?? "http://localhost:8080",
    durationSec: Number.parseInt(opts.duration ?? "60", 10),
    batchSize: Number.parseInt(opts.batch ?? "500", 10),
    concurrency: Number.parseInt(opts.concurrency ?? "8", 10),
    queryRatePerSec: Number.parseFloat(opts["query-rate"] ?? "1"),
    targetRatePerSec: Number.parseFloat(opts["target-rate"] ?? "0"),
    services: (opts.services ?? "auth,checkout,orders,catalog,payments,gateway").split(","),
    queryMix: opts["query-mix"] === "true",
  };
}

const LEVELS = ["debug", "info", "warn", "error"] as const;
const REGIONS = ["eu-west", "us-east", "us-west", "ap-south"];

function makeBatch(size: number, services: string[]): unknown {
  const now = Date.now();
  const logs = new Array(size);
  for (let i = 0; i < size; i++) {
    const ts = new Date(now - Math.floor(Math.random() * 10_000)).toISOString();
    logs[i] = {
      timestamp: ts,
      level: LEVELS[Math.floor(Math.random() * LEVELS.length)],
      service: services[Math.floor(Math.random() * services.length)],
      message: Math.random() < 0.02 ? "payment declined by upstream" : `event ${Math.random().toString(36).slice(2)}`,
      attributes: {
        user_id: String(Math.floor(Math.random() * 100_000)),
        region: REGIONS[Math.floor(Math.random() * REGIONS.length)]!,
        retries: Math.floor(Math.random() * 5),
      },
    };
  }
  return { logs };
}

class Histogram {
  private readonly samples: number[] = [];
  add(v: number): void { this.samples.push(v); }
  percentile(p: number): number {
    if (this.samples.length === 0) return 0;
    const sorted = [...this.samples].sort((a, b) => a - b);
    const idx = Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length));
    return sorted[idx]!;
  }
  count(): number { return this.samples.length; }
}

async function ingestLoop(
  args: Args,
  stop: { at: number },
  stats: { accepted: number; rejected: number; requests: number; errors: number; hist: Histogram },
): Promise<void> {
  // When target-rate is set, each worker sends at
  //   target_rate / concurrency  logs/s
  // by sleeping between batches to keep the average rate on target. This
  // simulates a rate-limited load generator (closed loop with think time)
  // rather than an open-loop hammer.
  const workerRate = args.targetRatePerSec > 0 ? args.targetRatePerSec / args.concurrency : 0;
  const batchIntervalMs = workerRate > 0 ? (args.batchSize / workerRate) * 1000 : 0;

  while (Date.now() < stop.at) {
    const startedAt = Date.now();
    const body = makeBatch(args.batchSize, args.services);
    const t0 = performance.now();
    try {
      const res = await fetch(`${args.url}/logs`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(body),
      });
      const dt = performance.now() - t0;
      stats.hist.add(dt);
      stats.requests++;
      if (!res.ok) {
        stats.errors++;
      } else {
        const parsed = (await res.json()) as { accepted: number; rejected: unknown[] };
        stats.accepted += parsed.accepted;
        stats.rejected += parsed.rejected.length;
      }
    } catch {
      stats.errors++;
    }
    if (batchIntervalMs > 0) {
      const elapsed = Date.now() - startedAt;
      if (elapsed < batchIntervalMs) await new Promise((r) => setTimeout(r, batchIntervalMs - elapsed));
    }
  }
}

// The brief asks for query latency percentiles, not just ingest throughput,
// and the interesting cases are the ones that cannot use an ordered btree
// scan. Each shape is timed separately so a single slow path shows up instead
// of being averaged away by the fast ones.
interface QueryShape {
  name: string;
  build: (args: Args) => string;
}

const at = (offsetMs: number) => encodeURIComponent(new Date(Date.now() + offsetMs).toISOString());
const pick = <T,>(xs: readonly T[]): T => xs[Math.floor(Math.random() * xs.length)]!;

const QUERY_SHAPES: QueryShape[] = [
  {
    // The "primary aggregation query" the brief names: 1-hour window, 1m
    // buckets, grouped by service. Served from the minute rollup tier.
    name: "aggregate 1h/1m by service",
    build: (a) =>
      `${a.url}/logs/aggregate?since=${at(-3600_000)}&until=${at(60_000)}&bucket=1m&group_by=service`,
  },
  {
    // Month-wide, hourly buckets. This is what the hourly rollup tier exists
    // for; from the minute tier alone it would have to sum ~1M rows.
    name: "aggregate 30d/1h by level",
    build: (a) =>
      `${a.url}/logs/aggregate?since=${at(-30 * 24 * 3600_000)}&until=${at(60_000)}&bucket=1h&group_by=level`,
  },
  {
    name: "list service+level",
    build: (a) => `${a.url}/logs?service=${pick(a.services)}&level=error&limit=100`,
  },
  {
    // GIN-backed with no `since` - exercises the time-window probe ladder
    // against a filter that matches almost nothing.
    name: "list attr (high card)",
    build: (a) => `${a.url}/logs?attr.user_id=${Math.floor(Math.random() * 100_000)}&limit=100`,
  },
  {
    // Same path, but a filter that matches ~25% of rows.
    name: "list attr (low card)",
    build: (a) => `${a.url}/logs?attr.region=${pick(REGIONS)}&limit=100`,
  },
  {
    name: "list q substring",
    build: (a) => `${a.url}/logs?q=declined&limit=100`,
  },
  {
    name: "list q+attr+service",
    build: (a) =>
      `${a.url}/logs?q=declined&attr.region=${pick(REGIONS)}&service=${pick(a.services)}&limit=100`,
  },
];

interface QueryStats {
  count: number;
  errors: number;
  hist: Histogram;
  perShape: Map<string, Histogram>;
}

async function queryLoop(args: Args, stop: { at: number }, stats: QueryStats): Promise<void> {
  if (args.queryRatePerSec <= 0) return;
  const intervalMs = 1000 / args.queryRatePerSec;
  const shapes = args.queryMix ? QUERY_SHAPES : [QUERY_SHAPES[0]!];
  let i = 0;
  while (Date.now() < stop.at) {
    const start = Date.now();
    const shape = shapes[i++ % shapes.length]!;
    const t0 = performance.now();
    try {
      const res = await fetch(shape.build(args));
      const dt = performance.now() - t0;
      stats.hist.add(dt);
      let perShape = stats.perShape.get(shape.name);
      if (!perShape) {
        perShape = new Histogram();
        stats.perShape.set(shape.name, perShape);
      }
      perShape.add(dt);
      stats.count++;
      if (!res.ok) stats.errors++;
      else await res.arrayBuffer();
    } catch {
      stats.errors++;
    }
    const elapsed = Date.now() - start;
    if (elapsed < intervalMs) await new Promise((r) => setTimeout(r, intervalMs - elapsed));
  }
}

async function main(): Promise<void> {
  const args = parseArgs();
  const stop = { at: Date.now() + args.durationSec * 1000 };
  const ingestStats = { accepted: 0, rejected: 0, requests: 0, errors: 0, hist: new Histogram() };
  const queryStats: QueryStats = {
    count: 0,
    errors: 0,
    hist: new Histogram(),
    perShape: new Map(),
  };

  // Wait for /health.
  const readyDeadline = Date.now() + 60_000;
  while (Date.now() < readyDeadline) {
    try {
      const res = await fetch(`${args.url}/health`);
      if (res.ok) break;
    } catch {
      // ignore
    }
    await new Promise((r) => setTimeout(r, 500));
  }

  console.log(
    `[loadsmoke] url=${args.url} duration=${args.durationSec}s batch=${args.batchSize} concurrency=${args.concurrency} qps=${args.queryRatePerSec} target-rate=${args.targetRatePerSec > 0 ? args.targetRatePerSec + " logs/s" : "max"}`,
  );

  const workers: Promise<void>[] = [];
  for (let i = 0; i < args.concurrency; i++) workers.push(ingestLoop(args, stop, ingestStats));
  workers.push(queryLoop(args, stop, queryStats));

  await Promise.all(workers);

  const seconds = args.durationSec;
  const ingestRate = ingestStats.accepted / seconds;
  console.log("\n=== ingest ===");
  console.log(`accepted=${ingestStats.accepted} rejected=${ingestStats.rejected} requests=${ingestStats.requests} errors=${ingestStats.errors}`);
  console.log(`throughput=${ingestRate.toFixed(1)} logs/s`);
  console.log(`batch latency ms: p50=${ingestStats.hist.percentile(50).toFixed(1)} p95=${ingestStats.hist.percentile(95).toFixed(1)} p99=${ingestStats.hist.percentile(99).toFixed(1)}`);

  console.log("\n=== queries ===");
  console.log(`queries=${queryStats.count} errors=${queryStats.errors}`);
  console.log(
    `overall latency ms: p50=${queryStats.hist.percentile(50).toFixed(1)} p95=${queryStats.hist.percentile(95).toFixed(1)} p99=${queryStats.hist.percentile(99).toFixed(1)}`,
  );
  if (queryStats.perShape.size > 1) {
    console.log(`\n${"per-shape latency ms".padEnd(28)} ${"n".padStart(5)} ${"p50".padStart(8)} ${"p95".padStart(8)} ${"p99".padStart(8)}`);
    for (const [name, h] of queryStats.perShape) {
      console.log(
        `  ${name.padEnd(26)} ${String(h.count()).padStart(5)} ` +
          `${h.percentile(50).toFixed(1).padStart(8)} ${h.percentile(95).toFixed(1).padStart(8)} ${h.percentile(99).toFixed(1).padStart(8)}`,
      );
    }
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
