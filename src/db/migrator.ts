import { promises as fs } from "node:fs";
import * as path from "node:path";
import type { Pool } from "pg";

const MIGRATIONS_TABLE = `
CREATE TABLE IF NOT EXISTS schema_migrations (
  id TEXT PRIMARY KEY,
  applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);`;

async function resolveMigrationsDir(): Promise<string> {
  const candidates = [
    path.resolve(process.cwd(), "migrations"),
    path.resolve(__dirname, "../../migrations"),
    path.resolve(__dirname, "../../../migrations"),
  ];
  for (const candidate of candidates) {
    try {
      const stat = await fs.stat(candidate);
      if (stat.isDirectory()) return candidate;
    } catch {
      // ignore and try next
    }
  }
  throw new Error(`Migrations directory not found. Looked in: ${candidates.join(", ")}`);
}

export async function runMigrations(pool: Pool, log: (msg: string) => void = () => {}): Promise<void> {
  const dir = await resolveMigrationsDir();
  const files = (await fs.readdir(dir))
    .filter((f) => f.endsWith(".sql"))
    .sort((a, b) => a.localeCompare(b));

  const client = await pool.connect();
  try {
    await client.query(MIGRATIONS_TABLE);
    const applied = new Set<string>(
      (await client.query<{ id: string }>("SELECT id FROM schema_migrations")).rows.map((r) => r.id),
    );

    for (const file of files) {
      if (applied.has(file)) continue;
      const sql = await fs.readFile(path.join(dir, file), "utf8");
      log(`[migrator] applying ${file}`);
      // Each migration file is executed in a single transaction. If a migration
      // needs CONCURRENTLY it must be split into a non-transactional file - not
      // needed for the initial schema.
      await client.query("BEGIN");
      try {
        await client.query(sql);
        await client.query("INSERT INTO schema_migrations(id) VALUES ($1)", [file]);
        await client.query("COMMIT");
      } catch (err) {
        await client.query("ROLLBACK");
        throw new Error(`Migration ${file} failed: ${(err as Error).message}`);
      }
    }
  } finally {
    client.release();
  }
}
