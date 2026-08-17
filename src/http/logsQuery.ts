import type { FastifyPluginAsync } from "fastify";
import type { LogRepository, StoredLog } from "../db/repositories/logRepository";
import { encodeCursor } from "../domain/cursor";
import { parseListParams } from "./queryParsing";

export interface QueryDeps {
  repo: LogRepository;
}

interface LogResponseEntry {
  id: string;
  timestamp: string;
  level: string;
  service: string;
  message: string;
  attributes: Record<string, string>;
}

function toResponseEntry(row: StoredLog): LogResponseEntry {
  return {
    id: row.id,
    timestamp: row.timestamp.toISOString(),
    level: row.level,
    service: row.service,
    message: row.message,
    attributes: row.attributes,
  };
}

// GET /logs — search and paginate stored logs.
//
// Ordering: `timestamp DESC, id DESC` (deterministic on ties).
// Pagination: opaque cursor encoding (timestamp, id) of the last returned row.
export const queryRoutes = (deps: QueryDeps): FastifyPluginAsync => {
  return async (app) => {
    app.get("/logs", async (req) => {
      const parsed = parseListParams(req.query as Record<string, unknown>);
      const rows = await deps.repo.query({
        filters: parsed.filters,
        limit: parsed.limit,
        cursor: parsed.cursor,
      });
      const items = rows.map(toResponseEntry);
      const nextCursor =
        rows.length === parsed.limit
          ? encodeCursor({ t: rows[rows.length - 1]!.timestamp.toISOString(), i: rows[rows.length - 1]!.id })
          : null;
      return { logs: items, next_cursor: nextCursor };
    });
  };
};
