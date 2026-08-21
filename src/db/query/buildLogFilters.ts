// Query building for `GET /logs` and `GET /logs/aggregate`.
//
// Every user-supplied value is passed as a parameter ($N). Column names and
// operators are constants that come from validated enums (level, group_by,
// bucket) - never from user input. This keeps the surface safe from SQL
// injection while still letting us build a dynamic WHERE clause.

import type { Cursor } from "../../domain/cursor";
import type { LogLevel } from "../../domain/logSchemas";

export interface LogFilters {
  service?: string;
  level?: LogLevel;
  since?: Date;
  until?: Date;
  attributes: Record<string, string>;
  q?: string;
}

export interface BuiltWhere {
  clause: string; // starts with "WHERE" or is empty
  params: unknown[];
}

function likeEscape(value: string): string {
  // Escape %, _ and \ so that user text is treated literally by ILIKE.
  return value.replace(/([\\%_])/g, "\\$1");
}

export interface WhereOptions {
  cursor?: Cursor | null;
}

export function buildWhereClause(filters: LogFilters, opts: WhereOptions = {}): BuiltWhere {
  const clauses: string[] = [];
  const params: unknown[] = [];
  const add = (sql: string, ...values: unknown[]) => {
    // Replace `?` placeholders with $N-style ordinals.
    let idx = params.length;
    const rendered = sql.replace(/\?/g, () => `$${++idx}`);
    clauses.push(rendered);
    params.push(...values);
  };

  if (filters.since) add(`"timestamp" >= ?`, filters.since);
  if (filters.until) add(`"timestamp" < ?`, filters.until);

  if (filters.service !== undefined) add(`service = ?`, filters.service);
  if (filters.level !== undefined) add(`level = ?::log_level`, filters.level);

  // Attribute equality via a single containment check. All values are strings
  // (coerced at ingest) so the query object matches the stored representation.
  const attrKeys = Object.keys(filters.attributes);
  if (attrKeys.length > 0) {
    const containment: Record<string, string> = {};
    for (const key of attrKeys) containment[key] = filters.attributes[key]!;
    add(`attributes @> ?::jsonb`, JSON.stringify(containment));
  }

  if (filters.q) add(`message ILIKE ? ESCAPE '\\'`, `%${likeEscape(filters.q)}%`);

  if (opts.cursor) {
    // Deterministic secondary ordering by id ensures stable pagination even
    // when many rows share the same timestamp.
    add(`("timestamp", id) < (?, ?::bigint)`, new Date(opts.cursor.t), opts.cursor.i);
  }

  if (clauses.length === 0) return { clause: "", params };
  return { clause: `WHERE ${clauses.join(" AND ")}`, params };
}
