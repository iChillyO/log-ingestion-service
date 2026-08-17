import type { FastifyPluginAsync } from "fastify";
import type { LogRepository } from "../db/repositories/logRepository";
import { parseAggregateParams } from "./queryParsing";

export interface AggregateDeps {
  repo: LogRepository;
}

// GET /logs/aggregate — time-bucketed counts with optional grouping.
export const aggregateRoutes = (deps: AggregateDeps): FastifyPluginAsync => {
  return async (app) => {
    app.get("/logs/aggregate", async (req) => {
      const parsed = parseAggregateParams(req.query as Record<string, unknown>);
      const buckets = await deps.repo.aggregate({
        filters: parsed.filters,
        since: parsed.since,
        until: parsed.until,
        bucket: parsed.bucket,
        groupBy: parsed.groupBy,
      });
      return {
        buckets: buckets.map((b) => ({
          start: b.start.toISOString(),
          group: b.group,
          count: b.count,
        })),
      };
    });
  };
};
