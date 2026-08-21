import type { FastifyPluginAsync } from "fastify";
import { IngestOverloadedError, type LogRepository } from "../db/repositories/logRepository";
import { validateBatch } from "../domain/logSchemas";
import { BadRequestError, ServiceUnavailableError } from "./errors";

export interface IngestDeps {
  repo: LogRepository;
  maxBatchSize: number;
}

// POST /logs — ingest a batch of log entries with per-entry validation.
//
// Response contract:
//   * 200 with `{accepted, rejected[]}` when at least one entry was accepted
//   * 400 when every entry was rejected, or when the payload shape is wrong
//   * Never 200 with an empty batch we did not durably persist
export const ingestRoutes = (deps: IngestDeps): FastifyPluginAsync => {
  return async (app) => {
    app.post("/logs", async (req, reply) => {
      const body = req.body as unknown;
      if (typeof body !== "object" || body === null || Array.isArray(body)) {
        throw new BadRequestError("request body must be a JSON object with a 'logs' array");
      }
      const logs = (body as { logs?: unknown }).logs;
      if (!Array.isArray(logs)) {
        throw new BadRequestError("'logs' must be an array");
      }
      if (logs.length === 0) {
        throw new BadRequestError("'logs' must contain at least one entry");
      }
      if (logs.length > deps.maxBatchSize) {
        throw new BadRequestError(`batch too large: ${logs.length} > ${deps.maxBatchSize}`);
      }

      const { valid, rejected } = validateBatch(logs);

      if (valid.length === 0) {
        reply.status(400);
        return { accepted: 0, rejected };
      }

      try {
        await deps.repo.insertMany(valid);
      } catch (err) {
        // Bubble up so the Fastify error handler emits a JSON error and
        // (critically) the caller does NOT treat this batch as accepted.
        if (err instanceof IngestOverloadedError) {
          throw new ServiceUnavailableError(err.message, err.retryAfterSeconds);
        }
        req.log.error({ err }, "insert failed");
        throw err;
      }

      return { accepted: valid.length, rejected };
    });
  };
};
