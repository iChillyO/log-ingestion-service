import Fastify, { type FastifyInstance } from "fastify";
import type { Pool } from "pg";
import type { AppConfig } from "./config/env";
import type { LogRepository } from "./db/repositories/logRepository";
import { healthRoutes } from "./http/health";
import { ingestRoutes } from "./http/logsIngest";
import { queryRoutes } from "./http/logsQuery";
import { aggregateRoutes } from "./http/logsAggregate";
import { BadRequestError, ForbiddenError, UnauthorizedError } from "./http/errors";

export interface AppDeps {
  config: AppConfig;
  pool: Pool;
  repo: LogRepository;
  isReady: () => boolean;
}

export function buildApp(deps: AppDeps): FastifyInstance {
  const app = Fastify({
    logger: {
      level: deps.config.logLevel,
    },
    bodyLimit: deps.config.ingestMaxBodyBytes,
    // Silence per-request access logs on the hot ingest path - too noisy at
    // 15k+ rps and pino serialization is non-trivial CPU cost on 0.5 vCPU.
    // Deprecated in Fastify 5 in favour of `logController.disableRequestLogging`
    // but still works; we'll follow up when we upgrade to Fastify 6.
    disableRequestLogging: true,
  });

  const repo = deps.repo;

  app.setErrorHandler((err, req, reply) => {
    if (err instanceof BadRequestError) {
      reply.status(400).send({ error: err.message });
      return;
    }
    if (err instanceof UnauthorizedError) {
      reply.status(401).send({ error: err.message });
      return;
    }
    if (err instanceof ForbiddenError) {
      reply.status(403).send({ error: err.message });
      return;
    }
    // Fastify's built-in validation error path (e.g. body too large, malformed
    // JSON) gets mapped to a 400 with a clean error field.
    const e = err as { statusCode?: number; code?: string; message?: string };
    const statusCode = e.statusCode ?? 500;
    const message = e.message ?? "bad request";
    if (statusCode === 400 || e.code === "FST_ERR_CTP_INVALID_JSON_BODY") {
      reply.status(400).send({ error: message });
      return;
    }
    if (statusCode === 413) {
      reply.status(400).send({ error: "request body too large" });
      return;
    }
    req.log.error({ err }, "unhandled error");
    reply.status(500).send({ error: "internal error" });
  });

  app.setNotFoundHandler((_req, reply) => {
    reply.status(404).send({ error: "not found" });
  });

  app.register(healthRoutes({ isReady: deps.isReady }));
  app.register(ingestRoutes({ repo, maxBatchSize: deps.config.ingestMaxBatch }));
  app.register(queryRoutes({ repo }));
  app.register(aggregateRoutes({ repo }));

  return app;
}
