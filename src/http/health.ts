import type { FastifyPluginAsync } from "fastify";

export interface HealthDeps {
  isReady: () => boolean;
}

// GET /health — always unauthenticated, returns 200 once the service is ready
// (DB connected, migrations applied, retention worker started).
export const healthRoutes = (deps: HealthDeps): FastifyPluginAsync => {
  return async (app) => {
    app.get("/health", async (_req, reply) => {
      if (!deps.isReady()) {
        reply.status(503);
        return { status: "starting" };
      }
      return { status: "ok" };
    });
  };
};
