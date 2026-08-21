// Structured errors thrown by handlers and mapped to the contract's
// `{ "error": "..." }` response body by the Fastify error handler.

export class BadRequestError extends Error {
  readonly statusCode = 400;
  constructor(message: string) {
    super(message);
    this.name = "BadRequestError";
  }
}

export class UnauthorizedError extends Error {
  readonly statusCode = 401;
  constructor(message = "missing or invalid credentials") {
    super(message);
    this.name = "UnauthorizedError";
  }
}

export class ForbiddenError extends Error {
  readonly statusCode = 403;
  constructor(message = "insufficient scope") {
    super(message);
    this.name = "ForbiddenError";
  }
}

/**
 * Backpressure. The brief is explicit that shedding load with 503 + Retry-After
 * beats crashing, and that a 200 must never be returned for a batch we have not
 * durably accepted.
 */
export class ServiceUnavailableError extends Error {
  readonly statusCode = 503;
  constructor(
    message = "service temporarily unable to accept writes",
    readonly retryAfterSeconds = 1,
  ) {
    super(message);
    this.name = "ServiceUnavailableError";
  }
}
