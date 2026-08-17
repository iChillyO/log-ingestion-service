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
