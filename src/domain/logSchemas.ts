// Validation and normalisation for ingested log entries.
//
// The contract requires per-entry validation with structured rejection
// reasons, so we hand-roll validators instead of using a generic schema
// library. Keeping this file tight matters: it runs once per log on the
// ingest hot path.

export const LOG_LEVELS = ["debug", "info", "warn", "error"] as const;
export type LogLevel = (typeof LOG_LEVELS)[number];
const LEVEL_SET: ReadonlySet<string> = new Set(LOG_LEVELS);

// Attribute values must be scalars (string | number | boolean) per contract.
// We coerce every scalar to its text representation at ingest so that all
// `attr.<key>=value` comparisons are trivially string equality.
export type AttributeInput = Record<string, string | number | boolean>;
export type AttributeStored = Record<string, string>;

export interface RawLogEntry {
  timestamp?: unknown;
  level?: unknown;
  service?: unknown;
  message?: unknown;
  attributes?: unknown;
}

export interface ValidatedLogEntry {
  timestamp: Date;
  level: LogLevel;
  service: string;
  message: string;
  attributes: AttributeStored;
}

export interface RejectedEntry {
  index: number;
  reason: string;
}

const FIVE_MINUTES_MS = 5 * 60 * 1000;
const MAX_SERVICE_LENGTH = 256;
const MAX_MESSAGE_LENGTH = 64 * 1024;
const MAX_ATTR_KEY_LENGTH = 128;
const MAX_ATTR_VALUE_LENGTH = 4096;
const MAX_ATTR_COUNT = 128;

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function parseTimestamp(value: unknown): Date | string {
  if (typeof value !== "string" || value.length === 0) {
    return "timestamp is required and must be an ISO 8601 string";
  }
  // Date.parse is lenient; require the T separator and a timezone/Z to keep
  // this strictly ISO 8601 rather than accepting free-form strings.
  if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})$/.test(value)) {
    return `invalid timestamp: '${value}'`;
  }
  const ms = Date.parse(value);
  if (!Number.isFinite(ms)) {
    return `invalid timestamp: '${value}'`;
  }
  if (ms - Date.now() > FIVE_MINUTES_MS) {
    return `timestamp more than five minutes in the future: '${value}'`;
  }
  return new Date(ms);
}

function validateAttributes(value: unknown): AttributeStored | string {
  if (value === undefined || value === null) return {};
  if (!isPlainObject(value)) {
    return "attributes must be a flat object";
  }
  const keys = Object.keys(value);
  if (keys.length > MAX_ATTR_COUNT) {
    return `attributes contains too many keys (max ${MAX_ATTR_COUNT})`;
  }
  const out: AttributeStored = {};
  for (const key of keys) {
    if (key.length === 0 || key.length > MAX_ATTR_KEY_LENGTH) {
      return `attribute key '${key}' has invalid length`;
    }
    const raw = (value as Record<string, unknown>)[key];
    if (raw === null || raw === undefined) {
      // Skip null/undefined attribute values silently rather than rejecting
      // the entire entry - callers commonly pass optional fields.
      continue;
    }
    let coerced: string;
    if (typeof raw === "string") {
      coerced = raw;
    } else if (typeof raw === "number") {
      if (!Number.isFinite(raw)) return `attribute '${key}' has non-finite number value`;
      coerced = String(raw);
    } else if (typeof raw === "boolean") {
      coerced = raw ? "true" : "false";
    } else {
      return `attribute '${key}' has unsupported value type: ${typeof raw === "object" ? (Array.isArray(raw) ? "array" : "object") : typeof raw}`;
    }
    if (coerced.length > MAX_ATTR_VALUE_LENGTH) {
      return `attribute '${key}' value exceeds ${MAX_ATTR_VALUE_LENGTH} characters`;
    }
    out[key] = coerced;
  }
  return out;
}

export function validateLogEntry(raw: unknown): ValidatedLogEntry | string {
  if (!isPlainObject(raw)) return "entry must be an object";
  const entry = raw as RawLogEntry;

  const ts = parseTimestamp(entry.timestamp);
  if (typeof ts === "string") return ts;

  if (typeof entry.level !== "string" || !LEVEL_SET.has(entry.level)) {
    return `invalid level: ${JSON.stringify(entry.level)}`;
  }

  if (typeof entry.service !== "string" || entry.service.length === 0) {
    return "service is required and must be a non-empty string";
  }
  if (entry.service.length > MAX_SERVICE_LENGTH) {
    return `service exceeds ${MAX_SERVICE_LENGTH} characters`;
  }

  if (typeof entry.message !== "string" || entry.message.length === 0) {
    return "message is required and must be a non-empty string";
  }
  if (entry.message.length > MAX_MESSAGE_LENGTH) {
    return `message exceeds ${MAX_MESSAGE_LENGTH} characters`;
  }

  const attrs = validateAttributes(entry.attributes);
  if (typeof attrs === "string") return attrs;

  return {
    timestamp: ts,
    level: entry.level as LogLevel,
    service: entry.service,
    message: entry.message,
    attributes: attrs,
  };
}

export interface BatchValidationResult {
  valid: ValidatedLogEntry[];
  rejected: RejectedEntry[];
}

export function validateBatch(logs: unknown[]): BatchValidationResult {
  const valid: ValidatedLogEntry[] = [];
  const rejected: RejectedEntry[] = [];
  for (let i = 0; i < logs.length; i++) {
    const result = validateLogEntry(logs[i]);
    if (typeof result === "string") {
      rejected.push({ index: i, reason: result });
    } else {
      valid.push(result);
    }
  }
  return { valid, rejected };
}
