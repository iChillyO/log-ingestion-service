import { describe, it, expect } from "vitest";
import { validateBatch, validateLogEntry } from "../src/domain/logSchemas";

function future(minutes: number): string {
  return new Date(Date.now() + minutes * 60_000).toISOString();
}

describe("validateLogEntry", () => {
  it("accepts a well-formed entry", () => {
    const result = validateLogEntry({
      timestamp: "2026-07-20T14:32:01.123Z",
      level: "error",
      service: "checkout",
      message: "payment declined",
      attributes: { user_id: "42", retries: 3, active: true },
    });
    expect(typeof result).not.toBe("string");
    if (typeof result === "string") return;
    expect(result.level).toBe("error");
    expect(result.service).toBe("checkout");
    // Numbers and booleans are normalised to strings for consistent
    // downstream comparison semantics.
    expect(result.attributes).toEqual({ user_id: "42", retries: "3", active: "true" });
  });

  it("rejects invalid level", () => {
    const result = validateLogEntry({
      timestamp: "2026-07-20T14:32:01Z",
      level: "critical",
      service: "svc",
      message: "hi",
    });
    expect(result).toBe(`invalid level: "critical"`);
  });

  it("rejects timestamp more than 5 minutes in the future", () => {
    const result = validateLogEntry({
      timestamp: future(10),
      level: "info",
      service: "svc",
      message: "hi",
    });
    expect(typeof result).toBe("string");
    expect(String(result)).toContain("more than five minutes in the future");
  });

  it("accepts timestamp within the 5-minute future window", () => {
    const result = validateLogEntry({
      timestamp: future(1),
      level: "info",
      service: "svc",
      message: "hi",
    });
    expect(typeof result).not.toBe("string");
  });

  it("rejects nested attribute objects", () => {
    const result = validateLogEntry({
      timestamp: "2026-07-20T14:32:01Z",
      level: "info",
      service: "svc",
      message: "hi",
      attributes: { nested: { a: 1 } },
    });
    expect(String(result)).toContain("unsupported value type");
  });

  it("rejects attribute arrays", () => {
    const result = validateLogEntry({
      timestamp: "2026-07-20T14:32:01Z",
      level: "info",
      service: "svc",
      message: "hi",
      attributes: { arr: [1, 2] },
    });
    expect(String(result)).toContain("unsupported value type");
  });

  it("rejects missing required fields", () => {
    expect(validateLogEntry({})).toContain("timestamp");
    expect(
      validateLogEntry({ timestamp: "2026-07-20T14:32:01Z", level: "info", service: "s" }),
    ).toContain("message");
    expect(
      validateLogEntry({ timestamp: "2026-07-20T14:32:01Z", level: "info", message: "m" }),
    ).toContain("service");
  });

  it("rejects malformed ISO timestamps", () => {
    expect(String(validateLogEntry({ timestamp: "yesterday", level: "info", service: "s", message: "m" })))
      .toContain("invalid timestamp");
  });
});

describe("validateBatch", () => {
  it("separates valid and invalid entries with indices", () => {
    const batch = [
      { timestamp: "2026-07-20T14:32:01Z", level: "info", service: "s", message: "m1" },
      { timestamp: "2026-07-20T14:32:01Z", level: "bogus", service: "s", message: "m2" },
      { timestamp: "2026-07-20T14:32:01Z", level: "info", service: "s", message: "m3" },
    ];
    const { valid, rejected } = validateBatch(batch);
    expect(valid).toHaveLength(2);
    expect(rejected).toEqual([{ index: 1, reason: `invalid level: "bogus"` }]);
  });
});

describe("NUL rejection", () => {
  const base = {
    timestamp: "2026-07-20T14:32:01Z",
    level: "info",
    service: "svc",
    message: "hello",
  };

  it("rejects a NUL in message, service or an attribute value", () => {
    expect(String(validateLogEntry({ ...base, message: "a\u0000b" }))).toContain("NUL");
    expect(String(validateLogEntry({ ...base, service: "a\u0000b" }))).toContain("NUL");
    expect(String(validateLogEntry({ ...base, attributes: { k: "a\u0000b" } }))).toContain("NUL");
  });

  it("still accepts the tabs and newlines that COPY escapes", () => {
    const result = validateLogEntry({ ...base, message: "line1\nline2\tcol\end" });
    expect(typeof result).not.toBe("string");
  });
});
