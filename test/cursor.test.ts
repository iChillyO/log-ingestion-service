import { describe, it, expect } from "vitest";
import { encodeCursor, decodeCursor } from "../src/domain/cursor";

describe("cursor codec", () => {
  it("round-trips a cursor", () => {
    const original = { t: "2026-07-20T14:32:01.123Z", i: "123456789012345" };
    const encoded = encodeCursor(original);
    expect(decodeCursor(encoded)).toEqual(original);
  });

  it("returns null for malformed input", () => {
    expect(decodeCursor("")).toBeNull();
    expect(decodeCursor("!!!not-base64!!!")).toBeNull();
    expect(decodeCursor(Buffer.from("not-json").toString("base64url"))).toBeNull();
    expect(decodeCursor(Buffer.from(JSON.stringify({})).toString("base64url"))).toBeNull();
    expect(decodeCursor(Buffer.from(JSON.stringify({ t: "not-a-date", i: "1" })).toString("base64url"))).toBeNull();
    expect(decodeCursor(Buffer.from(JSON.stringify({ t: "2026-01-01T00:00:00Z", i: "abc" })).toString("base64url"))).toBeNull();
  });
});
