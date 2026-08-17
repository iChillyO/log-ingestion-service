import { describe, it, expect } from "vitest";
import { parseAggregateParams, parseListParams } from "../src/http/queryParsing";
import { BadRequestError } from "../src/http/errors";

describe("parseListParams", () => {
  it("parses all filters", () => {
    const p = parseListParams({
      service: "checkout",
      level: "error",
      since: "2026-07-20T14:00:00Z",
      until: "2026-07-20T15:00:00Z",
      "attr.user_id": "42",
      "attr.region": "eu-west",
      q: "declined",
      limit: "250",
    });
    expect(p.filters.service).toBe("checkout");
    expect(p.filters.level).toBe("error");
    expect(p.filters.since?.toISOString()).toBe("2026-07-20T14:00:00.000Z");
    expect(p.filters.until?.toISOString()).toBe("2026-07-20T15:00:00.000Z");
    expect(p.filters.attributes).toEqual({ user_id: "42", region: "eu-west" });
    expect(p.filters.q).toBe("declined");
    expect(p.limit).toBe(250);
    expect(p.cursor).toBeNull();
  });

  it("defaults limit to 100", () => {
    expect(parseListParams({}).limit).toBe(100);
  });

  it("rejects invalid limits", () => {
    expect(() => parseListParams({ limit: "0" })).toThrow(BadRequestError);
    expect(() => parseListParams({ limit: "1001" })).toThrow(BadRequestError);
    expect(() => parseListParams({ limit: "abc" })).toThrow(BadRequestError);
  });

  it("rejects invalid level", () => {
    expect(() => parseListParams({ level: "critical" })).toThrow(BadRequestError);
  });

  it("rejects until < since", () => {
    expect(() =>
      parseListParams({ since: "2026-01-02T00:00:00Z", until: "2026-01-01T00:00:00Z" }),
    ).toThrow(BadRequestError);
  });

  it("allows until == since", () => {
    expect(() =>
      parseListParams({ since: "2026-01-01T00:00:00Z", until: "2026-01-01T00:00:00Z" }),
    ).not.toThrow();
  });

  it("rejects invalid cursor", () => {
    expect(() => parseListParams({ cursor: "not-a-cursor" })).toThrow(BadRequestError);
  });
});

describe("parseAggregateParams", () => {
  it("requires since, until, bucket", () => {
    expect(() => parseAggregateParams({})).toThrow(/since is required/);
    expect(() =>
      parseAggregateParams({ since: "2026-01-01T00:00:00Z" }),
    ).toThrow(/until is required/);
    expect(() =>
      parseAggregateParams({ since: "2026-01-01T00:00:00Z", until: "2026-01-02T00:00:00Z" }),
    ).toThrow(/bucket is required/);
  });

  it("validates bucket size", () => {
    expect(() =>
      parseAggregateParams({
        since: "2026-01-01T00:00:00Z",
        until: "2026-01-02T00:00:00Z",
        bucket: "30s",
      }),
    ).toThrow(/invalid bucket/);
  });

  it("accepts group_by", () => {
    const p = parseAggregateParams({
      since: "2026-01-01T00:00:00Z",
      until: "2026-01-01T01:00:00Z",
      bucket: "1m",
      group_by: "service",
    });
    expect(p.groupBy).toBe("service");
  });

  it("rejects too-large windows", () => {
    expect(() =>
      parseAggregateParams({
        since: "2026-01-01T00:00:00Z",
        until: "2099-01-01T00:00:00Z",
        bucket: "1m",
      }),
    ).toThrow(/too many buckets/);
  });
});
