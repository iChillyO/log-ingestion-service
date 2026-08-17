import { describe, it, expect } from "vitest";
import { buildWhereClause } from "../src/db/query/buildLogFilters";

describe("buildWhereClause", () => {
  it("returns empty clause when no filters set", () => {
    const built = buildWhereClause({ attributes: {} });
    expect(built.clause).toBe("");
    expect(built.params).toEqual([]);
  });

  it("emits sequential $N placeholders", () => {
    const built = buildWhereClause(
      {
        attributes: { user_id: "42" },
        service: "checkout",
        level: "error",
        since: new Date("2026-07-20T14:00:00Z"),
        until: new Date("2026-07-20T15:00:00Z"),
        q: "declined",
      },
      { includeIngestionCeiling: true },
    );
    // Ensure every placeholder is unique and dense: $1..$N.
    const matches = built.clause.match(/\$\d+/g) ?? [];
    const nums = matches.map((m) => Number(m.slice(1)));
    expect(new Set(nums).size).toBe(nums.length);
    expect(Math.max(...nums)).toBe(built.params.length);
    expect(built.clause).toContain("WHERE");
    expect(built.clause).toContain("service = ");
    expect(built.clause).toContain("level = ");
    expect(built.clause).toContain("attributes @>");
    expect(built.clause).toContain("message ILIKE");
    expect(built.clause).toContain("NOW() + INTERVAL '5 minutes'");
  });

  it("adds cursor tuple comparison", () => {
    const built = buildWhereClause(
      { attributes: {} },
      { cursor: { t: "2026-07-20T14:32:01Z", i: "123" } },
    );
    expect(built.clause).toContain(`("timestamp", id) <`);
    expect(built.params).toHaveLength(2);
  });

  it("passes attributes as a JSON string containment object", () => {
    const built = buildWhereClause({ attributes: { user_id: "42", region: "eu" } });
    expect(built.params[0]).toBe(JSON.stringify({ user_id: "42", region: "eu" }));
    expect(built.clause).toContain("::jsonb");
  });

  it("escapes ILIKE metacharacters in q", () => {
    const built = buildWhereClause({ attributes: {}, q: "100%_bug" });
    expect(built.params[0]).toBe("100\\%\\_bug");
  });
});
