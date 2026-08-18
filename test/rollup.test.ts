import { describe, it, expect } from "vitest";
import { canUseRollup, rollupWindow } from "../src/db/query/rollup";

describe("canUseRollup", () => {
  it("allows time/service/level filters", () => {
    expect(
      canUseRollup({
        attributes: {},
        service: "auth",
        level: "error",
        since: new Date("2026-03-01T10:00:00Z"),
        until: new Date("2026-03-01T11:00:00Z"),
      }),
    ).toBe(true);
  });

  it("rejects message search and attribute filters", () => {
    expect(canUseRollup({ attributes: {}, q: "declined" })).toBe(false);
    expect(canUseRollup({ attributes: { region: "eu" } })).toBe(false);
  });
});

describe("rollupWindow", () => {
  it("uses only the rollup for minute-aligned bounds", () => {
    const since = new Date("2026-03-01T10:00:00.000Z");
    const until = new Date("2026-03-01T11:00:00.000Z");
    const win = rollupWindow(since, until);
    expect(win.rollupFrom.toISOString()).toBe("2026-03-01T10:00:00.000Z");
    expect(win.rollupTo.toISOString()).toBe("2026-03-01T11:00:00.000Z");
    expect(win.edge1From.getTime()).toBe(win.edge1To.getTime());
    expect(win.edge2From.getTime()).toBe(win.edge2To.getTime());
  });

  it("splits unaligned leftovers onto the raw edges", () => {
    const since = new Date("2026-03-01T10:00:15.000Z");
    const until = new Date("2026-03-01T10:02:40.000Z");
    const win = rollupWindow(since, until);
    expect(win.edge1From.toISOString()).toBe("2026-03-01T10:00:15.000Z");
    expect(win.edge1To.toISOString()).toBe("2026-03-01T10:01:00.000Z");
    expect(win.rollupFrom.toISOString()).toBe("2026-03-01T10:01:00.000Z");
    expect(win.rollupTo.toISOString()).toBe("2026-03-01T10:02:00.000Z");
    expect(win.edge2From.toISOString()).toBe("2026-03-01T10:02:00.000Z");
    expect(win.edge2To.toISOString()).toBe("2026-03-01T10:02:40.000Z");
  });

  it("reads the whole window from logs when it spans no complete minute", () => {
    const since = new Date("2026-03-01T10:00:10.000Z");
    const until = new Date("2026-03-01T10:00:40.000Z");
    const win = rollupWindow(since, until);
    expect(win.rollupFrom.getTime()).toBe(win.rollupTo.getTime());
    expect(win.edge1From.toISOString()).toBe("2026-03-01T10:00:10.000Z");
    expect(win.edge1To.toISOString()).toBe("2026-03-01T10:00:40.000Z");
    expect(win.edge2From.getTime()).toBe(win.edge2To.getTime());
  });
});
