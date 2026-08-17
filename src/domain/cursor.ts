// Opaque cursor codec for `GET /logs`.
//
// A cursor encodes the (timestamp, id) of the last row returned so the next
// page can continue with `WHERE (timestamp, id) < (ts, id)`. We base64url the
// JSON so the value survives any transport and stays opaque to the client.

export interface Cursor {
  t: string; // ISO timestamp of the last row on the previous page
  i: string; // id of the last row (bigint serialised as decimal string)
}

function base64urlEncode(input: string): string {
  return Buffer.from(input, "utf8")
    .toString("base64")
    .replace(/=+$/g, "")
    .replace(/\+/g, "-")
    .replace(/\//g, "_");
}

function base64urlDecode(input: string): string {
  const padded = input.replace(/-/g, "+").replace(/_/g, "/");
  const pad = padded.length % 4 === 0 ? "" : "=".repeat(4 - (padded.length % 4));
  return Buffer.from(padded + pad, "base64").toString("utf8");
}

export function encodeCursor(cursor: Cursor): string {
  return base64urlEncode(JSON.stringify(cursor));
}

export function decodeCursor(raw: string): Cursor | null {
  if (typeof raw !== "string" || raw.length === 0 || raw.length > 512) return null;
  let decoded: string;
  try {
    decoded = base64urlDecode(raw);
  } catch {
    return null;
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(decoded);
  } catch {
    return null;
  }
  if (typeof parsed !== "object" || parsed === null) return null;
  const c = parsed as { t?: unknown; i?: unknown };
  if (typeof c.t !== "string" || typeof c.i !== "string") return null;
  // Validate the timestamp portion is parseable and the id is a positive int.
  if (!Number.isFinite(Date.parse(c.t))) return null;
  if (!/^\d+$/.test(c.i)) return null;
  return { t: c.t, i: c.i };
}
