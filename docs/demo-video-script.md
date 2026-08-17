# 5-Minute Demo Video Script

**Total runtime:** ~5:00. Speaking pace assumed at ~150 words per minute. If you speak faster, add pauses; if slower, trim the parenthetical detail.

**Recording setup checklist (do this before hitting record):**
- Docker Desktop running
- Terminal 1 open at project root (`F:\Projects\Coding\FinalProject`) with a big font (Ctrl+= a few times in Windows Terminal)
- Terminal 2 open, also at project root, for the docker-compose exec queries
- VS Code / Cursor open with `src/db/repositories/logRepository.ts` and `migrations/001_init.sql` in tabs
- Browser tab open to `http://localhost:8080/health` (don't refresh yet)
- Have `docs/interview-guide.pdf` open in case you blank
- **Warm the stack first** (before recording): `docker compose down -v; docker compose up -d --build`, wait for healthy, ingest ~100k rows with `npx tsx scripts/load-smoke.ts --duration 10 --concurrency 4 --query-rate 0`, then `docker compose down -v` again. This pre-pulls all images so the real recording doesn't stall on downloads.

---

## Timing overview

| Segment | Time | What you're showing |
| --- | --- | --- |
| Intro / what is this | 0:00 – 0:30 | Slide or terminal |
| Tech stack + start it up | 0:30 – 1:15 | Terminal 1: `docker compose up --build` |
| Live demo of the 4 endpoints | 1:15 – 2:30 | Terminal 1: curl commands |
| Schema & indexes decision | 2:30 – 3:30 | Editor: `001_init.sql`, `002_indexes.sql` |
| Performance results + EXPLAIN | 3:30 – 4:15 | Terminal 2: `EXPLAIN ANALYZE` |
| Known limitation & wrap | 4:15 – 5:00 | Editor: `README.md` Known Limitations |

---

## 0:00 – 0:30 — Intro (30 seconds)

**SHOW:** Your face on camera, or a terminal window with the project directory listing.

**SAY:**

> "Hi. This is my log ingestion and query service — a simplified Datadog or Grafana Loki. Applications POST batches of structured logs to it, and it stores them in PostgreSQL and makes them searchable and aggregatable. The whole system runs in Docker Compose and is designed to hit fifteen thousand logs per second on half a vCPU for the app and one vCPU for Postgres. Let me walk you through it."

---

## 0:30 – 1:15 — Tech stack + start it up (45 seconds)

**ACTION:** Alt-tab to Terminal 1. Type but don't press Enter yet: `docker compose up --build`

**SAY (while typing):**

> "It's TypeScript on Fastify — Fastify because it's about three times faster than Express, which matters when every microsecond counts at fifteen thousand requests a second. PostgreSQL because the brief required it as the source of truth. I use the raw pg driver with parameterized SQL, no ORM overhead on the hot path."

**ACTION:** Press Enter. Compose starts.

**SAY (as containers start):**

> "One command — `docker compose up --build` — brings the whole stack up. Postgres 16 tuned for the one GB memory limit, then the app container. Migrations run automatically at startup, and only after they finish and the retention worker has ensured today's partition exists does `/health` return 200."

**ACTION:** Wait for `Container logs-app Started` (~15-20 seconds since images are cached).

**SAY:**

> "There we go — both containers up."

---

## 1:15 – 2:30 — Live demo of the 4 endpoints (75 seconds)

**ACTION:** Type each command, pausing between them to narrate. Keep the terminal focused so the JSON responses are visible.

**Command 1 — health:**
```powershell
curl.exe http://localhost:8080/health
```

**SAY:**

> "Health check — the service is ready."

**ACTION:** Then, using a prepared batch payload (see appendix below), ingest a log.

**Command 2 — ingest:**
```powershell
$ts = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
$body = @{ logs = @(@{ timestamp = $ts; level = "error"; service = "checkout"; message = "payment declined"; attributes = @{ user_id = "42"; region = "eu-west"; retries = 3 } }) } | ConvertTo-Json -Depth 6 -Compress
Set-Content .tmp.json $body -Encoding utf8
curl.exe -X POST http://localhost:8080/logs -H "content-type: application/json" --data-binary "@.tmp.json"
```

**SAY (as the command runs):**

> "POST slash logs takes a batch — I'm sending one log. The response tells me it was accepted, with an empty rejected list. If any entry in the batch had been invalid, it would come back here with its index and the reason, without failing the rest of the batch — that's a hard requirement in the contract."

**Command 3 — query:**
```powershell
curl.exe "http://localhost:8080/logs?service=checkout&level=error&limit=5"
```

**SAY:**

> "GET slash logs takes freely combinable filters — service, level, time range, attribute equality, and message substring — with cursor-based pagination. Notice the response includes an id and a `next_cursor`. The cursor is base64-encoded JSON containing the last row's timestamp and id — the tuple I use as the deterministic sort key."

**Command 4 — aggregate:**
```powershell
$since = ((Get-Date).ToUniversalTime().AddHours(-1)).ToString("yyyy-MM-ddTHH:mm:ssZ")
$until = ((Get-Date).ToUniversalTime().AddMinutes(1)).ToString("yyyy-MM-ddTHH:mm:ssZ")
curl.exe "http://localhost:8080/logs/aggregate?since=$since&until=$until&bucket=1m&group_by=service"
```

**SAY:**

> "And aggregation. Time-bucketed counts with an optional group-by. This one bucket has our one log grouped by checkout. Buckets are aligned to the epoch using Postgres's `date_bin` function, so a one-minute bucket at 14:32 is always the same bucket, regardless of the query window."

---

## 2:30 – 3:30 — Schema and indexes (60 seconds — the technical heart of the demo)

**ACTION:** Alt-tab to your editor. Open `migrations/001_init.sql`. Highlight the `CREATE TABLE logs ... PARTITION BY RANGE ("timestamp")` block.

**SAY:**

> "The most important design decision: the logs table is range-partitioned by day. Each day gets its own child table. This gives me two big wins."
>
> "First, retention. Deleting thirty-day-old data is a `DROP TABLE` on that day's partition — metadata-only, milliseconds. The naive `DELETE FROM logs WHERE timestamp less than…` would lock the table, generate huge amounts of write-ahead-log, and leave the heap bloated needing a VACUUM. Partition drop sidesteps all three."
>
> "Second, partition pruning. A query for the last hour only scans today's partition — Postgres ignores the other twenty-nine."

**ACTION:** Switch to `migrations/002_indexes.sql`. Highlight one line at a time as you narrate.

**SAY:**

> "Five indexes, each aligned to a query shape from the contract. The primary sort index on timestamp descending, id descending, for cursor pagination. Two composite indexes for service and level filters. A GIN index on attributes using `jsonb_path_ops` — half the size of the default opclass and faster for containment queries. And a trigram GIN on the message column so `q` substring search uses an index instead of a sequential scan."

**ACTION:** Open `src/domain/logSchemas.ts` briefly, scroll to the attribute-validation section.

**SAY:**

> "One more schema decision worth calling out: I coerce every attribute value to a string at ingest. The contract says attribute equality is compared as strings, so storing the number three and querying for the string three would otherwise not match. Coercing at ingest makes every query trivially correct. Trade-off documented in the README."

---

## 3:30 – 4:15 — Performance and EXPLAIN ANALYZE (45 seconds)

**ACTION:** Alt-tab to Terminal 2. If the stack was just torn down at the start, note here that these numbers were measured earlier — or run a very quick warmup:

**SAY:**

> "Performance. Measured on the exact resource limits — half a vCPU for the app, one vCPU for Postgres."
>
> "Peak ingest throughput: twenty thousand three hundred and eighty-three logs per second, zero errors. That's above the fifteen thousand target and clears the twenty-K stretch goal in the brief."
>
> "Aggregation query alone at one point two million rows: p50 of two hundred and eighty-five milliseconds, p95 of three hundred and twenty-eight — well under the one-second target."

**ACTION:** Run in Terminal 2 (or paste from prepared clipboard):

```powershell
docker compose exec -T postgres psql -U logs -d logs -c "EXPLAIN ANALYZE SELECT date_bin(INTERVAL '1 minute', timestamp, TIMESTAMPTZ 'epoch') AS b, service, count(*) FROM logs WHERE timestamp >= now() - INTERVAL '1 hour' GROUP BY b, service ORDER BY b;"
```

**SAY (as the plan prints):**

> "Here's the plan. Notice two things: partition pruning — the scan only touches today's partition — and Postgres picks a parallel plan with two workers doing partial hash aggregation, then a finalize step merging them. That's why sub-second aggregation on a million rows is achievable on one vCPU."

---

## 4:15 – 5:00 — Known limitation + wrap (45 seconds)

**ACTION:** Open `README.md`, scroll to the "Known limitations" section, highlight the first bullet.

**SAY:**

> "Being honest about what doesn't work perfectly: under simultaneous max-throughput ingest and concurrent aggregation queries, my aggregate p95 exceeds one second. The reason is CPU contention — eight ingest client backends plus a parallel query worker plus autovacuum all competing for one vCPU."
>
> "I tried three optimizations to fix it in-place — a covering index for index-only scans, disabling parallel workers, and a BRIN index — and reverted all three because they made things worse. All of that is documented in the README."
>
> "The real fix is pre-aggregated rollup tables. A background job maintains minute-level counts by service in a small table. The aggregate query then reads a tiny rollup instead of scanning millions of raw rows. I've documented the exact shape it would take and where it fits in the codebase. That would push aggregate p95 well under a hundred milliseconds even under peak ingest."

**ACTION:** Face camera again (or just terminal).

**SAY:**

> "That's the tour. Full test suite is thirty-eight tests — twenty-seven unit and eleven end-to-end contract tests through the real Fastify pipeline against real Postgres. CI runs all of them plus a `docker compose up` smoke test on every push. Thanks for watching."

**ACTION:** Stop recording.

---

## Appendix — command aliases for the demo

To keep the demo smooth, put these in `docs/demo-commands.ps1` and dot-source it in your recording terminal beforehand:

```powershell
function Ingest-Sample {
  $ts = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
  $body = @{ logs = @(@{
    timestamp = $ts
    level = "error"
    service = "checkout"
    message = "payment declined"
    attributes = @{ user_id = "42"; region = "eu-west"; retries = 3 }
  }) } | ConvertTo-Json -Depth 6 -Compress
  Set-Content .tmp.json $body -Encoding utf8
  curl.exe -X POST http://localhost:8080/logs -H "content-type: application/json" --data-binary "@.tmp.json"
  Remove-Item .tmp.json
}

function Query-Sample {
  curl.exe "http://localhost:8080/logs?service=checkout&level=error&limit=5"
}

function Aggregate-Sample {
  $since = ((Get-Date).ToUniversalTime().AddHours(-1)).ToString("yyyy-MM-ddTHH:mm:ssZ")
  $until = ((Get-Date).ToUniversalTime().AddMinutes(1)).ToString("yyyy-MM-ddTHH:mm:ssZ")
  curl.exe "http://localhost:8080/logs/aggregate?since=$since&until=$until&bucket=1m&group_by=service"
}

function Explain-Aggregate {
  docker compose exec -T postgres psql -U logs -d logs -c "EXPLAIN ANALYZE SELECT date_bin(INTERVAL '1 minute', timestamp, TIMESTAMPTZ 'epoch') AS b, service, count(*) FROM logs WHERE timestamp >= now() - INTERVAL '1 hour' GROUP BY b, service ORDER BY b;"
}
```

Then during the demo you just type `Ingest-Sample`, `Query-Sample`, `Aggregate-Sample`, `Explain-Aggregate` — cleaner than pasting multi-line commands on camera.

---

## Recording tools (all free)

**Easiest — Windows Xbox Game Bar (built in):**
- Press Win+G, click the record button. Records the focused window with your mic.
- Output lands in `C:\Users\Chilly\Videos\Captures`.

**Better — OBS Studio:**
- Download from https://obsproject.com — pick a "Display Capture" source and a mic source, hit Start Recording.
- Handles multi-monitor cleanly and lets you crop to just the terminal.

**Simplest for editing — Loom:**
- https://loom.com — records, uploads, gives you a shareable link. Free tier is 5 min per video (perfect fit).

## Rehearsal tip

Run through the script once end-to-end without recording, timing yourself with a stopwatch. If you land at 5:30, cut the parenthetical asides in sections 2 and 3. If you land at 4:00, slow down and add pauses between commands — dead air is fine, rushing is not.
