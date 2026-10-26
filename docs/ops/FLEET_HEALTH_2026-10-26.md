# Fleet health, 2026-10-26 - the five engines that were failing on their own schedules

Khalid, 2026-10-26: *"fix these: pre-existing fleet-health items unrelated to fusion: analytics-snapshot,
earnings-whisper, treasury-auto-updater, signal-genealogy, event-flow-monitor erroring on their own schedules."*
The post-fix census (ops 6664) had listed them; ops 6672 pulled 24 h of their REPORT / error lines, ops 6673 ran
signal-genealogy's own archive audit on the runner, ops 6674 sized every input.  Each fix below is aimed at a measured
cause.  Evidence: `aws/ops/reports/latest/ops_667{2,3,4,5,6}_*.md`.

| Engine | Symptom (24 h to 15:00 UTC) | Measured cause | Fix | Verified |
|---|---|---|---|---|
| `justhodl-analytics-snapshot` | 3/3 runs `Task timed out` at 600 s, 282 MB | reads every object under `equity-research/` (35,572 objects, **11.9 GB**, of which `industry2/` - the desk's 10-year peer-price cache, not reports - is 15,566 objects / 9.4 GB), `edgar-insiders/`, `equity-critique/` every night | **v2.0.0**: tables are the top level of their prefix (`Delimiter="/"`); reads are incremental by ETag against a persisted state (`analytics/_state/<table>.json.gz`); new/changed objects are read under the remaining time minus a 45 s reserve; a changed object not reached keeps its previous row (each row carries its own `generated_at`) and the flat files + `analytics/manifest.json` carry `complete` / `pending`. 1024 MB. 13 offline tests | ops 6676 |
| `justhodl-earnings-whisper` | 3/3 runs `Task timed out` at 60 s, memory at the 256 MB ceiling, last print "Loading data…" | its seven inputs grew: `eps-revision-velocity.json` **55 MB**, `options-flow-scanner.json` 19 MB, `8k-filings.json` 5.5 MB, `revenue-acceleration.json` 4 MB - all seven decoded and held at once in 256 MB | **v1.1.0**: each input is fetched, reduced to the fields its scorer reads, and released before the next; `config.json` added (1536 MB / 180 s) so the lane applies a budget (the function had none in the repo). Scoring and output schema unchanged (`engine_version`, `input_bytes` added). 4 offline tests | ops 6676 |
| `treasury-auto-updater` | 3/3 runs `Task timed out` at 3 s (11:00 UTC daily-morn tick) | its only job is `lambda.invoke(treasury-api, RequestResponse)`; `treasury-api` has been held at **reserved concurrency 0** since the 2026-10-20 retire plan (a broken writer, Khalid: "switch off the orphans and the broken writers"), so the invoke is throttled, botocore retries, 3 s timeout | **switched off the same way** (ops 6675): removed from the `daily-morn` fan-out tick in the live manifest (backup `backups/retire-2026-10-26/<stamp>/manifest/fanout-manifest.json`), the repo manifest, `config/retire-plan-2026-10-20.json` (`switch_off`) and `config/retired-engines.json`; `tests/deployment/test_retired_engines.py` keeps it off. Re-enable together with `treasury-api` or not at all | ops 6675: live and repo tick memberships agree |
| `justhodl-signal-genealogy` | every run (06:40 and 13:33 UTC) raises `ValueError: unreconciled archive cannot produce chronology` | the collector (`prospective_journal.register`, via `research_network_prospective`) attaches a `research_context` receipt to every record reference; the archive validator required the exact 8-key reference set, so **205 of 681 captures** failed `record_ref_schema` and 1,194 records were left unreferenced (ops 6673, ops 6674 shows the key set) | validator admits the receipt as an **optional, typed, per-capture field** coupled to the `created` flag (`retained` / `context_not_retained` / `existing_forecast_not_backfilled` - the collector's three shapes, nothing else), excludes it from the conflict-detection identity, never publishes it; applied to the deployed source, the accepted candidate (`aws/ops/checks/genealogy_native_candidate/`) and the ops-check copy. Independent review: ship; its three suggestions adopted (bounded `reason`, a real `register()` -> validator contract test, the mixed-vintage case) | ops 6673 re-run after deploy: `complete True`, failures 0 |
| `justhodl-event-flow-monitor` | 3/3 runs `Task timed out` at 120 s, memory at the 512 MB ceiling | the five storm day-logs (Oct 12-16, 22-33 MB each) sit inside the 14-day window and every line of every day was parsed into memory; there is **no audit day-log since 2026-10-17** (the bus has been silent) and the alert path crashed on `pulse['last_event_ts'][:19]` with `None` | **v1.1.0**: day-logs are streamed; a finished day is reduced once to per-event counts and cached (`system-events/audit/_counts/<day>.json`); reading is budgeted and the output states `window_days_read` / `audit_days_present`; the silent-bus crash is fixed (the pulse is reported RED, which is the truth). 1024 MB / 300 s. 5 offline tests | ops 6676 |

What the census also showed and this change does **not** touch: `justhodl-stock-screener` exhausts FMP 429 on
price-target-consensus (a bank-fusion wave-3 candidate); eps-revision-velocity / momentum-leaders / earnings-pead raise
their designed "preserve previous publication" under the FMP bandwidth block (the reviewed-contract engines, their own
reviewed changes); the event bus itself has routed nothing since 2026-10-17 - the monitor now says so instead of timing
out, and the Telegram alert path still ends at a dead bot token (KHALID_ACTIONS item 0, the SNS confirmation click).

## After the fleet lane (census 14:46-20:48 UTC, ops 6664 re-run)

184 functions invoked, **7 with errors** (13 in the morning census). The five engines above: **0 errors**. The remaining
seven: four reviewed-contract engines raising their designed "preserve previous publication" under the FMP block
(momentum-leaders, earnings-pead, eps-revision-velocity - the bank held nothing newer; estimate-revisions - its
Polygon/Benzinga calendar answers 403 on this key, since Oct 10), portfolio-snapshot once (`OrderingUnavailable: Snapshot
retention conflicts with lifecycle expiry`, pre-existing), cftc-futures-positioning-agent once (120 s timeout, legacy
agent) - and **justhodl-equity-research, the why.html desk, 16 errors in 129 user requests**: ops 6683 showed 19 of 145
requests dying out-of-memory or at 300 s with a median heap of 976 MB on the *successful* ones. Cause: every book miss
(a delisted, foreign or OTC name - ALPN, DOCMF, VINO, INNV, ARTR) fell back to decoding whole-market statement year-files
and the profile bulk inside a 1 GB request-serving container that never shrinks back; the bank-time cap was hit at 97 s
of bank time for the same reason. Fix (fmp_bank, lane ER): at or below 1 GB a point lookup never falls back from the
per-symbol books to a whole-market table decode once the book is built - the books answer or the shim goes live
(`FMP_BANK_POINT_SCAN_MIN_MB`, `STATS.book_only_misses`); batch engines above 1 GB keep the scan. Verified by ops 6685.

Platform lessons recorded:
* A nightly "read everything under a prefix" engine must list only its own document family and read incrementally;
  a prefix is shared by families that grow at different rates (`equity-research/industry2/` is 80 % of the bytes).
* An engine that consumes other engines' outputs needs a memory budget sized to those outputs, in `config.json`, or
  the lane never applies one (earnings-whisper ran on the 128 MB -> 256 MB default).
* When a producer adds a field to a contract-pinned document, the validator's test suite must drive the real producer
  (the drift ran live for weeks because the genealogy tests used hand-written fixtures only).
* An engine whose only job is to invoke another is switched off with it.
* A request-serving container must never decode a whole-market table for one symbol's answer: the per-symbol books are
  the only bank path at 1 GB. The morning's cold-container proof (227 MB) hid this because it asked for a name the book held.
