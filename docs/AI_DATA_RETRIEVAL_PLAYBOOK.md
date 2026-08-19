# AI DATA RETRIEVAL PLAYBOOK — ECB & FRED

v1 · ops 4900 · 2026-08-19 · written by Claude, for Claude and every
other AI or agent that ever touches this system.

READ THIS BEFORE WRITING ANY ECB OR FRED PULLER. This system spent
~8 months with ECB marked BLOCKED. The entire block was ONE HTTP
header on ONE endpoint, while a sibling engine pulled the same host
successfully 24/7 the whole time. Every hard-won detail is below so
that mistake — and the FRED ones — can never happen again.

Permanent copies: docs/AI_DATA_RETRIEVAL_PLAYBOOK.md (this file, repo)
· s3://justhodl-dashboard-live/data/warm/playbooks/ecb-fred-retrieval.md
(deny-Delete protected) · embedded at the bottom of data.html.

---------------------------------------------------------------------
PART 1 — ECB · data-api.ecb.europa.eu
---------------------------------------------------------------------

1.0 THE 8-MONTH BUG IN ONE PARAGRAPH
The catalog builder asked the STRUCTURE endpoint with
`Accept: application/xml` → ECB answers HTTP 406 Not Acceptable.
The dataflow catalog never landed, the walker starved on the missing
file, wrote no state, and the sentinel hardcoded sdmx-ecb = BLOCKED.
Meanwhile justhodl-ciss-stress pulled the DATA endpoint continuously
with no Accept header and `?format=csvdata` — which never negotiates.
Diagnosis method that solved it: a sibling engine succeeding against
the same host IS your diagnosis map. (ops 4893)

1.1 THE TWO ENDPOINTS — GOLDEN RULES
DATA:      https://data-api.ecb.europa.eu/service/data/{flowRef}
STRUCTURE: https://data-api.ecb.europa.eu/service/dataflow

DATA rule: put `?format=csvdata` in the query string. The query param
SIDESTEPS content negotiation entirely — there is no 406 on this
path. Send NO Accept header. Send an honest User-Agent with contact
(ours: "JustHodl Research raafouis@gmail.com"). That's it.

STRUCTURE rule: NEVER send `Accept: application/xml` (→ 406, the
whole outage). Use this ladder, first 2xx wins, record the winner:
  1. NO Accept header at all        ← wins (200, SDMX-ML 2.1 default)
  2. application/vnd.sdmx.structure+xml;version=2.1
  3. */*

1.2 THE CATALOG — 214 FLOWS, 5 AGENCIES, NOT 104
Bare `/service/dataflow` (no agency segment) returns ALL maintainer
agencies hosted on the portal: ECB 104 · ECB.DISS 89 (dissemination
views: *_PUB, JDF_*, MOBILE_*) · ESTAT 11 · EUROSTAT 6 · IMF 4
= 214 total. `/service/dataflow/ECB` returns only 104 — a silent
110-flow gap that hid GFS, MNA, QSA, EDP, LCI, ICPF, IEAQ, LFSI,
BP6, RA6 and more. (ops 4897 census, 4898 expansion)
Foreign-agency data flowRef uses a COMMA:
  /service/data/ESTAT,GFS?format=csvdata
We store those ids as "AGENCY:ID" and map ":" → "," at URL time.

1.3 FULL HISTORY & THE TRUNCATION TRAP
A bare pull (no startPeriod / lastNObservations) returns FULL history
since inception — that is the SDMX default. Rows arrive OLDEST-FIRST,
so any byte-cap truncation silently deletes the RECENT tail — the
worst possible loss. 48 of the 214 flows exceed 450MB of raw CSV;
RAM-buffered pulls can NEVER win. The house answer (justhodl-ecb-deep,
ops 4896): time-slice with `startPeriod=YYYY&endPeriod=YYYY` windows —
ladder 1900-1979 · 1980-1989 · 1990-1994 · 1995-1999 · 2000-2004 ·
2005-2009 · 2010-2014 · 2015-2019 · 2020-2022 · 2023-2024 · 2025-2035
— stream each response to /tmp (10GB ephemeral), gzip file-to-file,
upload as its own part. A window that still exceeds the 3.5GB guard
splits to single years, then to months (YYYY-MM). HTTP 404 on a
window = genuinely empty range (pre-inception) = fine, mark empty.

1.4 SERIES ENUMERATION WITHOUT ANY STRUCTURE CALL
  {flow}?format=csvdata&lastNObservations=1
returns exactly one row per series = the complete series-key list of
a flow. This is how ciss-stress enumerates 65 CISS/CLIFS/SovCISS
series with zero metadata calls.

1.5 INCEPTION VERIFICATION — DO IT RIGHT
Parse the CSV header, locate the TIME_PERIOD column index, read the
first data row. NEVER regex-scan for year-like tokens: integer
OBS_VALUEs (1998, 2045…) false-positive and produced fake
min_year=1900 / max_year=2099 in ops 4895. Burned once; never again.

1.6 KNOWN-EMPTY REGISTRY ENTRIES — DO NOT CHASE
These 7 exist in the registry but 404 with lastNObservations=1
(proven, ops 4899): DD · ECB.DISS:JVC_PUB · ECB.DISS:LCI_PUB ·
ECB.DISS:MOBILE_KEY_4 · ECB.DISS:MOBILE_KEY_5 · ECB.DISS:RESR_PUB ·
ECB.DISS:SUR_PUB. Ledger: data/warm/ecb/failures-classified.json.

1.7 REVISIONS
ECB revises HISTORY (BSI, ICP especially). Refreshing only the newest
window is wrong — rotate historical windows too (ecb-deep does one
per cycle over the whole back catalog).

1.8 WHERE EVERYTHING ALREADY LIVES — READ, DON'T RE-PULL
  data/warm/ecb/catalog.json.gz          214 flows, agency histogram,
                                         accept_winner + negotiation
  data/warm/ecb/data/{FLOW}.dat.gz       166 fast flows, gzip CSV,
                                         full history, weekly re-pull
  data/warm/ecb/data/{FLOW}__{s}_{e}.dat.gz   deep parts (48 giants)
  data/warm/ecb/data/{FLOW}.manifest.json     parts, first_period
  data/warm/ecb/coverage.json            live completeness ledger
  data/warm/ecb/failures-classified.json the 7 empties, proven
  data/_state/sdmx-walk-ecb.json         done/truncated/failures/lease
  data/_state/ecb-deep.json              window queue, mode, n_complete
  data/raw/ecb/YYYY-MM-DD/*.json.gz      F4 raw snapshots
  data/ciss-stress.json · data/ciss-ai.json   CISS engine outputs
Engines: justhodl-ecb-full-catalog (nightly) · justhodl-sdmx-walker
agency=ecb (hourly; event overrides: per, cap_mb, budget,
retry_truncated, retry_failures, reset_done) · justhodl-ecb-deep
(10-min Scheduler; backfill → refresh + revision rotation) ·
justhodl-ciss-stress / justhodl-ciss-ai (daily).
Schedules: justhodl-ecb-deep-10min · justhodl-ecb-rewalk-weekly
(SUN 03:15 UTC → walker {"agency":"ecb","reset_done":1,"budget":740,
"per":120,"cap_mb":150}; giants stay skipped — deep owns them).
Permanence: deny-Delete bucket policy on data/warm/*,
data/providers/*, data/raw/*, data/ciss* · lifecycle audited clean.

1.9 POLITENESS & LIMITS
≤24 parallel connections proved fine; sequential is always safe.
Timeouts: 60-240s metadata, 420s giant windows. No API key needed.

1.10 COPY-PASTE MINIMAL EXAMPLES
  # catalog (ALL agencies)
  curl -A "JustHodl Research raafouis@gmail.com" \
    "https://data-api.ecb.europa.eu/service/dataflow"
  # one series, full history
  curl -A "..." "https://data-api.ecb.europa.eu/service/data/\
CISS/D.U2.Z0Z.4F.EC.SS_CIN.IDX?format=csvdata"
  # whole flow, full history (fast flows only!)
  curl -A "..." ".../service/data/MIR?format=csvdata"
  # giant flow, one window, streamed
  curl -A "..." ".../service/data/BSI?format=csvdata\
&startPeriod=2010&endPeriod=2014" -o /tmp/w.csv
  # enumerate every series in a flow
  curl -A "..." ".../service/data/FM?format=csvdata\
&lastNObservations=1"
  # foreign-agency flow
  curl -A "..." ".../service/data/ESTAT,GFS?format=csvdata"

---------------------------------------------------------------------
PART 2 — FRED · api.stlouisfed.org
---------------------------------------------------------------------

2.0 THE KEY — THE BURNED LESSON
The key once sat hardcoded in this PUBLIC repo; strangers used it and
their traffic fed OUR 429s. The key is SSM-FIRST and never in code:
  /justhodl/fred-api-key   (primary)
  /justhodl/fred/api-key   (fallback path)
  env FRED_API_KEY         (last resort)
Dead-key rule: an HTTP 400 whose body names "api_key" means the key
is dead/rotated → halt ALL FRED calls for this invoke immediately;
never burn rate budget on a dead key.

2.1 RATE LIMIT & AIMD PACING (justhodl-fred-catalog, "priority-drain v2")
FRED hard limit ≈ 120 req/min. House pacing: start 70/min · +6/min
after each clean 120-call window · ×0.6 (floor 24/min) + 20s cooldown
on ANY 429 · ceiling from SSM /justhodl/fred/rate-ceiling (default
100). SINGLE-FLIGHT IS MANDATORY — ETag lease + reserved concurrency;
two parallel FRED pullers 429-storm each other into uselessness.
429-storm rule: sustained throttles in one invoke → STOP that invoke
and let the chain/cron resume later.

2.2 THE BUDGET WALL (ops 4575 crash — never sleep past the deadline)
Retry backoffs once slept minutes past the Lambda timeout mid-write:
state unsaved, lease wedged, chain dead. Rule: every sleep checks the
hard deadline and RAISES (BudgetExhausted) instead of sleeping past
it. Save state incrementally so a killed invoke loses nothing.

2.3 DISCOVERY — THE CATEGORY-TREE TRAP (ops 4560)
fred/category/series returns only DIRECT members of that category.
You MUST recurse fred/category/children from every top-level root or
you silently miss most of the catalog. Tree cache:
data/_state/fred-categories.json. Priority roots (ops 4553):
22 Interest Rates · 15 Exchange Rates · 24 Monetary Data ·
46 Financial Indicators · 23 Banking · 32360 Business Lending ·
32145 Foreign Exchange Intervention. full_catalog scope = every
top-level root, same drain.

2.4 THE QUEUE — NEVER RE-PAGE FROM OFFSET 0
Build a ONE-TIME popularity-DESC queue ledger
(data/_state/fred-queue.json.gz) and consume each category to
exhaustion once. The old model re-paged giant categories from
offset 0 every invoke and burned the entire rate budget on
re-listing instead of real observation fetches.

2.5 OBSERVATIONS & FRESHNESS
  https://api.stlouisfed.org/fred/series/observations?series_id={ID}
  &api_key={KEY}&file_type=json
Full history by default. FRESH_DAYS=90: series with no observation in
90 days are imported metadata-only (stale filter).

2.6 SELF-CHAIN PATTERN (~100% duty cycle)
While progressing, the invoke async re-invokes itself at budget end;
the 5-min cron is only the watchdog. Live SSM knobs, zero redeploys:
/justhodl/fred/paused=1 halts everything ·
/justhodl/fred/min-popularity=N cuts the popularity tail.

2.7 STORAGE & STATUS SEMANTICS
  data/providers/fred-scoped/series/page-NNNN.json   imported rows
  data/warm/fred-catalog/series-meta/page-NNNN.json  metadata pages
  data/_state/fred-scoped-import.json                state
COMPLETE_WITH_LEAKS = queue fully drained; "leaks" = series that are
unreachable via the category tree. 92 ICE BofA series are
independently cross-validated via the Trading Economics FRED mirror
(justhodl-te-fred-mirror, 100.0% agreement). Current scale: 282,141
catalog series · 277,453 banked · 4169/4169 categories.

---------------------------------------------------------------------
PART 3 — UNIVERSAL DOCTRINE (any API, forever)
---------------------------------------------------------------------
1. NEVER CLAIM — READ BACK. A pull "succeeded" only when the S3
   object is re-read and its content verified.
2. Honest User-Agent with a contact. Always.
3. STATED, NOT SILENT. Truncation, failures, empties: ledgered in a
   permanent, named artifact — never swallowed.
4. Before writing ANY new puller: read this playbook, then check the
   840+ existing lambdas and the state files above. The data you
   want probably already lands every 10 minutes.
5. When a provider "blocks" you: isolate the EXACT failing call
   (endpoint × header × param). A sibling engine succeeding against
   the same host is your diagnosis map — diff the two requests.
6. Keys live in SSM, never in code, never in a public repo.
7. Giant datasets: stream to disk, slice by time, part + manifest.
   RAM caps always lose eventually.
Evidence trail: aws/ops/reports/4893-4899 · AUTONOMY.md ·
docs/SESSION_CLAIMS.md.
