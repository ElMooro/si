# Data-flow audit - how the data gets refreshed, what it costs, what "current" would cost

Khalid, 2026-10-20 23:39 ET: "how does my data get refreshed; check how I'm doing it currently; suggestions to
improve it; is it going to change my budget; if I want it to stay current how much would it cost me - the only one
would be Polygon, every other one once a day is more than enough and honestly even once a week might be fine; check
and audit first; don't do anything."

Read-only. Nothing was changed. Sources: the repo at a44c654a (schedule + fan-out manifests after the pipeline-order
apply, every engine's source, `engine-manifest.json`, `docs/DATA_PROVIDERS.md`, the Worker), the ops 6546/6557 cost
data (per-engine measured runtimes and the once-a-day projection), the Massive/Polygon price page (2026-10-21).
Method and caveats at the end.

## 0. Bottom line

* **Today**: 803 scheduled engines fire 842 times a day (once each; 38 already weekly, 4 monthly). 584 pull a
  provider themselves with their own key and write their own JSON to `s3://justhodl-dashboard-live/data/...`; 468
  read other engines' files (311 do both, 157 are pure compute; ordered since this morning); 62 are monitors and
  self-contained jobs. Pages read everything through
  `https://justhodl.ai/data/*` (Cloudflare Worker, edge TTL 30 s - 6 h by path pattern, default 5 min) and 96 pages
  re-poll on a timer, mostly every 5 minutes. A daily freshness monitor checks every file against a 26-hour default.
* **What the refresh costs**: the whole scheduled fleet is **$3.58/day = $107/month** of Lambda + S3 requests at
  once a day. Pulling providers is the cheap part: all Polygon-sourced engines together $7.6/month, FMP $5.7, FRED
  $2.3, the slow macro sources (BLS/BEA/Census/BIS/IMF/OECD/Eurostat/EIA) $6. The money goes to a few heavy walkers
  and monitors (`justhodl-ai` $26, `justhodl-symdir` $26, `sdmx-walker` $8, cost-anomaly + fleet-error-monitor $7)
  and to the long tail of scrapes/Yahoo fallbacks ($22). The other $2.3/day of the bill (storage, egress, logs,
  DynamoDB) does not move with cadence at all.
* **Weekly instead of daily** is right for the monthly/quarterly sources (release-calendar data) but it is a
  freshness-semantics decision, not a budget one: weekly for the ~80 engines whose every input is slow saves
  **$8/month**; weekly for *everything* except Polygon saves **$59/month** and makes FRED rates, SEC filings, NY Fed /
  Treasury / OFR plumbing, Cboe and crypto up to a week old - the opposite of "not stale".
* **Polygon "current"**: the light Polygon feeds (tape reader, intraday pulse, futures curves, FX regime, options
  flow scanner, gamma, money-flow state, options flow/analytics, crypto ETF flows, portfolio snapshot, trade-ticket
  monitor) polled **every 5 min during the US session cost about $2/month** of AWS; **every minute about $9/month**
  (+ $1-3 of S3/edge). Three heavy Polygon research jobs (dealer-gex 129 s x 2 GB, market-internals 120 s x 3 GB,
  polygon-options-flow 439 s x 4 GB) are not polling candidates as they are ($65/month at 5-min, $320 at 1-min) -
  they stay end-of-day unless rebuilt incrementally. Plan side: the stocks data is **15-minute delayed on the Starter /
  Developer tiers ($29 / $79)**; **real-time is the Advanced tier at $199/month** (non-pro, display use). So "current"
  means "15 minutes behind" unless the plan changes - the polling cadence cannot buy what the plan does not deliver.
* **Budget**: November is projected at ~$6.50/day = ~$195/month. Weekly-by-source-class takes it to ~$182; adding
  the Polygon intraday lane at 5-min brings it back to ~$185, at 1-min ~$194. Either way far under $600. The
  real lever outside AWS is the provider subscriptions themselves (FMP Ultimate, Polygon Options $29 + Futures $29
  + ETF Global ~$297 per the inventory) - I cannot see those invoices from here.
* **The biggest freshness win costs nothing**: by publication-time arithmetic roughly **300 of 740 daily pulls
  run before their source publishes for the day** (83 of 171 FRED pulls, 100 of 273 FMP, 20 of 35 SEC, 19 of 36
  ECB, 10 of 17 OFR...) and therefore show the previous day's print until the next run. A second planner pass -
  the same machinery as this morning's pipeline order, with the outside world as the writer - would move them after
  their source's publication time inside the same window. Recommended first, before any cadence change.

## 1. How the data is refreshed today

```
 provider APIs (31 named + ~60 scrapes)         EventBridge: 781 cron triggers + 13 fan-out ticks (60 members)
        |  key from SSM per engine                        |  once a day each (cadence floor, ops 6554)
        v                                                 v
 803 scheduled engines (Lambda) ------------------> s3://justhodl-dashboard-live/data/*  (1.28 M current files,
   584 pull a provider directly                       10 old copies kept, replicated to the sealed archive)
   468 read other engines' files (157 pure compute)          |
   (2,117 reader->writer edges, ordered ops 6585)           v
                                               Cloudflare Worker justhodl.ai/data/*  (edge TTL by path:
                                               live/intraday 30 s, hourly/news 5 min, daily 1 h,
                                               reference 6 h, default 5 min; large publications edge-copied)
                                                            |
                                                            v
                                               158 pages (GitHub Pages, built daily) - 96 re-poll on a timer
                                               (17 x 5 min, 16 x 300 s, 6 x 60 s, 4 x 30 s ...)
 observers: fleet-freshness-monitor 07:57 UTC (26 h default, per-key overrides), data-census 12:45 UTC,
            spend governor hourly (cost, cadence, storage), governor-alerts relay hourly (GitHub issues)
 not in the flow: the event bus / coordinator (off), SageMaker factory (off), Function URLs (on demand)
```

Firings by UTC hour (842/day): the day has three humps - 11:00-14:00 (269 firings: pre-market US), 21:00-23:00
(195: post-close) and a flat overnight. Only 43 triggers are still `rate(1 day)` (their phase is whatever time
they were last re-created).

What each class of source is pulled with:

| Source (natural cadence) | Scheduled engines | Firings/day | $/month (Lambda+S3, split by provider) |
|---|---|---|---|
| Polygon/Massive (intraday tape; EOD aggregates) | 126 (+symdir) | 133 | 7.63 |
| FMP (EOD prices/ratios, quarterly statements, daily calendars) | 272 | 280 | 5.70 |
| FRED (daily/weekly/monthly by series) | 167 | 172 | 2.26 |
| SEC EDGAR (filings every business day; 13F quarterly) | 36 | 37 | 0.69 |
| ECB (daily FX/rates 16:00 CET; monthly statistics) | 26 | 32 | 3.28 |
| NY Fed (SOFR 08:00 ET, RRP 13:15 ET, primary dealers weekly) | 21 | 21 | 0.63 |
| Treasury FiscalData/TreasuryDirect (daily; auctions 13:00 ET) | 17 | 20 | 0.37 |
| OFR (FSI daily; HFM/MMF monthly) | 14 | 14 | 0.34 |
| Cboe (EOD) | 17 | 17 | 0.08 |
| Coinbase/CoinGecko/Deribit/CMC (real-time crypto) | 33 | 34 | 0.55 |
| CryptoQuant (daily on-chain) | 2 | 2 | 0.10 |
| FINRA (daily short volume; SI twice monthly) | 7 | 8 | 0.14 |
| Trading Economics (daily calendar) | 11 | 11 | 0.16 |
| News (NewsAPI/GDELT/Google) | 14 | 15 | 0.28 |
| TradingView (chart bars, 02:30 UTC) / Alpha Vantage / Quiver | 11 | 11 | 0.22 |
| **CFTC (weekly, Friday 15:30 ET)** | 13 | 15 | 0.29 |
| **BIS / IMF / World Bank / OECD / DBnomics (monthly, quarterly)** | 18 | 24 | 3.49 |
| **Eurostat (monthly, quarterly)** | 9 | 14 | 2.11 |
| **BLS (monthly releases 08:30 ET)** | 5 | 5 | 0.10 |
| **EIA (weekly Wed/Thu; monthly)** | 4 | 4 | 0.01 |
| **BEA / Census (monthly, quarterly)** | 6 | 7 | 0.10 |
| **OpenFIGI (static reference)** | 3 | 4 | 0.09 |
| Yahoo Finance fallback (unofficial) | 51 | 57 | 9.77 |
| Stooq / multpl / central-bank and statistics-office scrapes | 52 | 59 | 11.85 |
| Internal compute (no external source) | 133 | 140 | 8.53 |
| `justhodl-symdir` (symbol directory walker; references every provider; 5.9 chained runs/day) | 1 | 6 | 26.01 |
| other single-host scrapes (TIC, DTCC, ICMA, BCB, BCRP, Taiwan/Singapore stats...) | ~45 | ~50 | 17.85 |
| **Scheduled fleet** | **803** | **842** | **~$107** |

(Bold = monthly/quarterly sources pulled daily today. The LLM voices - 64 engines, Anthropic credits exhausted and
GLM returning 429 since September - and the 171 engines that also send Telegram alerts are inside the rows above.)

The 15 scheduled engines that cost the most - the refresh money is here, not in the provider pulls:

| Engine | $/month | Source | Run | What |
|---|---|---|---|---|
| `justhodl-ai` | 26.35 | internal + scrapes | 23 s x 3 GB, 3.3 runs/day | AI front window (Cost Explorer calls in the projection; the endpoint is gone since ops 6578, so this will land lower) |
| `justhodl-symdir` | 26.01 | every provider | 1 s x 6 GB, 5.9 chained runs/day | symbol directory walker |
| `justhodl-sdmx-walker` | 8.12 | BIS/Eurostat/OECD | 1 s x 10 GB, 3.1 runs/day | full-dataset SDMX walks |
| `justhodl-polygon-options-flow` | 3.65 | Polygon | 439 s x 4 GB | option-chain research |
| `justhodl-cost-anomaly` | 3.64 | AWS Cost Explorer | 36 s x 0.5 GB | $0.01 per CE call |
| `justhodl-fleet-error-monitor` | 3.55 | CloudWatch | 44 s x 0.5 GB | fleet error sweep |
| `justhodl-boj-full` | 2.52 | BoJ | 2 s x 1.5 GB | BoJ warehouse |
| `justhodl-data-census` | 2.13 | internal | 684 s x 3 GB | catalog of every published value |
| `justhodl-etf-constituents` | 1.68 | FMP + Polygon | 218 s x 3 GB | dated ETF holdings |
| `justhodl-thesis-engine` | 1.34 | many | 632 s x 3 GB | 207 watchlists as theses |
| `justhodl-ticker-360` | 1.14 | internal | 548 s x 1 GB | cross-engine ticker index |
| `justhodl-research-backtest` | 1.13 | FMP | 550 s x 2 GB | research track record |
| `justhodl-analytics-snapshot` | 1.01 | internal | 173 s x 0.5 GB | research/insider roll-up |
| `justhodl-provider-catalog` | 0.93 | internal | 384 s x 1 GB | data.html catalog |
| `justhodl-worldbank-full` | 0.92 | World Bank | 236 s x 1 GB | World Bank warehouse |

## 2. Is the data as fresh as the cadence allows? (the outside world as the writer)

This morning's pipeline order aligned readers with writers *inside* the fleet. The same question against the
providers' publication times - computed from each trigger's UTC time against when the source publishes (EDT) -
says a large share of daily pulls run **before** the day's print and so publish yesterday's value for a whole day:

| Source | Publication (UTC, EDT) | Daily pulls | Run after the print | Run before it (a day late) |
|---|---|---|---|---|
| FRED | H.15 rates ~21:15; most monthly series 12:30 | 171 | 88 | **83** |
| FMP | EOD prices from ~21:30; filings intraday | 273 | 173 | **100** |
| SEC EDGAR | filings accepted until 02:00; daily index after | 35 | 15 | **20** |
| ECB | reference rates 14:15 | 36 | 17 | **19** |
| OFR | FSI in the US evening | 17 | 7 | **10** |
| Polygon | grouped-daily after 21:00; session 13:30-20:00 | 136 | 93 | 43 |
| NY Fed | SOFR 12:00, RRP 17:15 | 25 | 18 | 7 |
| Treasury | auction results 17:00; debt data overnight | 23 | 15 | 8 |
| Cboe | EOD ~21:30 | 16 | 12 | 4 |
| CFTC | Friday 19:30 | 22 (14 of them daily pulls of a weekly file) | - | - |

These are arithmetic on assumed publication times, not measurements of the files (I cannot read S3 LastModified
from here). The first step of any fix is a one-off read-only census of `data/*.json` ages against each file's
expected cadence; the numbers above say where to look. Note the pipeline-order planner already contains everything
needed: treat each provider as a pinned "writer" firing at its publication time and let the readers move later inside
their window.

## 3. "Once a week is fine" - what it would and would not do

Classifying every scheduled engine by the **fastest source behind it** (its own providers plus everything upstream
in the reader->writer graph):

| Class | Engines | $/month | Weekly would cost freshness? |
|---|---|---|---|
| Downstream of Polygon (directly or through the graph) | 412 | 48.6 | yes - half the fleet consumes tape-derived files |
| Daily sources only (FRED, SEC, ECB, NY Fed, Treasury, OFR, FMP, Cboe, crypto, news...) | 312 | 49.5 | yes - the plumbing, filings and prices pages would show up to 7-day-old data |
| Slow sources only (BLS, BEA, Census, BIS/IMF/WB/OECD, Eurostat, EIA, OpenFIGI, CFTC) | 11 | 1.3 | no - the source changes monthly/weekly |
| No external source and no upstream found (self-contained compute / monitors) | 68 | 8.0 | mostly no |

Scenarios (Lambda + S3 requests only; the $2.3/day platform baseline is unchanged in all of them):

| Scenario | Engines moved to weekly | Saves | What goes stale |
|---|---|---|---|
| A. weekly for the slow-source and self-contained engines | 79 | **$8/month** | nothing that matters: monthly data read weekly |
| A+. A plus CFTC on Friday evening only, the 13F engines weekly off-season, `sdmx-walker` weekly | ~110 | **$12-15/month** | nothing: all weekly/monthly/quarterly sources |
| C. weekly for everything not downstream of Polygon | 391 | $50/month | FRED, SEC, ECB, NY Fed, Treasury, OFR, crypto: up to a week old |
| B. weekly for everything but the direct Polygon pullers | 676 | $59/month | as C plus every compute engine on the tape side |

My read: **A+ is the right cadence** - it matches the sources, cuts provider calls on APIs that rate-limit, and
removes ~110 pointless daily rewrites (each daily run of unchanged data creates a new S3 version and invalidates the
edge copy for nothing). It is worth ~$1 a week. C and B save real money only on paper: the fleet's purpose is the
daily plumbing/filings/prices read, and a week-old FRED or EDGAR file is exactly the "stale data" problem.

Two things must move together with any weekly change: `data/_freshness-manifest.json` (the monitor alarms past
26 h by default; weekly files need per-key `max_age_h` ~ 180) and the Worker TTL for those paths (6 h instead of
5 min - fewer origin pulls, no staleness since the file changes weekly).

## 4. Polygon "current" - what it means and what it costs

Which Polygon data is intraday by nature (everything else Polygon-sourced - grouped daily, ETF holdings, reference
tickers - is end-of-day data and stays daily):

| Engine (today once a day) | Measured run | AWS cost at 5-min polling, US session | at 1-min |
|---|---|---|---|
| `justhodl-tape-reader` (institutional footprint from the snapshot) | 8.7 s x 512 MB | $0.12/mo | $0.60 |
| `justhodl-intraday-pulse` | 1.0 s x 512 MB | $0.01 | $0.07 |
| `justhodl-polygon-futures-curves` (10-min delayed futures) | 12.2 s x 512 MB | $0.17 | $0.84 |
| `justhodl-polygon-fx-regime` | 6.4 s x 512 MB | $0.09 | $0.44 |
| `justhodl-options-flow-scanner` | 12.2 s x 2 GB | $0.67 | $3.33 |
| `justhodl-options-gamma` | 1.5 s x 1 GB | $0.04 | $0.21 |
| `justhodl-money-flow-state` | 10.8 s x 1 GB | $0.29 | $1.47 |
| `justhodl-options-flow` / `-analytics` | 10 s x 1 GB, 2.4 s x 1 GB | $0.35 | $1.72 |
| `justhodl-crypto-etf-flows`, `portfolio-snapshot`, `trade-ticket-monitor` | 0.4-1.7 s | $0.04 | $0.18 |
| **Light feeds together** | | **~$1.8/month** (+$0.5 S3 PUTs, +edge) | **~$9/month** (+$2-3) |
| `justhodl-dealer-gex` | 129 s x 2 GB | $7 | $35 |
| `justhodl-market-internals` | 120 s x 3 GB | $10 | $48 |
| `justhodl-polygon-options-flow` (full option-chain research) | 439 s x 4 GB | $48 | $240 |
| **Heavy research jobs as they are** | | **$65/month** - not a polling candidate | $320 |

How a "current" lane would work inside the guardrails, if you want it (nothing is built):

1. The light feeds get an **exemption with a reason in `config/cadence-floor.json`** (the floor rule says every engine
   runs at most once a day; the exemption list exists for exactly this; the governor's cadence sentinel otherwise
   retimes them back to daily). They run on their own 5-min (or 1-min) schedule **13:30-20:00 UTC, Mon-Fri only**;
   the $50/engine CAP stays as the brake (none of the light feeds reaches $4/month even at 1-min; a heavy job at
   1-min would hit the cap in a few days - which is the point of the cap).
2. They write **compact files** (tens of KB, not the full-market snapshot): the edge TTL rule already gives
   `live|intraday|...` paths 30 s; origin egress is one pull per TTL per file, i.e. nothing. The pages that
   matter already poll on 30-60 s timers.
3. The three heavy jobs stay end-of-day. If intraday dealer gamma / internals are wanted, they need an incremental
   rebuild (keep the chain in memory between minutes, update deltas) - a project, not a schedule change.
4. **The plan decides the latency, not the schedule**: the stocks tape on the Starter ($29) and Developer ($79)
   tiers is 15-minute delayed; only **Stocks Advanced ($199/month, "non-pros only", real-time)** removes the delay.
   Options and futures tiers are separate (the inventory lists Options $29 and Futures $29; futures aggregates
   are 10-min delayed). A real-time feed would also change the architecture (a WebSocket consumer needs a process
   that stays up; on Lambda that is a chain of 15-minute runs, ~$15/month of AWS for the session hours) - and
   real-time redistribution on a public site has exchange display-licence terms to read first.

So: **15-min-delayed-but-live costs $2-12/month more on AWS**; **true real-time costs a $199/month plan plus ~$15 of
AWS** and a design pass.

## 5. Suggestions, ranked by what they buy (none applied)

1. **Release-aligned pull times** (freshness, $0). The pass described in section 2: treat each provider as a
   pinned writer at its publication time; move its pullers later inside their window (same solver as the pipeline
   order; a dry run first). Expected effect: ~300 daily pulls one day fresher. Prerequisite: a one-off read-only
   census of file ages vs cadence to confirm the arithmetic.
2. **Cadence by source class** (scenario A+, ~$12-15/month, fewer no-op rewrites, fewer provider calls): weekly
   for BLS/BEA/Census/BIS/IMF/WB/OECD/Eurostat/EIA/OpenFIGI pullers and the compute engines that read only them;
   CFTC Friday evening; 13F weekly outside the filing season; `sdmx-walker` weekly. With the freshness manifest and
   Worker TTLs updated in the same change.
3. **Polygon intraday lane** (section 4): 12 light feeds at 5 min during the session, exempt with reasons, compact
   files, heavy jobs stay EOD; decide the plan tier separately.
4. **Pull once, read many** (consistency and rate limits more than money): 272 FMP engines and 167 FRED engines call
   the provider themselves, often for overlapping series at different minutes, so two pages can disagree on the
   same number and the 14:00 UTC FMP 429 storm recurs. Your own doctrine (2026-09-02) is warehouse-first: one raw
   puller per provider/series family writes `data/providers/<slug>/...`, consumers read S3. Worth doing provider by
   provider, FMP first.
5. **Write only when the content changed**: 1,809 `put_object` sites, 74 engines with any "unchanged" check, 266
   conditional-request references. A shared `put_if_changed` (compare the body hash to the current object's ETag)
   would stop identical daily rewrites: fewer versions, no needless edge invalidation, and `LastModified` becomes a
   true freshness signal for the monitor instead of "the engine ran".
6. **Retire the Yahoo fallback pulls** (51 engines, $10/month, unofficial endpoint that breaks without notice) where
   Polygon/FMP already cover the field; keep the chart's labelled fallback only.
7. **The LLM voices**: 64 engines run daily against exhausted Anthropic credits and a 429-ing GLM, producing the
   deterministic fallback text - fund the API or pause the voices; either way the daily run is not refreshing data.
8. **Page timers**: 96 pages poll on 30 s - 15 min timers against files that change once a day (harmless at the
   edge, misleading to a reader). A per-feed cadence hint (the Worker already knows the TTL) would let pages show
   "updated daily at 22:05 UTC" instead of spinning.

## 6. The budget answer

| | $/day | $/month |
|---|---|---|
| November projection today (cost audit §7, SageMaker off) | 6.50 | ~195 |
| ... of which the scheduled refresh (Lambda + S3 requests) | 3.58 | 107 |
| ... of which platform (storage 1.13, egress ~0.6, logs/alarms, DynamoDB, secrets...) | ~2.3 | ~70 |
| A+ cadence by source class | -0.45 | **~182** |
| + Polygon light feeds every 5 min | +0.08 | **~185** |
| + Polygon light feeds every minute instead | +0.40 | **~194** |
| + Stocks Advanced real-time plan (outside AWS) | | +199 |

Under the $600 cap in every case; the governor's $50/engine cap and the exemption list are the controls that
keep an intraday lane from becoming October's loop.

## 7. Method and caveats

* Provider attribution: hostnames in each engine's source plus the shared modules it imports, mapped to 31 named
  providers; an engine's projected cost (ops 6557 model: measured mean duration x memory at list price + S3
  requests, at one run a day) is split equally across its data providers. `justhodl-symdir` references every
  provider as a catalog and is shown on its own. Monthly figures are 30 x daily.
* "A day late" (section 2) uses assumed publication times in EDT; after Nov 1 the UTC offsets shift by an hour.
  It is a map of where to look, not a measurement; the census proposed in suggestion 1 is the measurement.
* Intraday polling costs assume the engine's measured mean duration and memory, 78 (5-min) or 390 (1-min) runs
  per US session, 21 sessions a month, Lambda compute at $0.0000166667 per GB-second.
* Prices: Massive/Polygon stocks tiers from massive.com/pricing on 2026-10-21 (Basic $0 EOD / Starter $29 /
  Developer $79, both 15-minute delayed / Advanced $199 real-time, non-pros only); options and futures tier prices
  were not on the fetched page - the inventory's $29/$29 are from 2026-09-15.
* Not measured from here: actual S3 file ages, version churn of identical rewrites, provider rate-limit incidents,
  the provider invoices. Each is a read-only ops script away.
