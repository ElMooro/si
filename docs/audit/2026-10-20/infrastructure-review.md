# Infrastructure review, 2026-10-20 - is the fleet designed and running as it should?

Khalid: "review my entire system infrastructure and see if its designed and running as it should and whether we
should tune it up to save on cost and increase performance and accuracy. I fear my old engines are old and stale and
duplicated and some engines aren't even tied to the front end just wasting my money."

Evidence (all read from the account, ops 6546/6557/6560, and the repo's static dependency map): for each of the
926 Lambda functions - its declared S3 outputs (1,436 keys) and whether they exist and when they were last
written; which pages and engines reference them (static); who actually fetched them on a normal day
(2026-10-10 access logs, 15,000-object sample); which functions sit behind an API Gateway or Function URL;
invocations, errors and cost per day in the normal window (Sep 15 - Oct 11); code age and runtime.

## 1. The answer in one table

| Class | Engines | Normal $/day | After once-a-day | What it means |
|---|---|---|---|---|
| **Healthy** - outputs exist, written in the 14 days before the freeze, read by pages or engines or visitors | 774 | 15.96 | 4.43 | the platform as designed |
| API / Function-URL handlers (no S3 output) | 57 | 0.05 | 0.04 | 33 are invoked daily (wl-series-api, websocket, edgar-insiders, benzinga...), 24 had no call in 27 days |
| No output by design (coordinator, scheduler, loggers, notifiers, graders) | 21 | 0.14 | 0.03 | control plane |
| **Stale** - consumed, but not updated for more than 14 days before the freeze | 15 | 0.12 | 0.12 | see 3 |
| **Broken** - every declared output missing on S3 | 6 | 0.00 | 0.00 | see 3 |
| **Orphan** - runs, nobody reads the output | 5 | 0.01 | 0.00 | see 3 |
| **Idle** - no trigger, no invocation in 27 days | 21 | 0.00 | 0.00 | cost nothing; delete at will |
| (27 functions newer than the dependency map, incl. the governor and its canary) | 27 | 0.07 | 0.07 | 14 are the new macro feeds (ESMA, FDIC, EBA, FHFA, NY Fed, FAO, WGI, EPU...) |

**The fear is not borne out by the data.** Orphaned, broken, stale and idle engines together cost **$0.13 a day**
(0.5% of the bill). The money went to cadence (8,081 scheduled runs a day) and to two monitoring engines polling
AWS APIs - both fixed today. 86% of the fleet is wired to the front end or to other engines and was writing fresh
data when the freeze started.

Visitors on a normal day fetch 328 distinct data files; the most-read engines are market-tape (448 fetches/day,
referenced by 470 pages), backlog, estimate-revisions, inventory-drawdown, earnings-quality, share-flows,
master-ranker, forensic-screen, accumulation-radar, ai-rerating-radar, best-setups, bottom.

## 2. Design problems that are real

1. **Cadence was the cost model.** 174 fast triggers and 152 duplicate triggers for the same job (fixed: ops
   6552-6554, floor enforced by the deploy gate and the governor's sentinel).
2. **Monitoring that cost more than what it watched.** fleet-error-monitor polled GetMetricStatistics 783,000
   times a day ($7.47/day = 87% of the CloudWatch line); the AI engine called Cost Explorer at $0.01 a call on each
   run; the fusion bridge published 108 custom metrics ($29/month) that no alarm or dashboard reads. Fixed:
   monitor hourly then daily; CE cached; fusion metrics behind a flag, off. The governor now does fleet monitoring
   from ONE hourly read (~$8/month) and acts on it.
3. **A public bucket read directly by hundreds of pages** (4.70/day of egress normally, up to $10/day in October).
   Crawler blocked, Worker edge copy live; the remaining fix is routing the pages' data reads through the Worker
   (KHALID_ACTIONS.md 0c) - a page change, not an engine change.
4. **No per-engine budget and no self-healing** - fixed today: $50/month kill switch (v1.3.0) and the anomaly
   sentinel with auto-hold / auto-release (v1.4.0).
5. **Near-duplicate engines.** Seven engines (top-signals, bottom-signals, dxy-predict, macro-predict,
   liq-indicators, liquidity-reversal, blackswan-watch) each re-download the same warm series and all write the
   same `data/warm/blackswan/*` keys (duplicate writers); khalid-metrics / ka-metrics; email-reports-v2 /
   fmp-stock-picks-agent; tv-notes-crawler / tv-notes-ingest both write `data/tradingview-notes.json`. Cost today
   is small (< $0.05/day for the group) but every one of them is a second writer racing the first. Recommended:
   one shared puller for the blackswan warm set and the seven as light compute steps; retire one of each pair.
6. **Public Function URLs with no auth on 153 functions** - the 102 heavy ones are capped at 2 concurrent (ops
   6558); the right design is AWS_IAM auth or a Cloudflare-fronted route with a token for anything the site does
   not need anonymously. Not changed today: the site calls some of them directly.
7. **S3 noncurrent versions** are growing again since the lifecycle rule was disabled on Oct 19 (storage $0.64 ->
   $1.13/day in the frozen window); the October balloon (0.9 -> 145 TB) was exactly this mechanism. Khalid's call.

## 3. The specific lists

**Stale (consumed, but not updated for 14+ days before the freeze)** - a2a-bus (8 outputs, 70 days; 950
invocations/day, so it runs but its outputs are not what readers use), analytics-snapshot (43 d), fundamental-
census (15 d), share-flows (20 d - one of the most-read files on the site, written 2026-09-26), crypto-funding
(37 d), trade-ticket-monitor (18 d), wealth-plan (37 d), catalyst-clusters (19 d), feedback (165 d),
ml-predictions (176 d), position-sizer (16 d), ticket-ai-rationale (18 d), trade-journal (162 d), watchlist
(162 d), watchlist-debate (94 d). Of these, **share-flows** and **fundamental-census** are the ones a visitor
notices; the rest are personal/portfolio desks that only move when Khalid uses them.

**Broken (declared output missing on S3)** - transcript-indexer, ecb-auto-updater, ecb-data-daily-updater,
ici-flows, ofrapi, treasury-api (44 invocations/day writing a key that does not exist - probably a renamed
key; worth a look, costs nothing).

**Orphan (runs, nobody reads)** - sec-bulk, engine-robustness, hist-banker, kill-switch (a 2026-06 relic, not
the governor), macro-financial-intelligence (17 runs/day into a missing key).

**Idle (no trigger, no run in 27 days)** - activist-13d, ai-council, alert-router, asymmetric-scorer,
brain-sync, calibration-snapshot, daily-macro-report, earnings, ecb-full-catalog, email-reports,
eurostat-history, fleet-auditor, industry-case, insider-cluster-scanner, prompt-iterator, soma-cusip,
tape-truth, tenor-signal-interpreter, transcript-query, warroom-weights, macro-report-api - plus 24 API/URL
handlers with no call in 27 days (aiapi-market-analyzer, autonomous-ai-processor, ecb, economyapi,
api-keys-admin, ask-desk, charts-agent, chat-api, ecb-proxy, history-api, investor-agents, portfolio-admin,
public-api-demo, push-api, screen-backtest, stock-ai-research, subscribe, treasury-proxy,
multi-agent-orchestrator, nyfed-financial-stability-fetcher, nyfed-primary-dealer-fetcher, nyfedapi-isolated,
openbb-system2-api, ultimate-multi-agent). Zero cost while idle; deleting them is housekeeping, not savings.

**Old code**: 908 of 926 functions on python3.12, 16 on 3.11, 1 on 3.13, 1 nodejs20. The oldest code dates
from May 2026 (autonomous-ai-processor, global-liquidity-agent-v2, google-trends-agent, multi-agent-orchestrator,
ultimate-multi-agent) - all in the idle set.

## 4. What I changed and what needs Khalid's word

Changed today (all verified): cadence floor; $600 cap; $50/engine kill switch; anomaly sentinel; CloudWatch
polling and fusion metrics; crawler block + edge copy; public-URL caps; alert channel re-sent.

Khalid's word needed (each a one-line reply):
- "switch off the orphans and the broken writers" (11 functions: disable their triggers; reversible);
- "delete the idle functions" (45 functions, zero cost, housekeeping);
- "merge the blackswan seven" (a design change: one puller + seven compute steps; a day of work, saves little
  money but removes seven duplicate writers);
- "route the pages through the worker" (egress; KHALID_ACTIONS.md 0c);
- the noncurrent-version lifecycle rule (0d in the action queue), and the two October loop engines under CAP.

## 5. Performance and accuracy

Once-a-day is now the floor; accuracy of intraday desks (options flow, news velocity, VIX curve, early movers)
is traded for cost by design - any of them can be exempted with a reason in `config/cadence-floor.json` and the
governor still caps it. The freshness monitor and the anomaly sentinel now make a stale or storming engine visible
within a day / an hour instead of at the end of the month. The honest accuracy gap is not in the engines but in
the two most-read files that were already stale before the freeze (share-flows 20 days, fundamental-census 15):
both should be first in line when the fleet thaws on Nov 1.
