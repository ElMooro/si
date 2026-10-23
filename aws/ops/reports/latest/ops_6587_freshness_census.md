# ops 6587 - freshness census: files vs schedules vs the data inside (read-only)

**Status:** success  
**Duration:** 6.7s  
**Finished:** 2026-10-23T00:03:58+00:00  

## Data

| dated | day_old_or_more | files | head_ok | on_schedule | recent |
|---|---|---|---|---|---|
| 336 | 129 | 684 | 667 | 567 | 548 |

## Log
- `00:03:51` engines with an aligned provider and declared json outputs: 450; files to check: 684
- `00:03:58` HEAD ok 667, missing/err 17 (["ClientError"])
## per provider

- `00:03:58` | provider | engines | files | written in the last 2 days | of those, within 45 min of the trigger | data age at write: median d | a day old or more |
- `00:03:58` | FRED | 138 | 214 | 180 | 178 | 0 (of 109 dated) | 45 |
- `00:03:58` | SEC EDGAR | 34 | 56 | 37 | 36 | 0.0 (of 18 dated) | 6 |
- `00:03:58` | ECB | 26 | 50 | 40 | 38 | 0.0 (of 20 dated) | 8 |
- `00:03:58` | NY Fed | 18 | 32 | 25 | 25 | 0.0 (of 20 dated) | 7 |
- `00:03:58` | Treasury | 16 | 30 | 22 | 16 | 0.0 (of 12 dated) | 2 |
- `00:03:58` | OFR | 11 | 24 | 19 | 19 | 0 (of 13 dated) | 3 |
- `00:03:58` | Cboe | 16 | 25 | 22 | 22 | 0 (of 17 dated) | 5 |
- `00:03:58` | BLS | 5 | 13 | 12 | 12 | 0.0 (of 6 dated) | 0 |
- `00:03:58` | BEA | 3 | 7 | 7 | 7 | 0.0 (of 4 dated) | 0 |
- `00:03:58` | Census | 3 | 5 | 5 | 5 | 0.0 (of 2 dated) | 0 |
- `00:03:58` | Eurostat | 9 | 15 | 14 | 14 | 0 (of 9 dated) | 3 |
- `00:03:58` | BIS/IMF/WB/OECD | 19 | 29 | 21 | 21 | 0 (of 7 dated) | 1 |
- `00:03:58` | EIA | 3 | 4 | 4 | 4 | 0 (of 3 dated) | 1 |
- `00:03:58` | FINRA | 7 | 11 | 9 | 9 | 0.0 (of 4 dated) | 1 |
- `00:03:58` | Polygon/Massive | 122 | 203 | 161 | 146 | 0 (of 77 dated) | 28 |
- `00:03:58` | FMP | 257 | 354 | 300 | 292 | 0 (of 155 dated) | 52 |
- `00:03:58` | CFTC | 13 | 20 | 15 | 14 | 9 (of 7 dated) | 4 |
- `00:03:58` note: the fleet was frozen 2026-10-17 .. 10-21 00:30 UTC; a file is representative only once its engine has run after the thaw (age <= 48 h)
- `00:03:58` files not written in the last 2 days: 119 (engines: justhodl-13f-clone-alpha, justhodl-13f-positions, justhodl-activist-filings-scanner, justhodl-alpha-calibrator, justhodl-alpha-compass, justhodl-backfill-orchestrator, justhodl-bagger-engine, justhodl-bis-crossborder, justhodl-bond-desk, justhodl-buyback-scanner, justhodl-capital-flow, justhodl-capital-inflows, justhodl-capitulation, justhodl-carry-surface, justhodl-china-liquidity, justhodl-chokepoint, justhodl-cot-tracker, justhodl-credit-stress, justhodl-cycle-clock, justhodl-daily-report-v3, justhodl-dark-pool, justhodl-edge-discovery, justhodl-equity-ftd, justhodl-estimate-revisions, justhodl-etf-constituents ...)
- `00:03:58` written recently but not within 45 min of the trigger (self-invoked, chained or event-driven writes): 15 engines: cftc-futures-positioning-agent, justhodl-auction-desk, justhodl-backfill-orchestrator, justhodl-bond-warroom, justhodl-deal-scanner, justhodl-earnings-iv-crush, justhodl-intraday-pulse, justhodl-liquidity-inflection, justhodl-market-map, justhodl-opex-calendar, justhodl-options-analytics, justhodl-polygon-futures-curves, justhodl-polygon-fx-regime, justhodl-sellside-views, manufacturing-global-agent
- `00:03:58` ✅ census written (20 KB)
