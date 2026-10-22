# ops 6587 - freshness census: files vs schedules vs the data inside (read-only)

**Status:** success  
**Duration:** 4.5s  
**Finished:** 2026-10-22T01:36:38+00:00  

## Data

| dated | day_old_or_more | files | head_ok | on_schedule | recent |
|---|---|---|---|---|---|
| 326 | 125 | 684 | 667 | 566 | 531 |

## Log
- `01:36:33` engines with an aligned provider and declared json outputs: 450; files to check: 684
- `01:36:37` HEAD ok 667, missing/err 17 (["ClientError"])
## per provider

- `01:36:37` | provider | engines | files | written in the last 2 days | of those, within 45 min of the trigger | data age at write: median d | a day old or more |
- `01:36:37` | FRED | 138 | 214 | 173 | 169 | 0 (of 101 dated) | 43 |
- `01:36:37` | SEC EDGAR | 34 | 56 | 37 | 36 | 0.0 (of 18 dated) | 5 |
- `01:36:37` | ECB | 26 | 50 | 40 | 38 | 0 (of 19 dated) | 8 |
- `01:36:37` | NY Fed | 18 | 32 | 25 | 25 | 0.0 (of 20 dated) | 8 |
- `01:36:37` | Treasury | 16 | 30 | 22 | 16 | 0.0 (of 10 dated) | 2 |
- `01:36:37` | OFR | 11 | 24 | 19 | 19 | 0.0 (of 10 dated) | 2 |
- `01:36:37` | Cboe | 16 | 25 | 22 | 22 | 0 (of 17 dated) | 5 |
- `01:36:37` | BLS | 5 | 13 | 12 | 10 | 0.0 (of 6 dated) | 0 |
- `01:36:37` | BEA | 3 | 7 | 7 | 6 | 0.0 (of 4 dated) | 0 |
- `01:36:37` | Census | 3 | 5 | 5 | 4 | 0.0 (of 2 dated) | 0 |
- `01:36:37` | Eurostat | 9 | 15 | 14 | 14 | 0 (of 9 dated) | 3 |
- `01:36:37` | BIS/IMF/WB/OECD | 19 | 29 | 21 | 21 | 0 (of 7 dated) | 2 |
- `01:36:37` | EIA | 3 | 4 | 4 | 4 | 0 (of 3 dated) | 1 |
- `01:36:37` | FINRA | 7 | 11 | 9 | 9 | 1 (of 3 dated) | 2 |
- `01:36:37` | Polygon/Massive | 122 | 203 | 158 | 145 | 0 (of 73 dated) | 29 |
- `01:36:37` | FMP | 257 | 354 | 286 | 279 | 0.0 (of 146 dated) | 46 |
- `01:36:37` | CFTC | 13 | 20 | 15 | 14 | 8 (of 7 dated) | 4 |
- `01:36:37` note: the fleet was frozen 2026-10-17 .. 10-21 00:30 UTC; a file is representative only once its engine has run after the thaw (age <= 48 h)
- `01:36:37` files not written in the last 2 days: 136 (engines: justhodl-13f-clone-alpha, justhodl-13f-positions, justhodl-activist-filings-scanner, justhodl-alpha-calibrator, justhodl-alpha-compass, justhodl-backfill-orchestrator, justhodl-bagger-engine, justhodl-bank-stress, justhodl-bis-crossborder, justhodl-bond-desk, justhodl-buyback-scanner, justhodl-capital-flow, justhodl-capital-inflows, justhodl-capitulation, justhodl-carry-surface, justhodl-china-liquidity, justhodl-chokepoint, justhodl-cot-tracker, justhodl-credit-stress, justhodl-cross-asset-rv, justhodl-cycle-clock, justhodl-daily-report-v3, justhodl-dark-pool, justhodl-earnings-pead, justhodl-edge-discovery ...)
- `01:36:37` written recently but not within 45 min of the trigger (self-invoked, chained or event-driven writes): 16 engines: bea-economic-agent, bls-labor-agent, census-economic-agent, cftc-futures-positioning-agent, justhodl-auction-desk, justhodl-backfill-orchestrator, justhodl-bond-warroom, justhodl-deal-scanner, justhodl-intraday-pulse, justhodl-liquidity-inflection, justhodl-market-map, justhodl-opex-calendar, justhodl-options-analytics, justhodl-polygon-fx-regime, justhodl-positioning-analog, manufacturing-global-agent
- `01:36:38` ✅ census written (20 KB)
