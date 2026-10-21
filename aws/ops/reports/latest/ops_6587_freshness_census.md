# ops 6587 - freshness census: files vs schedules vs the data inside (read-only)

**Status:** success  
**Duration:** 1.8s  
**Finished:** 2026-10-21T04:03:09+00:00  

## Data

| dated | day_old_or_more | files | head_ok | on_schedule | recent |
|---|---|---|---|---|---|
| 19 | 7 | 689 | 671 | 214 | 26 |

## Log
- `04:03:07` engines with an aligned provider and declared json outputs: 455; files to check: 689
- `04:03:09` HEAD ok 671, missing/err 18 (["ClientError"])
## per provider

- `04:03:09` | provider | engines | files | written on schedule (<=45 min after trigger) | written in the last 2 days | data age at write: median d | a day old or more |
- `04:03:09` | FRED | 154 | 241 | 82 | 10 | 0.0 (of 8 dated) | 1 |
- `04:03:09` | SEC EDGAR | 34 | 56 | 19 | 3 | 0 (of 1 dated) | 0 |
- `04:03:09` | ECB | 24 | 46 | 5 | 1 | 0 (of 1 dated) | 0 |
- `04:03:09` | NY Fed | 18 | 32 | 7 | 0 | None (of 0 dated) | 0 |
- `04:03:09` | Treasury | 16 | 30 | 8 | 0 | None (of 0 dated) | 0 |
- `04:03:09` | OFR | 10 | 21 | 5 | 0 | None (of 0 dated) | 0 |
- `04:03:09` | Cboe | 16 | 25 | 10 | 6 | 0 (of 5 dated) | 0 |
- `04:03:09` | BLS | 5 | 13 | 3 | 2 | 0.0 (of 2 dated) | 0 |
- `04:03:09` | BEA | 3 | 7 | 0 | 0 | None (of 0 dated) | 0 |
- `04:03:09` | Census | 3 | 5 | 0 | 0 | None (of 0 dated) | 0 |
- `04:03:09` | Eurostat | 9 | 15 | 3 | 1 | 0 (of 1 dated) | 0 |
- `04:03:09` | BIS/IMF/WB/OECD | 18 | 26 | 15 | 3 | 0.0 (of 2 dated) | 0 |
- `04:03:09` | EIA | 3 | 4 | 0 | 0 | None (of 0 dated) | 0 |
- `04:03:09` | FINRA | 6 | 10 | 5 | 0 | None (of 0 dated) | 0 |
- `04:03:09` | Polygon/Massive | 120 | 198 | 73 | 9 | 5 (of 7 dated) | 5 |
- `04:03:09` | FMP | 257 | 354 | 113 | 5 | 3 (of 1 dated) | 1 |
- `04:03:09` | CFTC | 13 | 20 | 10 | 0 | None (of 0 dated) | 0 |
- `04:03:09` files not written in the last 2 days: 645 (engines: bea-economic-agent, bls-labor-agent, census-economic-agent, cftc-futures-positioning-agent, eia-energy-agent, justhodl-13f-clone-alpha, justhodl-13f-positions, justhodl-13f-price-divergence, justhodl-52wk-quality-breakout, justhodl-accumulation-radar, justhodl-activist-filings-scanner, justhodl-activity-nowcast, justhodl-ai-infra-stack, justhodl-ai-rerating-radar, justhodl-alert-backtester, justhodl-alpha-calibrator, justhodl-alpha-compass, justhodl-alpha-research, justhodl-analyst-actions, justhodl-analyst-consensus, justhodl-anomaly-detector, justhodl-apac-flows, justhodl-apac-leadlag, justhodl-asia-leads, justhodl-asset-compass ...)
- `04:03:09` written recently but not within 45 min of the trigger (self-invoked, chained or event-driven writes): 1 engines: justhodl-sdmx-walker
- `04:03:09` ✅ census written (19 KB)
