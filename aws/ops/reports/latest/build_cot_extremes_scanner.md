# Phase 2A — COT Positioning Extremes Scanner

**Status:** success  
**Duration:** 28.0s  
**Finished:** 2026-04-25T16:11:36+00:00  

## Data

| invoke_s | n_cluster_alerts | n_extreme | n_processed | zip_size |
|---|---|---|---|---|
| 20.4 | 0 | 1 | 29 | 17309 |

## Log
## 1. Set up justhodl-cot-extremes-scanner Lambda

- `16:11:08` ✅   Wrote source: 17,156B, 428 LOC
- `16:11:12` ✅   Created justhodl-cot-extremes-scanner
## 2. First run — full 5-year fetch (this takes 2-3 minutes)

- `16:11:36` ✅   Initial 5-year fetch completed in 20.4s
- `16:11:36` 
  Response body:
- `16:11:36`     n_processed               29
- `16:11:36`     n_extreme                 1
- `16:11:36`     n_cluster_alerts          0
- `16:11:36`     top_extremes: 1 items
- `16:11:36`       {'contract': 'HG', 'pct': 95.8, 'extreme': 'high'}
## 3. Read cot/extremes/current.json — full report

- `16:11:36`   Processed: 29/29
- `16:11:36`   Errors: 0
- `16:11:36`   At >95th or <5th percentile: 1
- `16:11:36`   Category cluster alerts: 0
- `16:11:36` 
  Top 10 most-extreme contracts (by deviation from 50th pct):
- `16:11:36`     HG    Copper                    pct= 95.8 ratio=+0.2379 4w=↑ ← HIGH EXTREME
- `16:11:36`     SB    Sugar                     pct=  5.3 ratio=-0.1786 4w=↓
- `16:11:36`     PL    Platinum                  pct= 90.1 ratio=+0.2569 4w=↑
- `16:11:36`     NG    Natural Gas               pct= 15.6 ratio=-0.0638 4w=↓
- `16:11:36`     ZW    Wheat                     pct= 82.5 ratio=-0.0186 4w=↓
- `16:11:36`     ZS    Soybeans                  pct= 81.0 ratio=+0.1874 4w=↓
- `16:11:36`     CL    Crude Oil WTI             pct= 21.3 ratio=+0.0503 4w=→
- `16:11:36`     SI    Silver                    pct= 29.3 ratio=+0.0768 4w=↓
- `16:11:36`     GC    Gold                      pct= 67.7 ratio=+0.2541 4w=↑
- `16:11:36`     CT    Cotton                    pct= 58.9 ratio=+0.0950 4w=↑
## 4. Verify cot/history/ — sample 3 contracts

- `16:11:36`     ES: 264 weekly bars, 2021-04-06 → 2026-04-21
- `16:11:36`     GC: 264 weekly bars, 2021-04-06 → 2026-04-21
- `16:11:36`     CL: 264 weekly bars, 2021-04-06 → 2026-04-21
## 5. Schedule cron(0 19 ? * FRI *) — Friday post-CFTC publish

- `16:11:36` ✅   Created rule cron(0 19 ? * FRI *)
- `16:11:36` ✅   Added invoke permission
- `16:11:36` Done
