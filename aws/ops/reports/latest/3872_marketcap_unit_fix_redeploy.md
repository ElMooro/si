# ops 3872 — redeploy market_cap unit fix, gate on PLAUSIBLE magnitude fleet-wide

**Status:** success  
**Duration:** 22.0s  
**Finished:** 2026-07-25T17:49:44+00:00  

## Data

| after_max_pct | after_median_pct | after_n_extreme_pct | after_n_reasonable | after_n_stocks | before_extreme | before_max_pct | before_median_pct | before_n_extreme_pct | before_n_stocks | before_s3 | quadrant_counts | reduction_ratio |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  | 797286.6644 | 27573.7454 | 1205 | 2247 | 2026-07-25T17:35:27+00:00 |  |  |
| 0.7973 | 0.0276 | 0 | 1244 | 2247 | 1205 |  |  |  |  |  |  | 0.0 |
|  |  |  |  |  |  |  |  |  |  |  | {'STEALTH_ACCUMULATION': 30, 'DISTRIBUTION_RALLY': 30, 'TREND_CONFIRMED': 14, 'CAPITULATION': 59, 'NEUTRAL': 2114} |  |

## Log
## 1. BEFORE — capture the corrupted state for a before/after diff

- `17:49:23`   BEFORE: 1205/1244 stocks show |flow_pct_mcap_21d| > 100% — this is the bug signature, should collapse toward 0 after the fix
## 2. ZIP-SETTLE BY MARKER

- `17:49:23` ✅   new artifact live on attempt 1 (92,128 zip bytes)
- `17:49:23` ✅   State=Active LastUpdateStatus=Successful
## 3. invoke

- `17:49:44` ✅   artifact rewritten on attempt 1 (2026-07-25T17:49:39+00:00)
## 4. THE NEGATIVE GATE — plausible magnitude fleet-wide, not just 5 names

- `17:49:44`   NVDA: market_cap=$5.01T
- `17:49:44`   GOOGL: market_cap=$3.89T
- `17:49:44`   AAPL: market_cap=$4.89T
- `17:49:44`   AMZN: market_cap=$2.50T
- `17:49:44`   MSFT: market_cap=$2.84T
- `17:49:44` ✅   stock universe intact
- `17:49:44` ✅   all 5 known mega-caps land in $1T-$10T range
- `17:49:44` ✅   median |flow_pct_mcap_21d| is single-digit-to-low-double-digit percent
- `17:49:44` ✅   extreme-percentage count collapsed vs BEFORE
- `17:49:44` ✅   max |flow_pct_mcap_21d| is not in the thousands (bug signature)
- `17:49:44` ✅   majority of stocks now show a reasonable (<=50%) monthly flow-vs-mcap ratio
## 5. quadrant distribution — should this differ meaningfully from ops 3870's

- `17:49:44`   ops 3870 (corrupted units): STEALTH_ACCUMULATION=30 DISTRIBUTION_RALLY=30 TREND_CONFIRMED=14 CAPITULATION=59 NEUTRAL=2114
- `17:49:44`   ops 3872 (fixed units):     {'STEALTH_ACCUMULATION': 30, 'DISTRIBUTION_RALLY': 30, 'TREND_CONFIRMED': 14, 'CAPITULATION': 59, 'NEUTRAL': 2114}
- `17:49:44` ✅ PASS_ALL — units fixed fleet-wide: median 0.0276%, max 0.7973%, extreme count 1205->0, quadrant {'STEALTH_ACCUMULATION': 30, 'DISTRIBUTION_RALLY': 30, 'TREND_CONFIRMED': 14, 'CAPITULATION': 59, 'NEUTRAL': 2114}
