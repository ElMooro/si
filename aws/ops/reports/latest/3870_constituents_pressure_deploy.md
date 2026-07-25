# ops 3870 — deploy daily-pressure + sector/price + quadrant, hard-gated

**Status:** success  
**Duration:** 32.3s  
**Finished:** 2026-07-25T17:35:31+00:00  

## Data

| before_generated_at | before_n_stocks | before_s3 | had_daily_field | n_stocks | n_with_daily_pressure | n_with_price_return | n_with_sector | n_with_zscore | quadrant_counts | top_aggregate_carries_new_fields |
|---|---|---|---|---|---|---|---|---|---|---|
| 2026-07-24T22:45:42.730007+00:00 | 2248 | 2026-07-24T22:45:43+00:00 | False |  |  |  |  |  |  |  |
|  |  |  |  | 2247 | 2247 | 1420 | 1421 | 1244 | {'STEALTH_ACCUMULATION': 30, 'DISTRIBUTION_RALLY': 30, 'TREND_CONFIRMED': 14, 'CAPITULATION': 59, 'NEUTRAL': 2114} | True |

## Log
## 1. BEFORE

## 2. ZIP-SETTLE BY MARKER — never invoke the old artifact (ops 3830 lesson)

- `17:34:59` ✅   new artifact live on attempt 1 (91,857 zip bytes)
- `17:35:10` ✅   State=Active LastUpdateStatus=Successful Memory=1024 Timeout=300
- `17:35:10` ✗   memory bump did not land (1024 < 1536) — config.json drift, deploy-lambdas may have stomped it
## 3. invoke (async — ~284 parallel FMP holdings fetches + a 15.9MB S3 parse)

- `17:35:31` ✅   artifact rewritten on attempt 1 (2026-07-25T17:35:27+00:00)
## 4. real-data gate — every claim, checked against the live artifact

- `17:35:31` ✅   stock universe present and not shrunk
- `17:35:31` ✅   daily pressure computed for effectively all stocks
- `17:35:31` ✅   sector known for a meaningful share (finviz+universe join actually ran)
- `17:35:31` ✅   price return known for a meaningful share
- `17:35:31` ✅   cross-sectional z computed (n>=30 gate passed on live data)
- `17:35:31` ✅   at least one stock landed in each directional quadrant
- `17:35:31` ✅   top_aggregate_exposure carries the new fields (hand-written list didn't drop them)
- `17:35:31` ✅   no NaN/inf leaked into JSON (would have failed json.loads above, but check explicitly)
## 5. spot-check one real stock end to end

- `17:35:31`   MU: sector=Technology mcap=$0.0B daily=$+35.3M 5d=$-674.5M 21d=$+4730.6M perf_m=-12.17% z_xsec=4.78 quadrant=STEALTH_ACCUMULATION
- `17:35:31` ✅   MU carries a full real record
- `17:35:31` ✅ PASS_ALL — 2247 stocks, daily/weekly/monthly pressure live, sector 1421/2247 · price 1420/2247 · quadrant {'STEALTH_ACCUMULATION': 30, 'DISTRIBUTION_RALLY': 30, 'TREND_CONFIRMED': 14, 'CAPITULATION': 59, 'NEUTRAL': 2114}
