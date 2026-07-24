# ops 3788 — revenue growth + S&P500 membership (v4.2)

**Status:** success  
**Duration:** 36.0s  
**Finished:** 2026-07-24T00:18:58+00:00  

## Data

| invoke_seconds | invoke_status | scored | sp500_members | with_growth |
|---|---|---|---|---|
| 14.8 | 200 |  |  |  |
|  |  | 3176 | 490 | 1242 |

## Log
## G0

- `00:18:22` ✅ G0.v411 :: engine at v4.1.1
- `00:18:22` ✅ G0.inc :: annual income statement (limit=10) already fetched per name
- `00:18:22` ✅ G0.rev_pick :: revenue/currency block present to extend
- `00:18:22` ✅ G0.cap_carry :: cap_rows carry block present
- `00:18:22` ✅ G0.no_growth :: growth not yet computed
## [1] Revenue growth from the statement already fetched

- `00:18:22` ✅ P1.anchor :: anchor unique
## [2] Carry growth + gm_level through cap_rows

## [3] Growth tiers (absolute) + S&P500 join

- `00:18:22` ✅ P3.anchor :: percent block anchor unique
- `00:18:22` ✅ v4.2 spliced + compile clean
## Deploy

- `00:18:22`   zip: 102237 bytes
## 1. Lambda

- `00:18:23`   Lambda exists — updating
- `00:18:28` ✅   ✓ updated justhodl-chokepoint
- `00:18:43` ✅ settled attempt 1
- `00:18:43` ✅ DEPLOY.settled :: v4.2 live
## Invoke + verify

- `00:18:58` ✅ LIVE.v42 :: version=4.2
- `00:18:58` ✅ LIVE.no_err :: err=None
- `00:18:58` ✅ LIVE.growth :: growth on 1242 names (rpo_yoy covered only 68)
- `00:18:58` ✅ LIVE.sp500 :: 490 S&P500 members flagged
## Growth tier distribution

- `00:18:58`   None     1934
- `00:18:58`   LOW      495
- `00:18:58`   MEDIUM   469
- `00:18:58`   HIGH     278
- `00:18:58` ✅ SANITY.tiers_spread :: all three tiers populated — not a degenerate split
- `00:18:58` ✅ SANITY.nonusd_growth :: 66 non-USD filers publish growth (share is suppressed for them)
## Sample

- `00:18:58`   NVDA  yoy=65.5     cagr3y=100.0    tier=HIGH    sp500=True  basis=YoY+3y agree
- `00:18:58`   TSM   yoy=33.0     cagr3y=19.3     tier=HIGH    sp500=False basis=YoY 33% vs 3y CAGR 19% — disagree
- `00:18:58`   AMD   yoy=34.3     cagr3y=13.6     tier=HIGH    sp500=True  basis=YoY 34% vs 3y CAGR 14% — disagree
- `00:18:58`   MSFT  yoy=14.9     cagr3y=12.4     tier=MEDIUM  sp500=True  basis=YoY+3y agree
- `00:18:58`   AVGO  yoy=23.9     cagr3y=24.4     tier=HIGH    sp500=True  basis=YoY+3y agree
## Additive

- `00:18:58` ✅ ADDITIVE.capture_gap :: preserved
- `00:18:58` ✅ ADDITIVE.revenue_share_pct :: preserved
- `00:18:58` ✅ ADDITIVE.catchup_pct :: preserved
- `00:18:58` ✅ ADDITIVE.criticality_pctile :: preserved
## VERDICT

- `00:18:58` ✅ PASS_ALL — growth + S&P500 live; filters can now be built on real fields
