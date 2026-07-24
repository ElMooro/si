# ops 3789 — verify v7 filters / growth / tooltips / description

**Status:** failure  
**Duration:** 102.0s  
**Finished:** 2026-07-24T00:26:59+00:00  

## Error

```
SystemExit: 1
```

## Data

| leaderboard | scored | sp500 | with_growth |
|---|---|---|---|
|  | 3176 | 490 | 1242 |
| 50 |  |  |  |

## Log
## Feed precondition (v4.2)

- `00:25:18` ✅ FEED.v42 :: engine v4.2
- `00:25:18` ✅ FEED.growth :: growth on 1242 names
- `00:25:18` ✅ FEED.sp500 :: 490 S&P500 members
## Do the filters actually select anything?

- `00:25:18` ✅ FILTER.cap_mega :: 68 names
- `00:25:18` ✅ FILTER.cap_large :: 259 names
- `00:25:18` ✅ FILTER.cap_mid :: 652 names
- `00:25:18` ✅ FILTER.cap_small :: 2197 names
- `00:25:18` ✅ FILTER.sp500 :: 490 names
- `00:25:18` ✅ FILTER.growth_HIGH :: 278 names
- `00:25:18` ✅ FILTER.growth_MEDIUM :: 469 names
- `00:25:18` ✅ FILTER.growth_LOW :: 495 names
- `00:25:18` ✗ LEAD.growth_tier :: 0 of 50 leaderboard rows
- `00:25:18` ✗ LEAD.in_sp500 :: 0 of 50 leaderboard rows
- `00:25:18` ✅ LEAD.revenue_growth_yoy :: 28 of 50 leaderboard rows
- `00:25:18` ✅ LEAD.gm_level :: 50 of 50 leaderboard rows
## Served page v7

- `00:25:19` attempt 1: HTTP 200 · 32303 bytes · 0/11
- `00:25:44` attempt 2: HTTP 200 · 32303 bytes · 0/11
- `00:26:09` attempt 3: HTTP 200 · 32303 bytes · 0/11
- `00:26:34` attempt 4: HTTP 200 · 32303 bytes · 0/11
- `00:26:59` attempt 5: HTTP 200 · 42194 bytes · 11/11
- `00:26:59` ✅ SERVED.stamp :: present
- `00:26:59` ✅ SERVED.gloss :: present
- `00:26:59` ✅ SERVED.help_fn :: present
- `00:26:59` ✅ SERVED.cap_select :: present
- `00:26:59` ✅ SERVED.gro_select :: present
- `00:26:59` ✅ SERVED.growth_col :: present
- `00:26:59` ✅ SERVED.gm_col :: present
- `00:26:59` ✅ SERVED.gro_fn :: present
- `00:26:59` ✅ SERVED.about :: present
- `00:26:59` ✅ SERVED.hm_class :: present
- `00:26:59` ✅ SERVED.sp500_opt :: present
## Description names the engines it pulls from

- `00:26:59` ✅ DESC.justhodl_chokepoint :: cited
- `00:26:59` ✅ DESC.universe_builder :: cited
- `00:26:59` ✅ DESC.justhodl_backlog :: cited
- `00:26:59` ✅ DESC.fundamental_census :: cited
- `00:26:59` ✅ DESC.supply_chain_graph :: cited
- `00:26:59` ✅ DESC.Honest limits :: cited
## Additive — v6 surfaces intact

- `00:26:59` ✅ KEPT.Most_Undervalued :: intact
- `00:26:59` ✅ KEPT.By_Industry :: intact
- `00:26:59` ✅ KEPT.Full_Ledger :: intact
- `00:26:59` ✅ KEPT.rsh( :: intact
- `00:26:59` ✅ KEPT.dep( :: intact
- `00:26:59` ✅ KEPT.percent_critical_not :: intact
- `00:26:59` ✅ KEPT.data-lk :: intact
- `00:26:59` ✅ KEPT.data-bk :: intact
- `00:26:59` ✅ KEPT.Default_rank_is_blen :: intact
## Sample of what a filtered view returns

- `00:26:59`   HIGH-growth names on the leaderboard: 0
## VERDICT

- `00:26:59` ✗ FAILED: LEAD.growth_tier, LEAD.in_sp500
