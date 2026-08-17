# G0. FIELD-level spine contract

**Status:** success  
**Duration:** 44.9s  
**Finished:** 2026-08-17T15:08:04+00:00  

## Data

| invest_prev | sb_n_scored | sb_tiers | stock-buying_prev |
|---|---|---|---|
| 2026-08-17T15:01:07 |  |  |  |
|  |  |  | 2026-08-17T15:01:29 |
|  | 300 | {"EXPLOSIVE-SETUP": 0, "SETUP": 0, "WATCH": 58, "SCREENED": 438} |  |

## Log
- `15:07:20` ✅   spine LIVE, q1.beat_pct=32.7, assignments=4966
# 1. LastUpdateStatus gate (the 4819 miss)

- `15:07:20` ✅ justhodl-invest Active + LastUpdateStatus Successful
- `15:07:21` ✅ justhodl-stock-buying Active + LastUpdateStatus Successful
# 2. marker settle

- `15:07:21` ✅ justhodl-invest marker 'invest-odds v1' settled (attempt 1)
- `15:07:22` ✅ justhodl-stock-buying marker 'sb-odds v1' settled (attempt 1)
# 3. capture prev AFTER gates, invoke, poll (<=15 min)

- `15:07:43` ✅ justhodl-invest fresh in 20s
- `15:08:04` ✅ justhodl-stock-buying fresh in 41s
# 4. truths

- `15:08:04` ✅   invest schema unchanged (invest/0.1)
- `15:08:04` ✅   stock-buying schema_version unchanged (1)
- `15:08:04` ⚠   justhodl-invest: stock_picks empty (bootstrap tape) -- coverage skipped, meta={"as_of": "2026-08-17", "ledger_weeks": 53, "picks_with_odds": 0}
- `15:08:04` ✅   justhodl-stock-buying: odds coverage 289/289 (100%)
- `15:08:04` ✅   justhodl-stock-buying: sampled odds.q == spine q (40/40)
- `15:08:04` ✅   justhodl-stock-buying: header base_rates meta {"as_of": "2026-08-17", "ledger_weeks": 53, "rows_with_odds": 562}
- `15:08:04` ✅   sb pre-existing fields intact on chip rows
# 5. sample chips

- `15:08:04`   stock-buying  DELL   q=5 beat=41.6% LB95=38.4% dd=0_to_-10 medEx=-7.2pp
- `15:08:04`   stock-buying  GPN    q=5 beat=41.6% LB95=38.4% dd=0_to_-10 medEx=-7.2pp
- `15:08:04`   stock-buying  MPC    q=5 beat=41.6% LB95=38.4% dd=0_to_-10 medEx=-7.2pp
- `15:08:04`   stock-buying  BBY    q=4 beat=36.5% LB95=33.4% dd=0_to_-10 medEx=-7.0pp
# 6. verdict

- `15:08:04` ✅ Fusion 1 consumers LIVE -- invest + stock-buying quote the fleet's measured odds; neither can diverge from the spine
