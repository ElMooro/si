# ops 4093 — STEP 3: COT/COT3 → CFTC TFF, verified

**Status:** failure  
**Duration:** 653.4s  
**Finished:** 2026-07-29T20:48:10+00:00  

## Error

```
SystemExit: 1
```

## Data

| cftc_live | cot_aliased | cot_skipped | cot_total | n_aliases | vault_live | vault_rows |
|---|---|---|---|---|---|---|
|  | 65 | 253 | 340 | 864 |  |  |
| 0 |  |  |  |  | 1132 | 1314 |

## Log
## A. deploy resolver v3.0 + vault v3.13.0

- `20:37:39`   ✓ justhodl-symbol-resolver settled (attempt 3)
- `20:37:50`   ✓ justhodl-tradingview settled (attempt 2)
## B. resolve COT tickers (async + poll)

- `20:42:18`   ✓ data/symbol-aliases.json moved after 260s
- `20:42:18`   COT tickers 340 · aliased 65 · skipped 253
- `20:42:18`   by_route: {'fred-verified': 696, 'economics-matched': 103, 'cot-verified': 65}
## Verified COT aliases (sample)

- `20:42:18`   020601_FO_AMP_SPREAD       → cftc:yw9f-hn96:020601:asset_mgr_positions_sp asof 2026-07-21
- `20:42:18`   020601_FO_DP_SPREAD        → cftc:yw9f-hn96:020601:dealer_positions_sprea asof 2026-07-21
- `20:42:18`   020601_FO_LMP_SPREAD       → cftc:yw9f-hn96:020601:lev_money_positions_sp asof 2026-07-21
- `20:42:18`   020601_FO_ORP_SPREAD       → cftc:yw9f-hn96:020601:other_rept_positions_s asof 2026-07-21
- `20:42:18`   020601_FO_TAM_SPREAD       → cftc:yw9f-hn96:020601:traders_asset_mgr_spre asof 2026-07-21
- `20:42:18`   020601_FO_TD_SPREAD        → cftc:yw9f-hn96:020601:traders_dealer_spread_ asof 2026-07-21
- `20:42:18`   020601_FO_TLM_SPREAD       → cftc:yw9f-hn96:020601:traders_lev_money_spre asof 2026-07-21
- `20:42:18`   020601_FO_TOR_SPREAD       → cftc:yw9f-hn96:020601:traders_other_rept_spr asof 2026-07-21
- `20:42:18`   020601_F_AMP_SPREAD        → cftc:gpe5-46if:020601:asset_mgr_positions_sp asof 2026-07-21
- `20:42:18`   020601_F_DP_SPREAD         → cftc:gpe5-46if:020601:dealer_positions_sprea asof 2026-07-21
- `20:42:18`   020601_F_LMP_SPREAD        → cftc:gpe5-46if:020601:lev_money_positions_sp asof 2026-07-21
- `20:42:18`   020601_F_ORP_SPREAD        → cftc:gpe5-46if:020601:other_rept_positions_s asof 2026-07-21
- `20:42:18`   020601_F_TAM_SPREAD        → cftc:gpe5-46if:020601:traders_asset_mgr_spre asof 2026-07-21
- `20:42:18`   020601_F_TD_SPREAD         → cftc:gpe5-46if:020601:traders_dealer_spread_ asof 2026-07-21
## C. vault fetches them for real

- `20:48:10`   ✓ data/tradingview.json moved after 340s
- `20:48:10`   vault rows 1091 → 1314 · LIVE 975 → 1132
## CFTC-routed symbols carrying real values

## VERDICT

- `20:48:10`   ✓ resolver v3.0 settled
- `20:48:10`   ✓ vault v3.13.0 settled
- `20:48:10`   ✓ resolver async accepted
- `20:48:10`   ✓ COT aliases produced
- `20:48:10`   ✓ every COT alias points at a real CFTC column
- `20:48:10`   ✓ every COT alias was verified against a live row
- `20:48:10`   ✓ vault async accepted
- `20:48:10`   ✗ cftc: adapter returns real values
- `20:48:10`   ✓ pre-existing LIVE not regressed
- `20:48:10` ✗ FAILED: ['cftc: adapter returns real values']
