# ops 3920 — v2.1 all fixes verified live

**Status:** failure  
**Duration:** 128.1s  
**Finished:** 2026-07-26T20:04:53+00:00  

## Error

```
SystemExit: 1
```

## Data

| composite | posture |
|---|---|
| -0.47 | RISK_OFF |

## Log
- `20:02:45` ✅   settled attempt 1
- `20:02:52`   funding/dealer_net_treasury_b: -14.6 (OK, adj 0.0)
- `20:02:52`   funding/fails_cross_z: None (MISSING, adj 0.0)
- `20:02:52`   funding/auction_10y_grade: B- (OK, adj 0.0)
- `20:02:52`   funding/plumbing_composite: 21.6 (OK, adj 0.0)
- `20:02:52`   funding/xcc_basis_signals: {'worst_z_1y': -0.67, 'signals': ['rate_diff_jpy_3m:NORMAL', 'rate_diff_eur_3m:NORMAL', 'o (OK, adj 0.0)
- `20:02:52`   credit/credit_composite_0_100: 25 (OK, adj 0.0)
- `20:02:52`   credit/credit_z60_mean: 0.9 (OK, adj 0.0)
- `20:02:52`   credit/hyg_net_flow_20d_bn: -1.54 (OK, adj -0.3)
- `20:02:52`   dollar/btp_bund_widest_bp: 76.4 (OK, adj 0.0)
- `20:02:52`   dollar/bis_crossborder_yoy_median: 13.2 (STALE, adj 0.0)
- `20:02:52`   dollar/ecb_ciss_regime: CALM (OK, adj 0.0)
- `20:02:52`   carry/yen_unwind_risk_0_100: 37.9 (OK, adj 0.0)
- `20:02:52`   carry/eurodollar_stress_0_100: 12.9 (OK, adj 0.25)
- `20:02:52`   carry/china_m1_yoy_pct: 1.5 (OK, adj 0.0)
- `20:02:52`   growth/taiwan_exports_yoy: 48.33 (OK, adj 0.3)
- `20:02:52`   growth/freight_yoy_median: 2.2 (OK, adj 0.0)
- `20:02:52`   growth/air_cargo_yoy: 3.1 (OK, adj 0.0)
- `20:02:52`   growth/portwatch_chokepoint_yoy_median: -6.4 (OK, adj 0.0)
- `20:02:52`   structure/bond_vol_z_plus_funding: {'z': 0.05, 'funding_regime': 'TIGHTENING'} (OK, adj -0.2)
- `20:02:52`   structure/vol_migration_spill_z: -0.5 (OK, adj 0.0)
- `20:02:52`   structure/etf_net_flow_20d_usd_bn: -21.25 (OK, adj -0.3)
- `20:02:52`   structure/cftc_dealer_positioning: 29 (OK, adj 0.0)
- `20:04:53` ✅   v2.1 settled
- `20:04:53` ✅   plumbing_composite OK w/ real value
- `20:04:53` ✅   dealer_net_treasury_b OK
- `20:04:53` ✅   xcc extracted to worst_z scalar
- `20:04:53` ✅   HYG credit flow input OK
- `20:04:53` ✅   CISS regime input present
- `20:04:53` ✅   fails honestly MISSING (producer todo)
- `20:04:53` ✅   posture valid
- `20:04:53` ✗   served page renders FLEET INPUTS
- `20:04:53` ✗ FAILED: ['served page renders FLEET INPUTS']
