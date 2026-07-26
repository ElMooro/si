# ops 3918 — Risk Gate v2.0 fleet-fused deploy

**Status:** success  
**Duration:** 7.0s  
**Finished:** 2026-07-26T19:50:27+00:00  

## Data

| composite | posture | replay_composite | replay_posture | sizing | statuses | total_fleet_inputs |
|---|---|---|---|---|---|---|
| -0.395 | RISK_OFF | -0.4 | RISK_OFF | 0.45 |  |  |
|  |  |  |  |  | {'MISSING': 3, 'OK': 16, 'STALE': 1} | 20 |

## Log
- `19:50:20` ✅   settled attempt 1
- `19:50:27`   funding: fred=-2.0 fleet_adj=0.0 fused=-2.0 inputs=dealer_corp_net_bonds_b=None(MISSING,adj0.0) | fails_cross_z=None(MISSING,adj0.0) | auction_10y_grade=B-(OK,adj0.0) | plumbing_composite=None(MISSING,adj0.0) | xcc_basis_proxy_bp={'rate_diff_jpy_3m': {'available': True, 'latest_date': '2026-07-23', 'current_pct': 2.676, 'mean_1y_pct': 2.853, 'std_1y_pct': 0.447, 'z_score_1y': -0.4, 'delta_30d': 0.1, 'signal': 'NORMAL', 'n_observations': 272, 'interpretation': 'Rate differential within 1Y normal range', 'method': 'USD 3M T-Bill (DGS3MO) minus Japan 3M Interbank (IR3TIB01JPM156N), 1Y z-score', 'caveat': 'Approximates CIP deviation; true basis requires forward FX (not on FRED)'}, 'rate_diff_eur_3m': {'available': True, 'latest_date': '2026-07-23', 'current_pct': 1.765, 'mean_1y_pct': 1.947, 'std_1y_pct': 0.27, 'z_score_1y': -0.67, 'delta_30d': 0.097, 'signal': 'NORMAL', 'n_observations': 272, 'interpretation': 'Rate differential within 1Y normal range', 'method': 'USD 3M T-Bill (DGS3MO) minus EUR ESTR overnight (ECBESTRVOLWGTTRMDMNRT), 1Y z-score', 'caveat': 'Tenor mismatch (USD 3M vs EUR overnight); z-score of differential is what matters, not the level'}, 'broad_dollar_index': {'available': True, 'latest_date': '2026-07-17', 'level': 120.53, 'z_score_1y': 0.6, 'delta_30d_pct': 0.96, 'signal': 'NORMAL', 'interpretation': 'USD within 1Y normal range'}, 'obfr_iorb_spread': {'available': True, 'latest_date': '2026-07-23', 'spread_bps': -2.0, 'obfr_pct': 3.63, 'iorb_pct': 3.65, 'z_score_1y': 0.5, 'signal': 'NORMAL', 'interpretation': 'Unsecured plumbing functioning normally', 'note': 'Parallels SOFR-IORB but for unsecured side; combined picture of repo + fed funds'}}(OK,adj0.0)
- `19:50:27`   credit: fred=0.0 fleet_adj=0.0 fused=0.0 inputs=credit_composite_0_100=25(OK,adj0.0) | credit_z60_mean=0.9(OK,adj0.0)
- `19:50:27`   dollar: fred=0.0 fleet_adj=0.0 fused=0.0 inputs=btp_bund_widest_bp=76.4(OK,adj0.0) | bis_crossborder_yoy_median=13.2(STALE,adj0.0)
- `19:50:27`   carry: fred=0.0 fleet_adj=0.25 fused=0.25 inputs=yen_unwind_risk_0_100=37.9(OK,adj0.0) | eurodollar_stress_0_100=12.9(OK,adj0.25) | china_m1_yoy_pct=1.5(OK,adj0.0)
- `19:50:27`   growth: fred=1.0 fleet_adj=0.3 fused=1.3 inputs=taiwan_exports_yoy=48.33(OK,adj0.3) | freight_yoy_median=2.2(OK,adj0.0) | air_cargo_yoy=3.1(OK,adj0.0) | portwatch_chokepoint_yoy_median=-6.4(OK,adj0.0)
- `19:50:27`   structure: fred=0.0 fleet_adj=-0.5 fused=-0.5 inputs=bond_vol_z_plus_funding={'z': 0.05, 'funding_regime': 'TIGHTENING'}(OK,adj-0.2) | vol_migration_spill_z=-0.5(OK,adj0.0) | etf_net_flow_20d_usd_bn=-21.25(OK,adj-0.3) | cftc_dealer_positioning=29(OK,adj0.0)
- `19:50:27` ✅   v2 settled
- `19:50:27` ✅   invoke ok
- `19:50:27` ✅   all 6 legs carry fleet_inputs
- `19:50:27` ✅   15+ fleet inputs wired
- `19:50:27` ✅   all 5 Leg-1 inputs present
- `19:50:27` ✅   replay separated from fused live
- `19:50:27` ✅   October replay intact
- `19:50:27` ✅   posture valid
- `19:50:27` ✅ PASS_ALL — v2.0 live: RISK_OFF fused=-0.395 (FRED-only replay -0.4), 20 fleet inputs, statuses {'MISSING': 3, 'OK': 16, 'STALE': 1}
