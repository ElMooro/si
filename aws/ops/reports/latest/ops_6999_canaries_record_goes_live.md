# 6999 - the canaries' record goes live, 2026-11-27T17:12+00:00

**Status:** success  
**Duration:** 150.7s  
**Finished:** 2026-11-27T17:14:46+00:00  

## Data

| failed_checks | level | one_off_runs | turn |
|---|---|---|---|
| 0 | 49.2 (38.0 pct, CALM) | justhodl-canary-grid at 17:13 UTC (43 s); justhodl-warroom-weights at 17:14 UTC (99 s); justhodl-canary-warroom at 17:14 UTC (7 s) | 6.2 (93.2 pct, RISING FAST) |

## Log
## 0. The deployed packages are the new code

- `17:12:16` PASS  justhodl-canary-grid: its package holds lambda_function.py with the new code (modified 2026-11-27T17:09:50, 512 MB / 600 s, identical to the repository's file: True)
- `17:12:16` PASS  justhodl-warroom-weights: its package holds record_io.py with the new code (modified 2026-11-27T17:10:58, 1024 MB / 900 s, identical to the repository's file: True)
- `17:12:16` PASS  justhodl-warroom-weights carries canary_record.py as in the repository
- `17:12:16` PASS  justhodl-canary-warroom: its package holds lambda_function.py with the new code (modified 2026-11-27T17:10:24, 512 MB / 120 s, identical to the repository's file: True)
- `17:12:16` PASS  justhodl-canary-warroom carries canary_record.py as in the repository
## 1. The macro grid, once

- `17:13:00`     invoked justhodl-canary-grid with {}: 43 s, status 200
- `17:13:00`     answer: {"statusCode": 200, "body": "{\"ok\": true, \"early_warning_level\": 43.2, \"band\": \"ELEVATED\", \"n_available\": 60, \"n_total\": 65}"}
- `17:13:00` PASS  the grid ran without a function error
- `17:13:00` grid now: written 2026-11-27T17:13+00:00, early-warning level 43.2 (ELEVATED), 60 of 65 signals available; rates and credit {"label": "Rates & Credit", "score": 47.9, "band": "ELEVATED", "n_signals": 14, "lead_months": 4.6}
- `17:13:00`     hy_credit_oas  before 94.4 -> now 54.9 (z 0.22, as of 2026-11-24)
- `17:13:00`     euro_hy_oas    before 71.9 -> now 45.0 (z -0.23, as of 2026-11-24)
- `17:13:00`     em_corp_oas    before 67.8 -> now 41.0 (z -0.41, as of 2026-11-24)
- `17:13:00`     em_hy_oas      before 73.8 -> now 43.0 (z -0.32, as of 2026-11-24)
- `17:13:00`     us_hy_ytw      before 100.0 -> now 73.3 (z 1.06, as of 2026-11-24)
- `17:13:00`     em_hy_ytw      before 87.5 -> now 54.8 (z 0.22, as of 2026-11-24)
- `17:13:00`     bbb_aaa        before 52.6 -> now 36.9 (z -0.6, as of 2026-11-24)
- `17:13:00` PASS  all seven credit canaries are still available
- `17:13:00` PASS  the grid still has its signals (60 available)
## 2. The record engine, once

- `17:14:39`     invoked justhodl-warroom-weights with {}: 99 s, status 200
- `17:14:39`     answer: {"ok": true, "version": "2.0.0", "proxy_study": {"ok": true, "n_learned": 10, "weights": {"macro_grid": 0.818, "funding": 0.953, "vol": 1.192, "dollar": 0.985, "ciss": 1.0, "global_stress": 1.128, "leading_markets": 1.056, "plumbing": 0.962, "eurodollar": 0.832, "factor_regime": 1.183, "cftc": 0.891, "alerts": 1.0}}, "record": {"ok": true, "tested": 164, "defined": 192, "verdicts": {"no edge shown": 129, "no record": 28, "wrong way": 7, "leads": 16, "short record": 5, "stuck": 7}, "months": 653, "missing": ["LRUNTTTTCHM156S", "LRHUTTTTCHM156S"], "duration_s": 87.8, "refused": null, "written": true, "bytes": 422484}}
- `17:14:39` PASS  the record was published (version 2.0.0)
- `17:14:39`     the proxy study beside it: {"ok": true, "n_learned": 10, "weights": {"macro_grid": 0.818, "funding": 0.953, "vol": 1.192, "dollar": 0.985, "ciss": 1.0, "global_stress": 1.128, "leading_markets": 1.056, "plumbing": 0.962, "eurodollar": 0.832, "factor_regime": 1.183, "cftc": 0.891, "alerts": 1.0}}
- `17:14:39` record: generated 2026-11-27T17:14:39.373818+00:00, cut 20261125, 164 of 192 canaries, verdicts {"no edge shown": 129, "no record": 28, "wrong way": 7, "leads": 16, "short record": 5, "stuck": 7}, 653 barometer months, built in 87.8 s
- `17:14:39` PASS  the record is whole
- `17:14:39` PASS  it says what the dry run said: level early 0.5081, turn early 0.6478 +- 0.0633
- `17:14:39` against the engines' own readings: {"canaries": 114, "rebuilt": 46.7, "engines_own": 47.27, "gap": -0.57, "median_gap": 0.4, "flagged": ["ciss:Netherlands, Composite Indicator of Sovereign Stress, Index", "ciss:United States, New Composite Indicator of Systemic Stress (CISS), Index", "macro_grid:repo_sofr_iorb", "macro_grid:semiconductor_ip"]}
- `17:14:39` PASS  after the grid's run no credit canary is far from its rebuild (still flagged: [])
- `17:14:39`     hy_credit_oas  rebuilt 52.2 | engine's own 54.9 | gap -2.7 | leads, early score 0.664
- `17:14:39`     euro_hy_oas    rebuilt 44.9 | engine's own 45.0 | gap -0.1 | no edge shown, early score 0.6445
- `17:14:39`     em_corp_oas    rebuilt 40.6 | engine's own 41.0 | gap -0.4 | leads, early score 0.7222
- `17:14:39`     em_hy_oas      rebuilt 42.1 | engine's own 43.0 | gap -0.9 | leads, early score 0.6948
- `17:14:39`     us_hy_ytw      rebuilt 71.1 | engine's own 73.3 | gap -2.2 | no edge shown, early score 0.6103
- `17:14:39`     em_hy_ytw      rebuilt 54.9 | engine's own 54.8 | gap 0.1 | leads, early score 0.772
- `17:14:39`     bbb_aaa        rebuilt 34.0 | engine's own 36.9 | gap -2.9 | leads, early score 0.6662
- `17:14:39` the proxy study's weights: generated 2026-11-27T17:13:11.507394+00:00, 10 learned
## 3. The war room, once

- `17:14:46`     invoked justhodl-canary-warroom with {}: 7 s, status 200
- `17:14:46`     answer: {"ok": true, "version": "2.0.0", "calibrated": true, "barometer": 44.1, "per_mechanism": 46.0, "earned": 45.7, "master_ew": 24.2, "n_firing": 123, "n_divergences": 1}
- `17:14:46` PASS  the war room ran and calibrated
- `17:14:46` PASS  every key the board published before is still there (missing []); now also ['calibrated', 'version']
- `17:14:46` PASS  the board is 2.0.0 and carries the calibrated reading
- `17:14:46` PASS  it read the record just published
- `17:14:46` headline: 123 of 263 canaries firing across 9 voting mechanisms (12 watched); early-warning 24.2 (WATCH). 1 cross-mechanism divergence.
- `17:14:46` existing views: per canary 44.1, per mechanism 46.0, earned 45.7, early-warning 24.2
- `17:14:46` LEVEL 49.2 = 38.0 percentile, CALM; after it 55 of 326 months fell (0.1687), near a high 44 of 268 (0.1642)
- `17:14:46` TURN 6.2 against 20260831 (88 days back, the record's own month-end rebuild) = 93.2 percentile, RISING FAST; after it 16 of 52 fell (0.3077), near a high 4 of 20 (0.2); by mechanism {"ciss": 13.6, "macro_grid": 5.6, "plumbing": 7.1}; weights {"macro_grid": 0.1311, "ciss": 0.0, "plumbing": 0.0731}
- `17:14:46` cell: {"level": "low", "turn": "rising", "months": 67, "fell": 16, "fall_share": 0.2388, "early_months": 57, "early_fell": 13, "early_fall_share": 0.2281} | counted: {"fall": "a close 10% or more under the month-end's close inside 126 sessions", "months": 646, "fell": 138, "fall_share": 0.2136, "early_months": 397, "early_fell": 58, "early_fall_share": 0.1461, "index_from_high_at_cut": -0.081}
- `17:14:46` PASS  110 of 263 canaries on the board carry a record, the rest say why not
- `17:14:46` PASS  today's history entry keeps the calibrated reading ({"l": 49.2, "lp": 38.0, "t": 6.2, "tp": 93.2, "mech": {"ciss": 39.6, "macro_grid": 43.9, "plumbing": 64.1}})
- `17:14:46` PASS  the board carries no key and no address
- `17:14:46` ✅ LIVE: the record is published, the grid reads its credit canaries against their full window, and the board carries the calibrated reading
