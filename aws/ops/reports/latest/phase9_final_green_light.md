# Phase 9 GREEN LIGHT verifier — final

**Status:** success  
**Duration:** 6.9s  
**Finished:** 2026-04-26T22:08:50+00:00  

## Log
## 1. crisis-plumbing — re-invoke + signal sanity

- `22:08:43`   CodeSha256: 6oM9IZpyX90EWLs26IXb5CYJayJa22TOEvt/tqGg7qg=  modified: 2026-04-26T22:05:55.000+0000
- `22:08:46`   invoke OK (2.9s): {"statusCode": 200, "body": "{\"status\": \"ok\", \"elapsed_sec\": 1.8, \"composite_signal\": \"NORMAL\", \"composite_score\": 37.0, \"n_indices\": 4, \"n_flagged\": 0, \"s3_key\": \"data/crisis-plumb
- `22:08:46` 
- `22:08:46`   Funding & Credit signals (post unit fix):
- `22:08:46`     ✅ SOFR_IORB_SPREAD      0.0bps                     signal=NORMAL  z=0.11
- `22:08:46`     ✅ HY_OAS                2.86% = 286bps             signal=NORMAL  z=-0.6
- `22:08:46`     ✗ IG_BBB_OAS            unavailable
- `22:08:46`     ✅ T10YIE                2.42pct                    signal=NORMAL  z=1.84
- `22:08:46`     ✅ DFII10                1.92pct                    signal=WATCH  z=0.07
- `22:08:46`     ✅ SLOOS_TIGHTEN         5.3pct                     signal=NORMAL  z=None
- `22:08:46` 
- `22:08:46`   Cross-currency signals:
- `22:08:46`     ✅ rate_diff_jpy_3m           2.42%  z=-1.4                        signal=WATCH
- `22:08:46`     ✗ rate_diff_eur_3m           unavailable (?)
- `22:08:46`     ✅ broad_dollar_index         level=118.08  z=-1.77                signal=WATCH
- `22:08:46`     ✅ obfr_iorb_spread           -1.0bps                              signal=NORMAL
## 2. correlation-breaks — re-invoke + top breaking pairs

- `22:08:47`   CodeSha256: Aq5n3VsRewQndZsjNNbUplgqN+pNle+aE+DDiYN6MS0=  modified: 2026-04-26T22:07:59.000+0000
- `22:08:50`   invoke OK (3.0s): {"statusCode": 200, "body": "{\"status\": \"ok\", \"elapsed_sec\": 2.1, \"signal\": \"NORMAL\", \"fro_z\": 0.04, \"n_pairs_2sigma\": 17, \"n_pairs_3sigma\": 2, \"top_break\": [\"VIXCLS\", \"DGS10\"], 
- `22:08:50` 
- `22:08:50`   signal:                  NORMAL
- `22:08:50`   Frobenius Δ z-score 1Y:  0.04
- `22:08:50`   pairs > 2σ from norm:    17
- `22:08:50`   pairs > 3σ from norm:    2
- `22:08:50`   instruments aligned:     9
- `22:08:50`   dates aligned:           538
- `22:08:50`   interpretation: Cross-asset relationships within their typical 1Y range
- `22:08:50` 
- `22:08:50`   TOP BREAKING PAIRS:
- `22:08:50`     VIX                ↔ 10Y Yield           now=+0.255  base=-0.159  z=+3.46
- `22:08:50`     Nasdaq Comp        ↔ 10Y Yield           now=-0.217  base=+0.160  z=-3.10
- `22:08:50`     Broad USD          ↔ WTI Oil             now=+0.582  base=+0.159  z=+2.81
- `22:08:50`     S&P 500            ↔ 10Y Yield           now=-0.275  base=+0.094  z=-2.72
- `22:08:50`         ↳ Stock-bond correlation (flipping positive = inflation regime)
- `22:08:50`     HY OAS             ↔ WTI Oil             now=+0.494  base=-0.191  z=+2.69
## FINAL VERDICT

- `22:08:50`   ✅  crisis-plumbing — funding+credit ≥ 5/6
- `22:08:50`   ✅  correlation-breaks — top pairs ≥ 3
- `22:08:50` 
- `22:08:50`   🟢 PHASE 9 GREEN — all systems operational
- `22:08:50` Done
