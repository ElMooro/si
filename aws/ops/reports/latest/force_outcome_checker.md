# 1) Outcome census BEFORE force-run

**Status:** success  
**Duration:** 43.1s  
**Finished:** 2026-05-05T12:01:39+00:00  

## Log
- `12:00:57`   total scored outcomes: 1620
- `12:00:57`   unique signal types:   26
# 2) Force invoke outcome-checker

- `12:01:16`   status: 200, duration: 19.7s
- `12:01:16`   resp: {"statusCode": 200, "body": "{\"processed\": 108, \"timestamp\": \"2026-05-05T12:01:16.941710+00:00\"}"}
# 3) Outcome census AFTER force-run

- `12:01:19`   total scored outcomes: 1716
- `12:01:19`   unique signal types:   26
- `12:01:19`   new outcomes:          +96
# 4) Per-signal diff (only types with new outcomes)

- `12:01:19`   ✓ corr_break_composite_vs_spy          32 → 36  (+4)
- `12:01:19`   ✓ corr_break_composite_vs_vxx          32 → 36  (+4)
- `12:01:19`   ✓ corr_break_top_pair                  12 → 14  (+2)
- `12:01:19`   ✓ crisis_broad_dollar_vs_eem           4 → 6  (+2)
- `12:01:19`   ✓ crisis_broad_dollar_vs_spy           4 → 6  (+2)
- `12:01:19`   ✓ crisis_hy_oas_vs_hyg                 24 → 28  (+4)
- `12:01:19`   ✓ crisis_hy_oas_vs_spy                 4 → 6  (+2)
- `12:01:19`   ✓ crisis_obfr_iorb                     23 → 27  (+4)
- `12:01:19`   ✓ crisis_rate_diff_eur_3m              4 → 6  (+2)
- `12:01:19`   ✓ crisis_rate_diff_jpy_3m              4 → 6  (+2)
- `12:01:19`   ✓ crisis_sofr_iorb                     24 → 28  (+4)
- `12:01:19`   ✓ crypto_fear_greed                    149 → 157  (+8)
- `12:01:19`   ✓ crypto_risk_score                    150 → 158  (+8)
- `12:01:19`   ✓ edge_composite                       89 → 93  (+4)
- `12:01:19`   ✓ khalid_index                         75 → 77  (+2)
- `12:01:19`   ✓ ml_risk                              75 → 77  (+2)
- `12:01:19`   ✓ momentum_uso                         89 → 95  (+6)
- `12:01:19`   ✓ plumbing_stress                      119 → 123  (+4)
- `12:01:19`   ✓ screener_top_pick                    555 → 585  (+30)
# 5) Newly-activated signal types

- `12:01:19`   no newly-activated signal types this run
# 6) Run a 2nd time after delay to allow async score writes

- `12:01:39`   total scored: 1716  (+0 from 2nd census)
