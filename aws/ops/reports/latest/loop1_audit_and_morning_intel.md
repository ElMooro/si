# 1) Loop 1 calibration snapshot — did badge flip GREEN?

**Status:** success  
**Duration:** 21.3s  
**Finished:** 2026-05-04T20:23:34+00:00  

## Log
- `20:23:13`   generated_at: 2026-05-04T20:11:44.883061+00:00
- `20:23:13`   signal types tracked: None
- `20:23:13`   total outcomes 60d:   1302
- `20:23:13`   weighted accuracy:    None
- `20:23:13`   best signal:          None
- `20:23:13`   worst signal:         None
# 2) Direct DDB count — outcomes per signal type, last 60d

- `20:23:14`   total non-legacy outcomes 60d: 1302  (paginated 6x)
- `20:23:14` 
- `20:23:14`   Signal types by outcome count:
- `20:23:14`     ✅ screener_top_pick                 n=450
- `20:23:14`     ✅ crypto_risk_score                 n=121
- `20:23:14`     ✅ crypto_fear_greed                 n=121
- `20:23:14`     ✅ plumbing_stress                   n=104
- `20:23:14`     ✅ momentum_uso                      n=76
- `20:23:14`     ✅ edge_composite                    n=74
- `20:23:14`     ✅ ml_risk                           n=67
- `20:23:14`     ✅ khalid_index                      n=67
- `20:23:14`     ✅ momentum_gld                      n=33
- `20:23:14`     ✅ edge_regime                       n=30
- `20:23:14`     ✅ market_phase                      n=30
- `20:23:14`     ✅ carry_risk                        n=30
- `20:23:14`     🟡 corr_break_composite_vs_spy       n=18
- `20:23:14`     🟡 corr_break_composite_vs_vxx       n=18
- `20:23:14`     🟡 crisis_hy_oas_vs_hyg              n=13
- `20:23:14`     🟡 crisis_sofr_iorb                  n=13
- `20:23:14`     🟡 momentum_spy                      n=13
- `20:23:14`     🟡 crisis_obfr_iorb                  n=12
- `20:23:14`     🔴 corr_break_top_pair               n=5
- `20:23:14`     🔴 momentum_tlt                      n=5
- `20:23:14`     🔴 momentum_uup                      n=2
- `20:23:14` 
- `20:23:14`   → 12/21 types calibration-ready (n>=30)
# 3) Re-invoke justhodl-morning-intelligence (credits back)

- `20:23:34`   status: 200, duration: 20.2s
- `20:23:34`   resp: {"statusCode": 200, "body": "{\"success\": true, \"khalid\": {\"score\": 48, \"regime\": \"NEUTRAL\", \"signals\": [[\"DXY\", -12, \"118.7\"], [\"HY Spread\", 5, \"2.77%\"], [\"NFCI\", 5, \"-0.52\"], [\"Unemployment\", -8, \"4.3%\"], [\"Net Liq\", 3, \"$5.72T\"], [\"SPY Trend\", 5, \"$718\"]], \"ts\": \"2026-05-04T20:20:28.129800\"}, \"khalid_adj\": 48.0, \"regime\": \"NEUTRAL\", \"btc\": 80008, \"outcomes\": 1188, \"improved\": false, \"weights_active\": 32, \"ka\": {\"score\": 48, \"regime\": 
# 4) Quick smoke on other Anthropic Lambdas

- `20:23:34`   justhodl-investor-agents             state=Active   mod=2026-04-26T12:52:46  reserved=—
- `20:23:34`   justhodl-watchlist-debate            state=Active   mod=2026-04-25T12:32:55  reserved=—
- `20:23:34`   justhodl-financial-secretary         state=Active   mod=2026-04-26T12:18:17  reserved=—
# 5) SSM calibration weights — anything updated recently?

- `20:23:34`   last_modified: 2026-05-03 09:03:40.688000+00:00
- `20:23:34`   n_weights: 32
- `20:23:34`   top 5 weights:
- `20:23:34`     crisis_hy_oas_vs_hyg              w=1.4159
- `20:23:34`     screener_top_pick                 w=0.85
- `20:23:34`     edge_composite                    w=0.8317
- `20:23:34`     valuation_composite               w=0.8
- `20:23:34`     cftc_gold                         w=0.8
- `20:23:34`   accuracy params last_modified: 2026-05-03 09:03:40.733000+00:00
- `20:23:34`   n_acc keys: 16
