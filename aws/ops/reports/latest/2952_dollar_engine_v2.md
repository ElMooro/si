## justhodl-dollar-radar

**Status:** success  
**Duration:** 26.6s  
**Finished:** 2026-07-07T02:23:29+00:00  

## Data

| canaries | cc_verdict | crypto_confluence_dollar_context | dollar_pressure | history_rows | justhodl_crypto_confluence_env_keys | justhodl_crypto_confluence_timeout | justhodl_dollar_radar_env_keys | justhodl_dollar_radar_timeout | justhodl_risk_regime_env_keys | justhodl_risk_regime_timeout | regime | risk_regime_dollar_context | risk_score | risk_verdict | rr_verdict | schema | summary |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  | 3 | 180 |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  | 1 | 120 |  |  |  |  |  |  |  |
|  |  |  |  |  | 0 | 180 |  |  |  |  |  |  |  |  |  |  |  |
| 12 |  |  | 22 |  |  |  |  |  |  |  | LEAN PUMP |  | -32 | LEAN DUMP |  | 2.0 |  |
|  |  |  |  | 1 |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  | True |  |  | LEAN DUMP |  |  |
|  | LEAN DUMP | True |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | dollar v2: canaries=12 pressure=22 regime=LEAN PUMP | risk_tx=LEAN DUMP(-32) | rr_ctx=True cc_ctx=True hist=1 |

## Log
- `02:23:02`   zip: 13705 bytes
## 1. Lambda

- `02:23:03`   Lambda exists — updating
- `02:23:05` ✅   ✓ updated justhodl-dollar-radar
- `02:23:06` ✅   ✓ Function URL: https://idieu25nczsbkbm6zwfbvrwtfi0harwl.lambda-url.us-east-1.on.aws/
## 3. Smoke test

- `02:23:06`   invoking justhodl-dollar-radar…
- `02:23:16` ✅   ✓ smoke test passed
- `02:23:16`     ok                       True
- `02:23:16`     dollar_pressure          22
- `02:23:16`     regime                   LEAN PUMP
- `02:23:16`     canaries                 12
- `02:23:16`     indices                  4
- `02:23:16`     bilaterals               10
- `02:23:16`     double_top               False
- `02:23:16`     double_bottom            False
- `02:23:16`     build_seconds            9.6
## justhodl-risk-regime

- `02:23:16`   zip: 9893 bytes
## 1. Lambda

- `02:23:16`   Lambda exists — updating
- `02:23:19` ✅   ✓ updated justhodl-risk-regime
- `02:23:19` ✅   ✓ Function URL: https://mktqs3qczaemqm3ihy3weuje2e0nmpcm.lambda-url.us-east-1.on.aws/
## 3. Smoke test

- `02:23:20`   invoking justhodl-risk-regime…
- `02:23:23` ✅   ✓ smoke test passed
- `02:23:23`     ok                       True
- `02:23:23`     risk_regime_score        19.5
- `02:23:23`     risk_regime              MILD_RISK_ON
## justhodl-crypto-confluence

- `02:23:23`   zip: 5709 bytes
## 1. Lambda

- `02:23:23`   Lambda exists — updating
- `02:23:26` ✅   ✓ updated justhodl-crypto-confluence
- `02:23:26` ✅   ✓ Function URL: https://qxzbarsj6gfvvhuizhig5xddp40egtva.lambda-url.us-east-1.on.aws/
## 3. Smoke test

- `02:23:27`   invoking justhodl-crypto-confluence…
- `02:23:28` ✅   ✓ smoke test passed
- `02:23:28`     bullish_any              13
- `02:23:28`     bullish_multi            1
- `02:23:28`     bearish_any              1
## verify outputs on S3

- `02:23:29` canary: Fed net liquidity (13w change) | -32472 $bn | PUMP (w 2.00)
- `02:23:29` canary: Fed balance sheet trend (QE/QT) | +0.74% / 13w | DUMP (w 1.50)
- `02:23:29` canary: Reverse repo (RRP) drain | +2 $bn / 13w | NEUTRAL (w 1.00)
- `02:23:29` canary: Treasury General Account | +32519 $bn / 13w | PUMP (w 1.00)
- `02:23:29` canary: US 10y real yield trend | +0.29 pp / 13w | PUMP (w 1.50)
- `02:23:29` canary: US-Germany 10y spread | 1.44 pp (-0.20 / 13w) | DUMP (w 1.00)
- `02:23:29` canary: Equity volatility (VIX safe-haven) | 15.8 | DUMP (w 1.00)
- `02:23:29` canary: High-yield credit spreads | 2.74% (-0.03 / 13w) | NEUTRAL (w 1.00)
- `02:23:29` canary: Dollar index momentum | 120.69 vs 50d/200d | PUMP (w 1.50)
- `02:23:29` canary: US 10y nominal yield trend | 4.49% (+0.09 pp / 13w) | NEUTRAL (w 1.25)
- `02:23:29` canary: Fed path repricing (2y yield) | 4.14% (+0.26 pp / 13w) | PUMP (w 1.00)
- `02:23:29` canary: Fed FX swap lines outstanding | 0.2 $bn | NEUTRAL (w 0.75)
- `02:23:29` ✅ dollar engine v2 live end-to-end
