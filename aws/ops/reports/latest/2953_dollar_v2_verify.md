- `02:28:25` sibling data/eurodollar-stress.json: dict keys=as_of,v,composite_score,severity,regime,n_signals_used,n_signals_total,n_failures,signals,hot_signals,cold_signals,failures
- `02:28:25` sibling data/cb-stance.json: dict keys=generated_at,generated_at_unix,version,model,elapsed_seconds,n_fomc_statements_scored,fed,regime_changed_from_prior,new_statement_since_last_run
**Status:** success  
**Duration:** 13.8s  
**Finished:** 2026-07-07T02:28:39+00:00  

## Data

| canaries | netliq_reading | page_has_risk_strip | pressure | regime | risk | summary | tga_reading |
|---|---|---|---|---|---|---|---|
| 13 | +14 $bn |  | 2 | NEUTRAL | LEAN DUMP(-32) |  | +33 $bn / 13w |
|  |  | True |  |  |  |  |  |
|  |  |  |  |  |  | v2.1: canaries=13 pressure=2 NEUTRAL | risk=LEAN DUMP(-32) | tga=+33 $bn / 13w netliq=+14 $bn |  |

## Log
- `02:28:25` sibling data/china-liquidity.json: dict keys=schema_version,method,generated_at,elapsed_s,fred_failed,series_resolved,regime,regime_read,money,credit_impulse,interbank_rate,currency
- `02:28:25` sibling data/cftc-all-cache.json: dict keys=source,contracts,data,timestamp,cached_at
- `02:28:25`   zip: 13957 bytes
## 1. Lambda

- `02:28:26`   Lambda exists — updating
- `02:28:28` ✅   ✓ updated justhodl-dollar-radar
## 3. Smoke test

- `02:28:29`   invoking justhodl-dollar-radar…
- `02:28:39` ✅   ✓ smoke test passed
- `02:28:39`     ok                       True
- `02:28:39`     dollar_pressure          2
- `02:28:39`     regime                   NEUTRAL
- `02:28:39`     canaries                 13
- `02:28:39`     indices                  4
- `02:28:39`     bilaterals               10
- `02:28:39`     double_top               False
- `02:28:39`     double_bottom            False
- `02:28:39`     build_seconds            9.0
- `02:28:39` canary: Fed net liquidity (13w change) | +14 $bn | NEUTRAL
- `02:28:39` canary: Fed balance sheet trend (QE/QT) | +0.74% / 13w | DUMP
- `02:28:39` canary: Reverse repo (RRP) drain | +2 $bn / 13w | NEUTRAL
- `02:28:39` canary: Treasury General Account | +33 $bn / 13w | NEUTRAL
- `02:28:39` canary: US 10y real yield trend | +0.29 pp / 13w | PUMP
- `02:28:39` canary: US-Germany 10y spread | 1.44 pp (-0.20 / 13w) | DUMP
- `02:28:39` canary: Equity volatility (VIX safe-haven) | 15.8 | DUMP
- `02:28:39` canary: High-yield credit spreads | 2.74% (-0.03 / 13w) | NEUTRAL
- `02:28:39` canary: Dollar index momentum | 120.69 vs 50d/200d | PUMP
- `02:28:39` canary: Offshore dollar funding stress | 39/100 | NEUTRAL
- `02:28:39` canary: US 10y nominal yield trend | 4.49% (+0.09 pp / 13w) | NEUTRAL
- `02:28:39` canary: Fed path repricing (2y yield) | 4.14% (+0.26 pp / 13w) | PUMP
- `02:28:39` canary: Fed FX swap lines outstanding | 0.2 $bn | NEUTRAL
- `02:28:39` ✅ dollar v2.1 verified live: engine, units, page, consumers
