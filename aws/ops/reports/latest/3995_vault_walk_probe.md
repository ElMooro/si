# ops 3995 — vault-walk truncation forensics (v2.1)

**Status:** success  
**Duration:** 1.9s  
**Finished:** 2026-07-28T03:00:46+00:00  

## Data

| artifacts | artifacts_truncated_by_time_budget | barometers_rows | distinct_symbols | distinct_vault_symbols_captured | elapsed_s | generated_at | ledger_n | marker | n_total_indexed | parse_errors | scalar_paths | vault_rows_in_ledger |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 38394 | 2041 |  |  |  | 236.2 | 2026-07-27T22:50:20.595554+00:00 |  | data-census v2.1 ops3993 full-lists |  | 0 | 250007 |  |
|  |  |  |  |  |  |  | 150000 |  | 250007 |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  | 401 |
|  |  |  |  | 106 |  |  |  |  |  |  |  |  |
|  |  | 403 | 75 |  |  |  |  |  |  |  |  |  |

## Log
## A. the two priority artifacts, as the engine recorded them

- `03:00:45`   data/tradingview.json
- `03:00:45`       n_paths=401 skipped=None error=None size=391643 age_h=11.2
- `03:00:45`   data/domain-barometers.json
- `03:00:45`       n_paths=403 skipped=None error=None size=454131 age_h=10.5
- `03:00:45`   data/risk-gate.json
- `03:00:45`       n_paths=62 skipped=None error=None size=12279 age_h=11.8
- `03:00:45`   data/rotation-dashboard.json
- `03:00:45`       n_paths=405 skipped=None error=None size=52865 age_h=0.7
## B. ledger: per-artifact capture + WHERE the vault stopped

- `03:00:46`      530  data/global-business-cycle.json
- `03:00:46`      405  data/rotation-dashboard.json
- `03:00:46`      403  data/domain-barometers.json
- `03:00:46`      402  data/ai-rerating-radar.json
- `03:00:46`      402  data/asset-compass.json
- `03:00:46`      402  data/bagger-engine.json
- `03:00:46`      402  data/convergence-radar-history/2026-06-25.json
- `03:00:46`      401  data/ai-infra-stack.json
- `03:00:46`   first 3 vault paths: ['n_symbols', 'n_tv_notes', 'n_live']
- `03:00:46`   LAST 6 vault paths (truncation point evidence):
- `03:00:46`       symbols[USHMI].n_notes v=6.0 src=unresolved_economics
- `03:00:46`       symbols[USM0].n_notes v=6.0 src=fred_alias:BOGMBASE
- `03:00:46`       symbols[USM0].value v=5538.6 src=fred_alias:BOGMBASE
- `03:00:46`       symbols[USM0].prev v=5470.4 src=fred_alias:BOGMBASE
- `03:00:46`       symbols[USM0].chg_pct v=1.247 src=fred_alias:BOGMBASE
- `03:00:46`       elapsed_s v=91.7 src=None
- `03:00:46`   last symbols reached: ['THREEFYTP1', 'TREASURY', 'TWEXPYY', 'US03Y', 'USALOLITONOSTSAM', 'USFR', 'USHMI', 'USM0']
- `03:00:46`   EUBUND captured: False
- `03:00:46`   JP02Y captured: False
- `03:00:46`   NO03Y captured: False
- `03:00:46`   PETOT captured: False
- `03:00:46`   JPLG captured: True
- `03:00:46`   US10Y captured: True
## C. barometers same question

- `03:00:46`   barometers last symbols: ['US01MY', 'YIT1!', 'XDN', '2USNOTE', 'AGG', 'BAMLC0A0CM']
- `03:00:46` ✅ PROBE DONE — evidence above decides cap vs exception vs order
