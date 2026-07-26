# ops 3900 — verify options-flow-scanner's POLY_KEY is real before fixing anything

**Status:** success  
**Duration:** 0.7s  
**Finished:** 2026-07-26T03:21:26+00:00  

## Data

| all_env_keys | equity_research_POLYGON_KEY_present | length | looks_like_placeholder | matches_source | prefix | present | same_value_as_POLY_KEY | trade_evaluator_poly_key_length | trade_evaluator_poly_key_present |
|---|---|---|---|---|---|---|---|---|---|
| ['DAYS_BACK', 'MAX_TICKERS', 'N_WORKERS', 'POLY_KEY', 'S3_BUCKET', 'TIMEOUT_BUDGET_S'] |  | 32 | False |  | zvEY_KYY... | True |  |  |  |
| ['POLY_KEY'] |  |  |  | True |  |  |  | 32 | True |
|  | True | 32 |  |  |  |  | True |  |  |

## Log
## 1. live env on justhodl-options-flow-scanner (the declared inherit source)

## 2. live env on trade-evaluator itself — did the declared inherit actually resolve

## 3. cross-check — does ANY other lambda have a confirmed-different POLYGON_KEY (the OTHER naming convention) worth comparing

## 4. THE REAL TEST — does a live Polygon call with THIS key actually work

- `03:21:26` ✅   SUCCESS — real Polygon response: {"ticker": "AAPL", "queryCount": 1, "resultsCount": 1, "adjusted": true, "results": [{"T": "AAPL", "v": 47489415.0, "vw": 331.448, "o": 321.79, "c": 333.02, "h": 334.37, "l": 321.62, "t": 1784923200000, "n": 980938}], "status": "OK", "request_id": "bab53df8e9d0f2f93b7e585fb997b596", "count": 1}
- `03:21:26` ✅ PROBE COMPLETE — key confirmed genuinely working end to end
