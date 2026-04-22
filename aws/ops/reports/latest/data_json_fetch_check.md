# data.json reachability + shape

**Status:** success  
**Duration:** 0.6s  
**Finished:** 2026-04-22T22:56:00+00:00  

## Data

| key | sample | size | type |
|---|---|---|---|
| at_risk |  | 1 | list |
| bond_analysis | credit, interpretation, yield_curve | 3 | dict |
| bottomed |  | 0 | list |
| buys |  | 0 | list |
| downtrend |  | 8 | list |
| dxy | advanced, broad, em, impact, interpretation, monthly_change, strength, value | 9 | dict |
| fred | AAA, BAA, BAA10Y, BAMLC0A0CM, BAMLC0A0CMEY, BAMLC0A1CAAA, BAMLC0A2CAA, BAMLC0A3CA | 47 | dict |
| fred_count | 47 | 0 | number |
| gainers |  | 5 | list |
| generated | 2026-02-18T13:00:41.450626 | 26 | string |
| icebofa | BAMLC0A0CM, BAMLC0A0CMEY, BAMLC0A1CAAA, BAMLC0A2CAA, BAMLC0A3CA, BAMLC0A4CBBB, BAMLEMCBPIOAS, BAMLH0A0HYM2 | 12 | dict |
| khalid_index | 49 | 0 | number |
| liquidity | fed_balance_sheet, m2, reverse_repo, tga, trend | 5 | dict |
| losers |  | 5 | list |
| outlook | key_catalysts, key_risks, khalid_index, regime, scenarios | 5 | dict |
| portfolio | allocation, avoid, rationale, rebalance, regime, top_picks | 6 | dict |
| regime | NEUTRAL | 7 | string |
| reversals_down |  | 0 | list |
| reversals_up |  | 0 | list |
| sells |  | 6 | list |
| stock_count | 37 | 0 | number |
| stocks | AMLP, ARKK, BOTZ, DIA, EEM, EFA, FXI, GLD | 37 | dict |
| technicals | AMLP, ARKK, BOTZ, DIA, EEM, EFA, FXI, GLD | 37 | dict |
| topped |  | 6 | list |
| uptrend |  | 6 | list |
| version | 6.0 | 3 | string |
| warnings |  | 1 | list |

## Log
## Authenticated S3 read (boto3)

- `22:56:00` ✅ Read 60635 bytes via boto3 — timestamp: 2026-02-18T13:00:53+00:00
- `22:56:00` ✅ Parsed as JSON. Top-level keys (27):
- `22:56:00`   `at_risk` → list (length 1)
- `22:56:00`   `bond_analysis` → dict with subkeys: ['credit', 'interpretation', 'yield_curve']
- `22:56:00`   `bottomed` → list (length 0)
- `22:56:00`   `buys` → list (length 0)
- `22:56:00`   `downtrend` → list (length 8)
- `22:56:00`   `dxy` → dict with subkeys: ['advanced', 'broad', 'em', 'impact', 'interpretation', 'monthly_change', 'strength', 'value']…
- `22:56:00`   `fred` → dict with subkeys: ['AAA', 'BAA', 'BAA10Y', 'BAMLC0A0CM', 'BAMLC0A0CMEY', 'BAMLC0A1CAAA', 'BAMLC0A2CAA', 'BAMLC0A3CA']…
- `22:56:00`   `fred_count` → number: 47
- `22:56:00`   `gainers` → list (length 5)
- `22:56:00`   `generated` → string: 2026-02-18T13:00:41.450626
- `22:56:00`   `icebofa` → dict with subkeys: ['BAMLC0A0CM', 'BAMLC0A0CMEY', 'BAMLC0A1CAAA', 'BAMLC0A2CAA', 'BAMLC0A3CA', 'BAMLC0A4CBBB', 'BAMLEMCBPIOAS', 'BAMLH0A0HYM2']…
- `22:56:00`   `khalid_index` → number: 49
- `22:56:00`   `liquidity` → dict with subkeys: ['fed_balance_sheet', 'm2', 'reverse_repo', 'tga', 'trend']
- `22:56:00`   `losers` → list (length 5)
- `22:56:00`   `outlook` → dict with subkeys: ['key_catalysts', 'key_risks', 'khalid_index', 'regime', 'scenarios']
- `22:56:00`   `portfolio` → dict with subkeys: ['allocation', 'avoid', 'rationale', 'rebalance', 'regime', 'top_picks']
- `22:56:00`   `regime` → string: NEUTRAL
- `22:56:00`   `reversals_down` → list (length 0)
- `22:56:00`   `reversals_up` → list (length 0)
- `22:56:00`   `sells` → list (length 6)
- `22:56:00`   `stock_count` → number: 37
- `22:56:00`   `stocks` → dict with subkeys: ['AMLP', 'ARKK', 'BOTZ', 'DIA', 'EEM', 'EFA', 'FXI', 'GLD']…
- `22:56:00`   `technicals` → dict with subkeys: ['AMLP', 'ARKK', 'BOTZ', 'DIA', 'EEM', 'EFA', 'FXI', 'GLD']…
- `22:56:00`   `topped` → list (length 6)
- `22:56:00`   `uptrend` → list (length 6)
- `22:56:00`   `version` → string: 6.0
- `22:56:00`   `warnings` → list (length 1)
## Public HTTPS read (what browsers/Workers see)

- `22:56:00` ✗ Public HTTPS read failed: HTTP 403 — Forbidden
- `22:56:00` This means browser-based dashboards (edge.html, valuations.html, etc.) can't read data.json anymore
## Current bucket policy + ACL

- `22:56:00` Bucket policy present (484 bytes):
- `22:56:00`   - Effect: Allow | Principal: * | Action: s3:GetObject
- `22:56:00`   - Effect: Allow | Principal: * | Action: s3:GetObject
- `22:56:00`   - Effect: Allow | Principal: * | Action: s3:GetObject
- `22:56:00` Public access block: {'BlockPublicAcls': False, 'IgnorePublicAcls': False, 'BlockPublicPolicy': False, 'RestrictPublicBuckets': False}
- `22:56:00` Done
