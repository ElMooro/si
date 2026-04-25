# Diagnose Khalid Index history sources

**Status:** success  
**Duration:** 2.0s  
**Finished:** 2026-04-25T02:29:38+00:00  

## Log
## 1. Distinct signal_types in justhodl-signals

- `02:29:36`   Scanned 4779 items, 15 distinct signal_types:

- `02:29:36`      2830  screener_top_pick                   last: 2026-04-25T00:24:42
- `02:29:36`       186  edge_regime                         last: 2026-04-25T00:24:41
- `02:29:36`       186  market_phase                        last: 2026-04-25T00:24:41
- `02:29:36`       186  edge_composite                      last: 2026-04-25T00:24:41
- `02:29:36`       186  carry_risk                          last: 2026-04-25T00:24:41
- `02:29:36`       186  khalid_index                        last: 2026-04-25T00:24:41
- `02:29:36`       186  crypto_fear_greed                   last: 2026-04-25T00:24:41
- `02:29:36`       186  ml_risk                             last: 2026-04-25T00:24:41
- `02:29:36`       186  crypto_risk_score                   last: 2026-04-25T00:24:41
- `02:29:36`       186  plumbing_stress                     last: 2026-04-25T00:24:41
- `02:29:36`       118  momentum_uso                        last: 2026-04-25T00:24:41
- `02:29:36`        75  momentum_gld                        last: 2026-04-24T09:10:14
- `02:29:36`        51  momentum_spy                        last: 2026-04-23T09:10:14
- `02:29:36`        28  momentum_tlt                        last: 2026-04-19T21:10:13
- `02:29:36`         3  momentum_uup                        last: 2026-04-08T21:10:13
- `02:29:36` 
  Has signal_type='khalid_index': True
## 2. learning/morning_run_log.json

- `02:29:37`   Type: dict, size: 351B
- `02:29:37`   Last modified: 2026-04-24 13:00:47+00:00
- `02:29:37`   Top keys: ['improved', 'khalid', 'outcomes', 'regime', 'run_at', 'weights', 'wrong']
- `02:29:37`   Sample: {"run_at": "2026-04-24T13:00:46.150730+00:00", "outcomes": 168, "wrong": 0, "improved": false, "weights": 12, "khalid": {"score": 43, "regime": "BEAR", "signals": [["DXY", -12, "118.1"], ["HY Spread", 5, "2.84%"], ["Unemployment", -8, "4.3%"], ["Net Liq", 3, "$5.70T"], ["SPY Trend", 5, "$708"]], "ts": "2026-04-24T12:54:56.676793"}, "regime": "BEAR"}
## 3. S3 keys under learning/

- `02:29:37`   Found 2 keys under learning/
- `02:29:37`     learning/last_log_run.json                               80B  age 2.1h
- `02:29:37`     learning/morning_run_log.json                           351B  age 13.5h
## 4. archive/ S3 sample (most recent 20)

- `02:29:37`   Total archive keys (collected so far): 1000
- `02:29:37`   Most recent 20:
- `02:29:37`     archive/intelligence/2026/04/25/0014.json                     4449B  age 2.3h
- `02:29:37`     archive/intelligence/2026/04/25/0010.json                     4264B  age 2.3h
- `02:29:37`     archive/intelligence/2026/04/25/0004.json                     3366B  age 2.4h
- `02:29:37`     archive/intelligence/2026/04/24/2305.json                     2785B  age 3.4h
- `02:29:37`     archive/intelligence/2026/04/24/2205.json                     2785B  age 4.4h
- `02:29:37`     archive/intelligence/2026/04/24/2105.json                     2785B  age 5.4h
- `02:29:37`     archive/intelligence/2026/04/24/2005.json                     2785B  age 6.4h
- `02:29:37`     archive/intelligence/2026/04/24/1905.json                     2785B  age 7.4h
- `02:29:37`     archive/intelligence/2026/04/24/1805.json                     2785B  age 8.4h
- `02:29:37`     archive/intelligence/2026/04/24/1705.json                     2785B  age 9.4h
- `02:29:37`     archive/intelligence/2026/04/24/1605.json                     2785B  age 10.4h
- `02:29:37`     archive/intelligence/2026/04/24/1505.json                     2785B  age 11.4h
- `02:29:37`     archive/intelligence/2026/04/24/1405.json                     2785B  age 12.4h
- `02:29:37`     archive/intelligence/2026/04/24/1305.json                     2785B  age 13.4h
- `02:29:37`     archive/intelligence/2026/04/24/1210.json                     2785B  age 14.3h
- `02:29:37`     archive/intelligence/2026/04/24/1205.json                     2785B  age 14.4h
- `02:29:37`     archive/intelligence/2026/04/23/2305.json                     2785B  age 27.4h
- `02:29:37`     archive/intelligence/2026/04/23/2205.json                     2785B  age 28.4h
- `02:29:37`     archive/intelligence/2026/04/23/2105.json                     2785B  age 29.4h
- `02:29:37`     archive/intelligence/2026/04/23/2005.json                     2785B  age 30.4h
## 5. Current data/report.json khalid_index value

- `02:29:38`   khalid_index: {"score": 43, "regime": "BEAR", "signals": [["DXY", -12, "118.1"], ["HY Spread", 5, "2.86%"], ["Unemployment", -8, "4.3%"], ["Net Liq", 3, "$5.70T"], ["SPY Trend", 5, "$714"]], "ts": "2026-04-25T02:25
- `02:29:38` Done
