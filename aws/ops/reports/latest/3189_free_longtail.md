# ops 3189 — free long-tail: on-chain + COT covered internally at $0

**Status:** success  
**Duration:** 356.0s  
**Finished:** 2026-07-13T02:33:51+00:00  

## Data

| active | coverage_before | coverage_now | engines | firing | mapped_before | mapped_now | n_fails | n_warns | new_coinmetrics | new_cot | new_futures | probed | pruned | series_cached | src_coingecko | src_coinmetrics | src_cot | src_formula | src_fred | src_internals | src_market | src_worldbank | survivors | symbols | unmapped_before | verdict |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  | 74.1 |  |  |  | 4822 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 6507 | 1685 |  |
|  |  |  |  |  |  |  |  |  | 18 | 76 | 4 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  | 94 | 21 |  |  |  |  |  |  |  |  |  | 73 |  |  |  |
|  | 74.1 | 75.3 |  |  |  | 4899 |  |  |  |  |  |  |  |  | 37 | 18 | 76 | 337 | 833 | 13 | 2308 | 1298 |  |  |  |  |
| 161 |  |  | 161 | 0 |  |  |  |  |  |  |  |  |  | 2224 |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  | 0 | 1 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | PASS |

## Log
## 1. Residual gap census (post-3188)

- `02:27:56`   FTSE                 448 symbols
- `02:27:56`   ECONOMICS            383 symbols
- `02:27:56`   INTOTHEBLOCK         147 symbols
- `02:27:56`   USI                   95 symbols
- `02:27:56`   TVC                   91 symbols
- `02:27:56`   GLASSNODE             59 symbols
- `02:27:56`   COT3                  52 symbols
- `02:27:56`   CBOEEU                40 symbols
- `02:27:56`   EUREX                 38 symbols
- `02:27:56`   ICEEUR                30 symbols
- `02:27:56`   CME                   25 symbols
- `02:27:56`   COT                   24 symbols
- `02:27:56`   CME_MINI              16 symbols
- `02:27:56`   DFM                   14 symbols
- `02:27:56`   ADX                   13 symbols
- `02:27:56`   DJCFD                 12 symbols
- `02:27:56`   MULTPL                12 symbols
- `02:27:56`   CBOT                   8 symbols
- `02:27:56`   GLASSNODE      e.g. GLASSNODE:BTC_ATHDRAWDOWN; GLASSNODE:BTC_NEWADDRESSES; GLASSNODE:BTC_RECEIVINGADDRESSES; GLASSNODE:BTC_SENDINGADDRESSES
- `02:27:56`   INTOTHEBLOCK   e.g. INTOTHEBLOCK:BTCL_BEARSCOUNT; INTOTHEBLOCK:BTCL_BULLSCOUNT; INTOTHEBLOCK:BTC_BEARSCOUNT; INTOTHEBLOCK:BTC_BULLSCOUNT
- `02:27:56`   COT3           e.g. COT3:045601_F_LMP_L; COT3:098662_F_DP_L; COT3:098662_F_DP_S; COT3:099741_FO_AMP_L
## 2. Re-map (new: COINMETRICS, COT, extended FUT roots)

## 3. Probe (real fetches — dry entries are pruned)

- `02:32:01`   GLASSNODE      probed   3  hit   0  dry   3
- `02:32:01`   INTOTHEBLOCK   probed  15  hit   2  dry  13
- `02:32:01`   COT3           probed  52  hit  48  dry   4
- `02:32:01`     ✓ INTOTHEBLOCK:BTC_INFLOWTXCOUNT → btc|TxCnt  (6400 pts)
- `02:32:01`     ✓ INTOTHEBLOCK:BTC_HASHRATE → btc|HashRate  (6394 pts)
- `02:32:01`     ✓ COT3:045601_F_LMP_L → 6dca-aqww|045601|noncomm_long  (1829 pts)
- `02:32:01`     ✓ COT:088691_F_OI → 6dca-aqww|088691|open_interest  (1828 pts)
- `02:32:01`     ✓ COT:067651_F_CP_L → 6dca-aqww|067651|noncomm_long  (1827 pts)
## 4. Redeploy shared bundle (series_source.py changed)

- `02:32:01`   zip: 73361 bytes
## 1. Lambda

- `02:32:01`   Lambda exists — updating
- `02:32:07` ✅   ✓ updated justhodl-wl-engines
## 2. EB rule + permissions

- `02:32:07`   rule already correct: wl-engines-daily (cron(30 22 ? * TUE-SAT *))
- `02:32:07` ✅   ✓ target → justhodl-wl-engines
- `02:32:07` ✅   ✓ added invoke permission
- `02:32:08`   zip: 74980 bytes
## 1. Lambda

- `02:32:08`   Lambda exists — updating
- `02:32:13` ✅   ✓ updated justhodl-thesis-engine
## 2. EB rule + permissions

- `02:32:13`   rule already correct: thesis-engine-daily (cron(45 22 ? * TUE-SAT *))
- `02:32:13` ✅   ✓ target → justhodl-thesis-engine
- `02:32:13` ✅   ✓ added invoke permission
- `02:32:13`   zip: 71021 bytes
## 1. Lambda

- `02:32:14`   Lambda exists — updating
- `02:32:19` ✅   ✓ updated justhodl-symbol-dictionary
## 2. EB rule + permissions

- `02:32:19`   rule already correct: symbol-dictionary-weekly (cron(0 5 ? * SUN *))
- `02:32:19` ✅   ✓ target → justhodl-symbol-dictionary
- `02:32:19` ✅   ✓ added invoke permission
## 5. Re-run wl-engines

- `02:33:51` ✅ fleet re-ran on the widened map — 161 active engines
## 6. Residue after this ops

- `02:33:51`   FTSE                 448
- `02:33:51`   ECONOMICS            383
- `02:33:51`   INTOTHEBLOCK         145
- `02:33:51`   USI                   95
- `02:33:51`   TVC                   91
- `02:33:51`   GLASSNODE             59
- `02:33:51`   CBOEEU                40
- `02:33:51`   EUREX                 38
- `02:33:51`   ICEEUR                30
- `02:33:51`   CME                   24
- `02:33:51`   next free plays: ECONOMICS residuals via curated IMF/BIS DBnomics templates; EUREX/ICEEUR → cash-proxy flags;
- `02:33:51`   FTSE 4xxx family is LICENSED — no vendor at any tested tier (3187/3188); candidates for honest retirement.
- `02:33:51` ⚠ GLASSNODE: 0 probe hits — tile grammar needs a curated pass (see census examples above)
