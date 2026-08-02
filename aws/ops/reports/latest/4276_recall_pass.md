# ops 4276 -- glued-date recall pass

**Status:** success  
**Duration:** 15.5s  
**Finished:** 2026-08-02T17:43:52+00:00  

## Data

| amount | chamber | date | party | ticker | tx | who |
|---|---|---|---|---|---|---|
| $1,001 - $15,000 |  | 2026-07-30 |  | TSM | Purchase | Cleo Fields |
| $15,001 - $50,000 |  | 2026-07-27 |  | NVDA | Sale | Sam T. Liccardo |
| $1,001 - $15,000 |  | 2026-07-24 |  | ARCC | Sale | Pete Sessions |
| $1,001 - $15,000 |  | 2026-07-20 |  | FMAO | Purchase | Robert E. Latta |
| $1,001 - $15,000 |  | 2026-07-20 |  | BAC | Sale | James A. Himes |
| $15,001 - $50,000 |  | 2026-07-20 |  | XOM | Sale | James A. Himes |
|  | house |  | R | SPCX | purchase | Daniel Meuser |
|  | house |  | R | SPCX | purchase | William R. Timmons |
|  | house |  | R | SPCX | purchase | William R. Timmons |
|  | house |  | D | SPCX | purchase | Jared Moskowitz |
|  | house |  | D | HUBB | purchase | April McClain Delaney |
|  | house |  | D | HUBB | purchase | April McClain Delaney |

## Log
## 1. reparse the zero-row dozen on v1.1

- `17:43:51` invoked: {"ok": true, "new_docs": 20, "parsed": 20, "no_text": 0, "errors": 0, "elapsed_s": 4.9, "trades_total": 209}
- `17:43:51` histogram: {0: 8, 1: 13, 2: 5, 3: 2, 5: 1, 6: 1, 7: 1, 8: 1, 11: 1, 13: 1, 17: 1, 19: 1, 31: 1, 63: 1} -- trades_total=209
- `17:43:51` zero-rows split: true_miss=0 fund_only=8 unmeasured(pre-v1.1.1)=0
- `17:43:51` ✅ recall honest: 209 trades, zero-rows are fund-only or pre-metric
## 2. both chambers, attribution visible

- `17:43:52` invoked: {"statusCode": 200, "body": "{\"ok\": true, \"n_quiver\": 295, \"n_house\": 209, \"n_senate\": 86, \"n_tickers\": 153, \"n_clusters\": 13, \"n_bipartisan\": 4, \"duration_s\": 0.2}
- `17:43:52` chambers: senate=86 house=209 tickers=153
- `17:43:52` ✅ party attribution: 244/380 (64%) via name_map
## RESULT

- `17:43:52` ✅ OPS 4276 PASS -- recall recovered, two chambers live with visible attribution
