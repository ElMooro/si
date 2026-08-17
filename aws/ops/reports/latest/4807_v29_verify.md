# ops 4799 -- settle-gated v2.9 verify

**Status:** success  
**Duration:** 515.7s  
**Finished:** 2026-08-17T01:51:38+00:00  

## Data

| check | env_FRED_API_KEY | env_TE_API_KEY | last_pct | value | waited_s | zip_kb |
|---|---|---|---|---|---|---|
|  | present |  |  |  |  |  |
|  |  | present |  |  |  |  |
| deployed_marker_2_4 |  |  |  | True |  | 114 |
| engine_completed |  |  |  | True | 510.7 |  |
| engine_v |  |  |  | 2.9 |  |  |
| bund_sane_0_8pct |  |  | 3.22 | True |  |  |
| bund_minus_aaa_bp |  |  |  | 5.433497199999993 |  |  |
| sftr_rows |  |  |  | 12 |  |  |
| sftr_junk_rows_on_board |  |  |  | 0 |  |  |
| board_total |  |  |  | 1653 |  |  |

## Log
## 1. env keys (heal if deploy wiped them)

## 2. deploy settle: marker in deployed zip

## 3. async run + assertions

- `01:51:35` diag: {"bbk_de": "ok:7366", "ecb_yc": "ok:5608", "te_key_present": true, "te": {"germany": "HTTPError:HTTP Error 403: Forbidden", "italy": "HTTPError:HTTP Error 403: Forbidden", "france": "HTTPError:HTTP Error 403: Forbidden", "spain": "HTTPError:HTTP Error 403: Forbidden"}, "sftr": {"stage": "page", "links": 2, "files_new": 2}, "dtcc": {"live_rows": 250, "treasury": 250, "agency": 250}}
- `01:51:35` ✅ DE10Y_BBK: 1997-08-07 -> 2026-08-14 n=7366 last=3.22
- `01:51:35` ✅ EA_AAA_10Y: 2004-09-06 -> 2026-08-13 n=5608 last=3.155665028
- `01:51:35` ✅ D_BUND_EA_AAA: 2004-09-06 -> 2026-08-13 n=5573 last=5.433497199999993
- `01:51:35` ✅ DTCC-TREASURY-FAILS: 2025-08-14 -> 2026-08-14 n=250 last=34843330834.03
- `01:51:35` ✅ SFTR-EU-outstanding-all-sfts-total-sft-cash-value-eur-mn: 2026-08-07 -> 2026-08-07 n=1 last=18635629.57903577
- `01:51:35` ✅ sftr sample: SFTR-EU-newt-all-sfts-total-sft-cash-value-eur-mn last=18886667.515700083
## ECB FM wildcard discovery: per-country daily gov-bond yields?

- `01:51:36` FM[D..EUR.4F.BB..YLDA]: HTTPError: HTTP Error 404: Not Found
- `01:51:37` FM[D..EUR.4F.BB..YLD]: HTTPError: HTTP Error 404: Not Found
- `01:51:37` FM[D.IT.EUR.4F.BB..]: HTTPError: HTTP Error 404: Not Found
- `01:51:38` FM[D.DE.EUR.4F.BB..]: HTTPError: HTTP Error 404: Not Found
