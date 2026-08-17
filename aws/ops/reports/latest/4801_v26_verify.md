# ops 4799 -- settle-gated v2.6 verify

**Status:** success  
**Duration:** 503.3s  
**Finished:** 2026-08-17T00:41:00+00:00  

## Data

| check | env_FRED_API_KEY | env_TE_API_KEY | last_usd | value | waited_s | zip_kb |
|---|---|---|---|---|---|---|
|  | present |  |  |  |  |  |
|  |  | present |  |  |  |  |
| deployed_marker_2_4 |  |  |  | True |  | 113 |
| engine_completed |  |  |  | True | 502.6 |  |
| engine_v |  |  |  | 2.6 |  |  |
| dtcc_tsy_sane_5bn_500bn |  |  | 34843330834.03 | True |  |  |
| sftr_rows |  |  |  | 12 |  |  |
| board_total |  |  |  | 1650 |  |  |
| groups_total |  |  |  | 20 |  |  |

## Log
## 1. env keys (heal if deploy wiped them)

## 2. deploy settle: marker in deployed zip

## 3. async run + assertions

- `00:41:00` diag: {"te_key_present": true, "te": {"germany": "HTTPError:HTTP Error 403: Forbidden", "italy": "HTTPError:HTTP Error 403: Forbidden", "france": "HTTPError:HTTP Error 403: Forbidden", "spain": "HTTPError:HTTP Error 403: Forbidden"}, "sftr": {"stage": "page", "links": 2, "files_new": 2, "series_put": 12}, "dtcc": {"live_rows": 250, "treasury": 250, "agency": 250}}
- `00:41:00` ✅ DTCC-TREASURY-FAILS: 2025-08-14 -> 2026-08-14 n=250 last=34843330834.03
- `00:41:00` ✅ DTCC-AGENCY-FAILS: 2025-08-14 -> 2026-08-14 n=250 last=40577210.08
- `00:41:00` ✅ WREPOFOR: 2002-12-18 -> 2026-08-12 n=1235 last=349641.0
- `00:41:00` ⚠ SFTR-EU-outstanding-all-sfts-total-sft-cash-value-eur-mn: ABSENT
- `00:41:00` ✅ sftr sample: SFTR-EU-newt-5-6-cash-value-eur-mn last=18886667.515700083
