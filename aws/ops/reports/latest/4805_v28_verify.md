# ops 4799 -- settle-gated v2.8 verify

**Status:** success  
**Duration:** 636.5s  
**Finished:** 2026-08-17T01:33:34+00:00  

## Data

| check | env_FRED_API_KEY | env_TE_API_KEY | last_usd | value | waited_s | zip_kb |
|---|---|---|---|---|---|---|
|  | present |  |  |  |  |  |
|  |  | present |  |  |  |  |
| deployed_marker_2_4 |  |  |  | True |  | 113 |
| engine_completed |  |  |  | True | 539.5 |  |
| engine_v |  |  |  | 2.8 |  |  |
| dtcc_tsy_sane_5bn_500bn |  |  | 34843330834.03 | True |  |  |
| sftr_rows |  |  |  | 12 |  |  |
| sftr_junk_rows_on_board |  |  |  | 0 |  |  |
| board_total |  |  |  | 1650 |  |  |
| groups_total |  |  |  | 20 |  |  |

## Log
## 1. env keys (heal if deploy wiped them)

## 2. deploy settle: marker in deployed zip

## 3. async run + assertions

- `01:31:59` diag: {"te_key_present": true, "te": {"germany": "HTTPError:HTTP Error 403: Forbidden", "italy": "HTTPError:HTTP Error 403: Forbidden", "france": "HTTPError:HTTP Error 403: Forbidden", "spain": "HTTPError:HTTP Error 403: Forbidden"}, "sftr": {"stage": "page", "links": 2, "files_new": 2}, "dtcc": {"live_rows": 250, "treasury": 250, "agency": 250}}
- `01:31:59` ✅ DTCC-TREASURY-FAILS: 2025-08-14 -> 2026-08-14 n=250 last=34843330834.03
- `01:31:59` ✅ DTCC-AGENCY-FAILS: 2025-08-14 -> 2026-08-14 n=250 last=40577210.08
- `01:31:59` ✅ WREPOFOR: 2002-12-18 -> 2026-08-12 n=1235 last=349641.0
- `01:31:59` ✅ SFTR-EU-outstanding-all-sfts-total-sft-cash-value-eur-mn: 2026-08-07 -> 2026-08-07 n=1 last=18635629.57903577
- `01:31:59` ✅ sftr sample: SFTR-EU-newt-all-sfts-total-sft-cash-value-eur-mn last=18886667.515700083
## opportunistic Wayback CDX retry

- `01:32:04` cdx attempt 0: HTTPError
- `01:32:29` cdx attempt 1: HTTPError
- `01:33:34` cdx attempt 2: HTTPError
- `01:33:34` archive.org still down -- 4803 pattern will be re-run in a later session
