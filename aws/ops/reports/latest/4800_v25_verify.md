# ops 4799 -- settle-gated v2.5 verify

**Status:** success  
**Duration:** 531.5s  
**Finished:** 2026-08-17T00:24:14+00:00  

## Data

| check | env_FRED_API_KEY | env_TE_API_KEY | last_bp | value | waited_s | zip_kb |
|---|---|---|---|---|---|---|
|  | present |  |  |  |  |  |
|  |  | present |  |  |  |  |
| deployed_marker_2_4 |  |  |  | True |  | 113 |
| engine_completed |  |  |  | True | 530.6 |  |
| engine_v |  |  |  | 2.5 |  |  |
| daily_btp_sane |  |  | None | False |  |  |
| sftr_rows |  |  |  | 0 |  |  |
| board_total |  |  |  | 1636 |  |  |
| groups_total |  |  |  | 18 |  |  |

## Log
## 1. env keys (heal if deploy wiped them)

## 2. deploy settle: marker in deployed zip

## 3. async run + assertions

- `00:24:14` diag: {"te_key_present": true, "te": {"germany": "HTTPError:HTTP Error 403: Forbidden", "italy": "HTTPError:HTTP Error 403: Forbidden", "france": "HTTPError:HTTP Error 403: Forbidden", "spain": "HTTPError:HTTP Error 403: Forbidden"}, "sftr": {"stage": "page", "links": 2}}
- `00:24:14` ⚠ DE10Y_TE: ABSENT
- `00:24:14` ⚠ IT10Y_TE: ABSENT
- `00:24:14` ⚠ FR10Y_TE: ABSENT
- `00:24:14` ⚠ ES10Y_TE: ABSENT
- `00:24:14` ⚠ D_BTP_BUND_D: ABSENT
- `00:24:14` ✅ WREPOFOR: 2002-12-18 -> 2026-08-12 n=1235 last=349641.0
- `00:24:14` ✅ WLRRAFOIAL: 2002-12-18 -> 2026-08-12 n=1235 last=357392.0
