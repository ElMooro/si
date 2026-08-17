# ops 4799 -- settle-gated v2.4 verify

**Status:** success  
**Duration:** 632.8s  
**Finished:** 2026-08-17T00:10:30+00:00  

## Data

| check | env_FRED_API_KEY | env_TE_API_KEY | last_bp | value | waited_s | zip_kb |
|---|---|---|---|---|---|---|
|  | present |  |  |  |  |  |
|  |  | present |  |  |  |  |
| deployed_marker_2_4 |  |  |  | True |  | 112 |
| engine_completed |  |  |  | True | 631.7 |  |
| engine_v |  |  |  | 2.4 |  |  |
| daily_btp_sane |  |  | None | False |  |  |
| sftr_rows |  |  |  | 0 |  |  |
| board_total |  |  |  | 1634 |  |  |
| groups_total |  |  |  | 18 |  |  |

## Log
## 1. env keys (heal if deploy wiped them)

## 2. deploy settle: marker in deployed zip

## 3. async run + assertions

- `00:10:30` ⚠ DE10Y_TE: ABSENT
- `00:10:30` ⚠ IT10Y_TE: ABSENT
- `00:10:30` ⚠ FR10Y_TE: ABSENT
- `00:10:30` ⚠ ES10Y_TE: ABSENT
- `00:10:30` ⚠ D_BTP_BUND_D: ABSENT
- `00:10:30` ⚠ WREPOFOR: ABSENT
- `00:10:30` ⚠ WLRRAFOIAL: ABSENT
