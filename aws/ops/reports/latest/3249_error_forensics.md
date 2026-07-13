# ops 3249 — error forensics

**Status:** success  
**Duration:** 4.8s  
**Finished:** 2026-07-13T12:35:27+00:00  

## Data

| hours_since_last_error | last_error | last_error_at | n_fails | n_warns | singles_flagged | verdict |
|---|---|---|---|---|---|---|
|  | — | none-in-recent-streams |  |  |  |  |
| 9.0 |  |  |  |  |  |  |
|  |  |  |  |  | 1 |  |
|  |  |  | 0 | 1 |  | PASS |

## Log
## 1. wl-engines hourly error distribution (13h)

- `12:35:22`   23:00Z  
- `12:35:22`   00:00Z  
- `12:35:22`   01:00Z  
- `12:35:22`   02:00Z  ████████████████████████24
- `12:35:22`   03:00Z  ███3
- `12:35:22`   04:00Z  
- `12:35:22`   05:00Z  
- `12:35:22`   06:00Z  
- `12:35:22`   07:00Z  
- `12:35:24` ✅ wl-engines errors STOPPED — marathon-era, certified historical (fresh feed + clean recent streams)
## 2. Singles triage

- `12:35:24`   justhodl-consumer-pulse              last_err=—  …no recent error trace
- `12:35:25`   justhodl-cb-injection                last_err=—  …no recent error trace
- `12:35:25`   justhodl-theme-rotation-engine       last_err=11:00:19  ⚠ REAL
- `12:35:25`       [ERROR] AttributeError: 'NoneType' object has no attribute 'get'
- `12:35:25`   justhodl-khalid-metrics              last_err=—  …no recent error trace
- `12:35:26`   justhodl-boj-detail                  last_err=—  …no recent error trace
- `12:35:26`   justhodl-ka-metrics                  last_err=—  …no recent error trace
- `12:35:26`   justhodl-yen-carry                   last_err=—  …no recent error trace
- `12:35:27`   justhodl-snb-detail                  last_err=—  …no recent error trace
- `12:35:27` ⚠ 1 single-error functions carry real tracebacks — fix queue
