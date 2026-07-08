## 1. Preflight dogfood (lints itself + the new tools)

**Status:** success  
**Duration:** 0.7s  
**Finished:** 2026-07-08T20:20:16+00:00  

## Data

| dollar_radar_json | gen_state_out | gen_state_rc | n_fails | n_warns | preflight_out | preflight_rc | repo_market_json | risk_regime_json | verdict |
|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  | WARN: aws/ops/_preflight.py: classic EventBridge put_rule -- rule cap is SATURATED; use EventBridge Scheduler
PREFLIGHT PASS (3 file(s), 1 warn(s))
 | 0 |  |  |  |
|  | STATE.md regenerated: next_free=3007 pending=17 reports=229
 | 0 |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  | 6.7h |  |  |
| 1.1h |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  | 7.6h |  |
|  |  |  | 0 | 0 |  |  |  |  | PASS |

## Log
## 2. STATE.md generator runs clean

## 3. Engine health ping (repo-market + fused consumers)

## verdict

- `20:20:16` PASS -- concurrency + STATE.md + preflight all live
