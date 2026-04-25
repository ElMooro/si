# Phase 1A — Bond Market Regime Detector

**Status:** success  
**Duration:** 9.1s  
**Finished:** 2026-04-25T15:33:17+00:00  

## Data

| indicators_extreme | invoke_s | n_signals | regime | regime_changed | regime_strength | zip_size |
|---|---|---|---|---|---|---|
| 0 | 1.6 | 7 | NEUTRAL | False | 57.9 | 14202 |

## Log
## 1. Verify data dependencies

- `15:33:08`   repo-data.json: 16,418B, age 212.2min
- `15:33:08`   fred-cache-secretary.json: 9,358B, age 127.2min
- `15:33:08`     BAMLH0A0HYM2: 30 history pts, latest=2.86
- `15:33:08`     BAMLC0A0CM: 30 history pts, latest=0.8
- `15:33:08`     T10Y2Y: 30 history pts, latest=0.53
- `15:33:08`     DTWEXBGS: 30 history pts, latest=118.0795
- `15:33:08`     T5YIE: 30 history pts, latest=2.61
- `15:33:08` ⚠     MOVE: NOT FOUND in repo-data
- `15:33:08`     NFCI (in systemic): value=-0.497, z=-0.011731914274663389
- `15:33:08` ⚠     VIXCLS: NOT FOUND in repo-data
## 2. Set up justhodl-bond-regime-detector Lambda

- `15:33:08` ✅   Wrote source: 14,034B, 381 LOC
- `15:33:08` ✅   Syntax OK
- `15:33:08`   Deployment zip: 14,202B
- `15:33:12` ✅   Created justhodl-bond-regime-detector
## 3. Test invoke

- `15:33:16` ✅   Invoked in 1.6s
- `15:33:16` 
  Response body:
- `15:33:16`     regime                    NEUTRAL
- `15:33:16`     previous_regime           NEUTRAL
- `15:33:16`     regime_changed            False
- `15:33:16`     regime_strength           57.9
- `15:33:16`     indicators_extreme        0
- `15:33:16`     n_signals                 7
## 4. Read regime/current.json

- `15:33:16`   Regime: NEUTRAL (strength: 57.9/100)
- `15:33:16`   Extreme: 0/7
- `15:33:16`   Risk-off signals: 0, Risk-on: 0
- `15:33:16`   Consensus: MIXED
- `15:33:16` 
  Per-indicator signals:
- `15:33:16`     HY OAS               z=-1.18  RISK_ON    value=2.86
- `15:33:16`     IG OAS               z=-1.26  RISK_ON    value=0.8
- `15:33:16`     NFCI                 z=-0.01  RISK_ON    value=-0.497
- `15:33:16`     VIX                  z=-0.97  RISK_ON    value=19.31
- `15:33:16`     2s10s velocity       z=-0.78  RISK_ON    value=0.53, Δ5d=-0.0200
- `15:33:16`     DXY 5d               z=-0.62  RISK_ON    value=118.079, Δ5d=-0.7757
- `15:33:16`     5Y BE 5d             z=+1.17  RISK_ON    value=2.61, Δ5d=+0.0500
## 5. EventBridge schedule cron(0 */4 * * ? *) — every 4h

- `15:33:17` ✅   Created rule cron(0 */4 * * ? *)
- `15:33:17` ✅   Added invoke permission
- `15:33:17` Done
