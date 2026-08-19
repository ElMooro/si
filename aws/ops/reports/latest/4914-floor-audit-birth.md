# ops 4914 -- floor-audit birth

**Status:** success  
**Duration:** 224.2s  
**Finished:** 2026-08-19T19:26:53+00:00  

## Data

| alert | alerts | as_of | backlog_join | cov | coverage | crypto_coverage | dd20 | discovered | donor | edge | feed | fn | g0_ok | g1 | g2 | g3 | g4 | g5 | g6 | key_len | mcap_musd | nlav_musd | note | page | probe | residual20 | schedule | sense | sev | severity | snapshot | status | universe | v | verdict |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  |  |  | justhodl-equity-research |  |  |  |  | PASS |  |  |  |  |  | 32 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  | justhodl-floor-audit |  |  | PASS |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | PASS |  |  |  |  |  |  |  |  |  |  | created |  |  |  |  |  |  |  |  |
|  | 11 | 2026-08-19T19:23:46+00:00 |  |  |  |  |  | 19 |  |  |  |  | 28 |  |  |  | PASS |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 33 |  |  |
|  |  |  |  |  | 0.0497 | 0.0047 | -0.159375 |  |  |  |  |  |  |  |  |  |  | PASS |  |  | 485.1 | 24.1 |  |  | BTBT | -0.15982 |  | 0 |  | NONE |  |  |  |  | IN_LINE |
| GLXY |  |  |  | 3040406.2685 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 0 | CRITICAL |  |  |  |  | BELOW_LIQUID_FLOOR |  |
| GDC |  |  |  | 58.8413 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 0 | CRITICAL |  |  |  |  | BELOW_LIQUID_FLOOR |  |
| AIFC |  |  |  | 6.6154 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 0 | CRITICAL |  |  |  |  | BELOW_LIQUID_FLOOR |  |
| UPXI |  |  |  | 1.7315 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 0 | CRITICAL |  |  |  |  | BELOW_LIQUID_FLOOR |  |
| CNTN |  |  |  | 3.9737 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 0 | CRITICAL |  |  |  |  | BELOW_LIQUID_FLOOR |  |
|  |  |  | bound |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | PASS |  |  |  |  |  |  |  |  |  |  |  | data/floor-audit/history/2026-08-19.json |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  | PENDING |  |  |  |  |  |  |  |  |  |  |  |  | pages.yml may still be publishing -- repo state is not proof of live; recheck next op if PENDING |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  | https://justhodl.ai/data/floor-audit.json |  |  |  |  |  |  |  |  |  |  |  |  | https://justhodl.ai/floor.html |  |  |  |  |  |  |  | GREEN |  |  |  |

## Log
## G1 key inheritance

## G2 deploy

- `19:23:09`   zip: 107942 bytes
## 1. Lambda

- `19:23:10`   Lambda exists — updating
- `19:23:15` ✅   ✓ updated justhodl-floor-audit
- `19:23:15` ✅   ✓ Function URL: https://2svmcowoxlyhacedhjgjbdmsni0yqmve.lambda-url.us-east-1.on.aws/
## 3. Smoke test

- `19:23:16`   invoking justhodl-floor-audit…
## G3 schedule

## G4 first run

- `19:23:32` async invoke fired; polling data/floor-audit.json for fresh as_of (prev=2026-08-19T19:23:30+00:00)
## G5 payload assertions

## G6 history

## edge (soft)

