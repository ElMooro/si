# ops 4915 -- floor-audit v1.0.1 quarantine ladder

**Status:** success  
**Duration:** 135.8s  
**Finished:** 2026-08-19T19:32:38+00:00  

## Data

| alert | alerts | as_of | blocklist_n | cov | critical | critical_n | crypto | dat | dd20 | duration_s | edge | fund_wrappers | g0_ok | g1 | g2 | g3 | g4 | g5 | g6 | mcap_m | nlav_m | reason | res | res20 | residual_flags | sense | sev | status | suspect_inputs | universe | v | verdict | version_src |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  |  |  |  |  |  |  |  | PASS |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 1.0.1 |
|  |  |  | 16 |  |  |  |  |  |  |  |  |  |  |  | PASS |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  | 2026-08-19T19:31:02+00:00 |  |  |  |  |  |  |  |  |  |  | 28 |  |  | PASS |  |  |  |  |  |  |  |  |  |  |  |  |  | 33 |  |  |  |
|  | 8 |  |  |  |  |  |  |  |  |  |  | ARKB,BMNR,ETHA,ETHB,EZBC,HODL,IBIT |  |  |  |  | PASS |  |  |  |  |  |  |  |  |  |  |  | GDC |  |  |  |  |
|  |  |  |  |  | AIFC,CNTN,TONX,UPXI,HOOD,FWDI | 6 |  |  |  |  |  |  |  |  |  |  |  | PASS |  |  |  |  |  |  | ABTC(MEDIUM),HIVE(MEDIUM) |  |  |  |  |  |  |  |  |
| AIFC |  |  |  | 6.5575 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | -0.816459 |  |  | 0 | CRITICAL |  |  |  | BELOW_LIQUID_FLOOR |  |  |
| CNTN |  |  |  | 3.9689 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | -0.651316 |  |  | 0 | CRITICAL |  |  |  | BELOW_LIQUID_FLOOR |  |  |
| TONX |  |  |  | 3.0004 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | -0.525353 |  |  | 0 | CRITICAL |  |  |  | BELOW_LIQUID_FLOOR |  |  |
| UPXI |  |  |  | 1.7192 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | None |  |  | None | CRITICAL |  |  |  | BELOW_LIQUID_FLOOR |  |  |
| HOOD |  |  |  | 1.6213 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | None |  |  | None | CRITICAL |  |  |  | BELOW_LIQUID_FLOOR |  |  |
| FWDI |  |  |  | 1.1071 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | None |  |  | None | CRITICAL |  |  |  | BELOW_LIQUID_FLOOR |  |  |
| ABTC |  |  |  | 0.7961 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | -0.575644 |  |  | 0 | MEDIUM |  |  |  | SENSELESS_DRAWDOWN |  |  |
| HIVE |  |  |  | 0.3107 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | -0.436773 |  |  | 0 | MEDIUM |  |  |  | STRETCHED |  |  |
|  |  |  |  | 0.0497 |  |  | 0.0047 | BTBT | -0.159375 |  |  |  |  |  |  |  |  |  |  | 485 | 24 |  |  | -0.159805 |  | 0 | NONE |  |  |  |  | IN_LINE |  |
|  |  |  |  | 0.9726 |  |  | 0.9443 | BMNR | 0.0 |  |  |  |  |  |  |  |  |  |  | 12019 | 11690 |  |  | -0.086436 |  | None | NONE |  |  |  |  | FUND_WRAPPER |  |
|  |  |  |  |  |  |  |  | MSTR |  |  |  |  |  |  |  |  |  |  |  |  |  | no shares series |  |  |  |  |  | SKIP |  |  |  |  |  |
|  |  |  |  | 0.0377 |  |  | 0.0009 | SBET | 0.0 |  |  |  |  |  |  |  |  |  |  | 1517 | 57 |  |  | -8.2e-05 |  | None | NONE |  |  |  |  | IN_LINE |  |
|  |  |  |  | -0.4193 |  |  | 0.6968 | DFDV | 0.0 |  |  |  |  |  |  |  |  |  |  | 104 | -44 |  |  | -0.072357 |  | None | NONE |  |  |  |  | IN_LINE |  |
|  |  |  |  | 1.7192 |  |  | 2.9746 | UPXI | -0.007264 |  |  |  |  |  |  |  |  |  |  | 61 | 106 |  |  | -0.316152 |  | None | CRITICAL |  |  |  |  | BELOW_LIQUID_FLOOR |  |
|  |  |  |  | 0.7961 |  |  | 0.8356 | ABTC | 0.0 |  |  |  |  |  |  |  |  |  |  | 667 | 531 |  |  | -0.044324 |  | 0 | MEDIUM |  |  |  |  | SENSELESS_DRAWDOWN |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | PASS |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  | PENDING |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  | 135 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | GREEN |  |  |  |  |  |

## Log
## G1 redeploy

- `19:30:22`   zip: 108578 bytes
## 1. Lambda

- `19:30:22`   Lambda exists — updating
- `19:30:29` ✅   ✓ updated justhodl-floor-audit
## 3. Smoke test

- `19:30:29`   invoking justhodl-floor-audit…
## G2 config reset

## G3 fresh run

## G4 alert hygiene

## G5 real edges intact

## G6 DAT board

## edge (soft)

