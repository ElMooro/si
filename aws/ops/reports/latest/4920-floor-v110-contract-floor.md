# ops 4920 -- floor-audit v1.1.0 contract floor

**Status:** success  
**Duration:** 235.5s  
**Finished:** 2026-08-20T01:32:11+00:00  

## Data

| alert | alerts | as_of | attempts | backlog_musd | backlog_seeded | btbt | btbt_cov | btbt_crypto | committed | committed_bound_n | committed_floor | committed_high | contract_floors | cov | cov_x | duration_s | feed_version | g0_ok | g1 | g2 | g3 | g4 | g5 | g6 | page_marker | rpo_musd | seed_max_add | seed_min_usd | sense | sev | status | universe | v | verdict | was |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | PASS |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  | 1.5 | 3.0 |  |  |  |  |  |  |  | PASS |  |  |  |  |  |  | 15 | 300000000.0 |  |  |  |  |  |  |  |
|  | 9 | 2026-08-20T01:29:07+00:00 |  |  | ACM,APH,BA,CAT,EMR,ETN,FSLR,GD,HON,LHX,NOC,NOW,PWR,RTX,WDAY |  |  |  |  |  |  |  |  |  |  |  |  | 43 |  |  | PASS |  |  |  |  |  |  |  |  |  |  | 48 |  |  | 33 |
|  |  |  |  |  |  |  |  |  |  | 19 |  |  |  |  |  |  |  |  |  |  |  | PASS |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  | 694709 |  |  |  |  | BA |  |  |  |  |  | 4.0728 |  |  |  |  |  |  |  |  |  |  | 715261 |  |  |  |  | MINED |  |  | BACKLOG_FLOOR |  |
|  |  |  |  | 61000 |  |  |  |  | FSLR |  |  |  |  |  | 2.5522 |  |  |  |  |  |  |  |  |  |  | None |  |  |  |  | MINED |  |  | BACKLOG_FLOOR |  |
|  |  |  |  | 39700 |  |  |  |  | ACM |  |  |  |  |  | 2.4372 |  |  |  |  |  |  |  |  |  |  | 20400 |  |  |  |  | MINED |  |  | BACKLOG_FLOOR |  |
|  |  |  |  | 76000 |  |  |  |  | NOC |  |  |  |  |  | 1.2645 |  |  |  |  |  |  |  |  |  |  | 104700 |  |  |  |  | MINED |  |  | IN_LINE |  |
|  |  |  |  | 186900 |  |  |  |  | GD |  |  |  |  |  | 1.1122 |  |  |  |  |  |  |  |  |  |  | 118000 |  |  |  |  | MINED |  |  | IN_LINE |  |
|  |  |  |  | 271000 |  |  |  |  | RTX |  |  |  |  |  | 0.9731 |  |  |  |  |  |  |  |  |  |  | 289000 |  |  |  |  | MINED |  |  | IN_LINE |  |
|  |  |  |  | 42000 |  |  |  |  | LHX |  |  |  |  |  | 0.8139 |  |  |  |  |  |  |  |  |  |  | 42000 |  |  |  |  | MINED |  |  | IN_LINE |  |
|  |  |  |  | 37500 |  |  |  |  | HON |  |  |  |  |  | 0.5408 |  |  |  |  |  |  |  |  |  |  | 38008 |  |  |  |  | MINED |  |  | IN_LINE |  |
|  |  |  |  |  |  | SENSELESS_DRAWDOWN | 0.7265 | 0.6833 |  |  |  |  | ACM,BA,FSLR |  |  |  |  |  |  |  |  |  | PASS |  |  |  |  |  |  |  |  |  |  |  |  |
| AIFC |  |  |  |  |  |  |  |  |  |  |  |  |  | 6.6455 |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 0 | CRITICAL |  |  | BELOW_LIQUID_FLOOR |  |  |
| CNTN |  |  |  |  |  |  |  |  |  |  |  |  |  | 3.9439 |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 0 | CRITICAL |  |  | BELOW_LIQUID_FLOOR |  |  |
| BMNR |  |  |  |  |  |  |  |  |  |  |  |  |  | 1.0291 |  |  |  |  |  |  |  |  |  |  |  |  |  |  | None | CRITICAL |  |  | BELOW_LIQUID_FLOOR |  |  |
| UPXI |  |  |  |  |  |  |  |  |  |  |  |  |  | 1.7982 |  |  |  |  |  |  |  |  |  |  |  |  |  |  | None | CRITICAL |  |  | BELOW_LIQUID_FLOOR |  |  |
| TONX |  |  |  |  |  |  |  |  |  |  |  |  |  | 3.0091 |  |  |  |  |  |  |  |  |  |  |  |  |  |  | None | CRITICAL |  |  | BELOW_LIQUID_FLOOR |  |  |
| FWDI |  |  |  |  |  |  |  |  |  |  |  |  |  | 1.1148 |  |  |  |  |  |  |  |  |  |  |  |  |  |  | None | CRITICAL |  |  | BELOW_LIQUID_FLOOR |  |  |
| BTBT |  |  |  |  |  |  |  |  |  |  |  |  |  | 0.7265 |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 0 | HIGH |  |  | SENSELESS_DRAWDOWN |  |  |
| ABTC |  |  |  |  |  |  |  |  |  |  |  |  |  | 0.8263 |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 0 | MEDIUM |  |  | SENSELESS_DRAWDOWN |  |  |
| HIVE |  |  |  |  |  |  |  |  |  |  |  |  |  | 0.3049 |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 0 | MEDIUM |  |  | STRETCHED |  |  |
|  |  |  | 7 |  |  |  |  |  |  |  |  |  |  |  |  |  | 1.1.0 |  |  |  |  |  |  | PASS | floor-audit-v1.1.0 |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 235 |  |  |  |  |  |  |  |  |  |  |  |  |  |  | GREEN |  |  |  |  |

## Log
## G1 deploy

- `01:28:16`   zip: 110820 bytes
## 1. Lambda

- `01:28:16`   Lambda exists — updating
- `01:28:19` ✅   ✓ updated justhodl-floor-audit
## 3. Smoke test

- `01:28:20`   invoking justhodl-floor-audit…
## G2 config reset

## G3 fresh run

## G4 order-book leg

## G5 ladder integrity (live tape)

## G6 edge

