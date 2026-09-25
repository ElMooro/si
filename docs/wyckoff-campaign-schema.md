# Wyckoff campaign schema (locked 2026-09-25)

Projection over live `data/bottom.json` (justhodl-bottom v1.2.0). No separate pump.json this turn. Chart, desk, and Khalid all project the same map.

## Campaign map

| Campaign | Live states | Meaning |
| --- | --- | --- |
| BOTTOM | CLIMAX, TESTING, NO_RALLY, NO_TEST_BREAKOUT | Proof 1 stopping action. Not a buy. |
| ACCUM | ST_CONFIRMED, TRIGGERED | Proofs 2-4. Wait for LPS / hinge. |
| PUMP | MARKUP, COMPLETED | Proof 5 SOS. Start of bull run. |
| ABORT | FAILED, STOPPED | Sequence died. |

## Additive row fields

```
campaign        BOTTOM | ACCUM | PUMP | ABORT | WATCH
proofs          [{id, label, passed, evidence}]  P1_SC P2_AR P3_ST P4_SPRING P5_SOS
proofs_passed   0-5
hinge           {hinge, springboard, approach_vol_slope, approach_range_x, st_vol_ratio_sc, why}
pump_start      {flag, ready, chase, rule}
```

Hinge rule: `approach_vol_slope < 0 AND approach_range_x < 1 AND st_vol_ratio_sc <= 0.4`

Pump ready (LPS): campaign ACCUM AND hinge AND (spring OR higher-low).
Pump chase: campaign PUMP AND bars_in_state > 5 — do not buy, already running.

## Button / workspace IDs

- `btn-bot` → `jhOpenWorkspace("bottom")`
- `btn-pump` → `jhOpenWorkspace("pump")`

## Historical (live base_rates only — do not invent)

From `data/bottom.json` base_rates as of 2026-09-25, 5y window:

- 6560 SC sequences, 2032 triggered
- unmanaged trigger: 63-bar median +2.55%, hit 55%
- Grade A: 63-bar median +19.85%, hit 89% (n=464)
- quiet tests <=0.4x SC vol: 63-bar median +3.2% vs loud +1.9%
- crowd AR bounce: 63-bar median +5.64%, hit 60%, DD -12.2%, 49% later undercut SC low
- paper stop 0.1 ATR under test: stop hit 62%, managed 63-bar median -6.0%

Quiet-test outperformance is the hinge/dry-up edge from transcript 2. Crowd bounce is why Proof 1 is not an entry.
