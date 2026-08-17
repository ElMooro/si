# G0. FIELD-level contracts + pre snapshot

**Status:** success  
**Duration:** 62.4s  
**Finished:** 2026-08-17T15:59:23+00:00  

## Data

| pre_composite | pre_fleet_adj | pre_funding_fused | pre_posture | pre_sizing |
|---|---|---|---|---|
| -0.352 | -0.2 | -2.0 | RISK_OFF | 0.45 |

## Log
- `15:58:21` ✅   G0 plumbing LIVE age=0.2h scarcity_z=-0.78 breadth=0.41 composite=-0.295
# 1. settle v2.4

- `15:58:22` ✅ marker settled (attempt 1)
# 2. Event-invoke + poll (<=8 min, FRED replay)

- `15:59:23` ✅ fresh doc in 61s elapsed_s=48.7
# 3. truths

- `15:59:23` ✅   marker == v2.4
- `15:59:23` ✅   input plumbing_board_composite     OK adj=+0.00 age=0.2h
- `15:59:23` ✅   input plumbing_scarcity_haircuts   OK adj=+0.00 age=0.2h
- `15:59:23` ✅   score_adjs == independent recompute (+0.00, +0.00)
- `15:59:23` ✅   calm-tape prediction held: plumbing leaves the gate untouched today
- `15:59:23` ✅   5 legacy funding inputs preserved
- `15:59:23` ✅   fleet_adj identity (-0.200)
- `15:59:23` ✅   score_fused identity (-2.000)
- `15:59:23` ✅   live composite identity (-0.352)
- `15:59:23` ✅   posture mapping identity (RISK_OFF)
- `15:59:23` ✅   replay purity canary intact (flips=36)
- `15:59:23` ✅   pre->post unchanged (posture RISK_OFF, fleet_adj -0.2) -- calm wiring proven inert
# 4. readout

- `15:59:23`   dealer_net_treasury_b        OK       adj=+0.00
- `15:59:23`   fails_cross_z                MISSING  adj=+0.00
- `15:59:23`   auction_10y_grade            OK       adj=+0.00
- `15:59:23`   plumbing_composite           OK       adj=+0.20
- `15:59:23`   xcc_basis_signals            OK       adj=-0.40
- `15:59:23`   plumbing_board_composite     OK       adj=+0.00
- `15:59:23`   plumbing_scarcity_haircuts   OK       adj=+0.00
- `15:59:23`   funding score=-2.0 fleet_adj=-0.2 fused=-2.0
- `15:59:23`   posture=RISK_OFF composite=-0.352 sizing=0.45
# 5. verdict

- `15:59:23` ✅ risk-gate v2.4 LIVE -- Fusion 2 complete: the repo master board reaches sizing through two stress-only funding inputs; calm tape verified inert, replay purity intact
