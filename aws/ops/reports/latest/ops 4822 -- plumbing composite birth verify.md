# G0. FIELD-level feed contracts

**Status:** success  
**Duration:** 14.3s  
**Finished:** 2026-08-17T15:48:23+00:00  

## Data

| mem | state | timeout |
|---|---|---|
| 512 | Active | 180 |

## Log
- `15:48:09` ✅   G0 board rows = 832
- `15:48:10` ✅   G0 D_SOFR_IORB            last=-2.9999999999999805 @ 2026-08-14
- `15:48:10` ✅   G0 D_SOFR_P75_P25         last=8.000000000000007 @ 2026-08-14
- `15:48:10` ✅   G0 D_DVP_SOFR             last=0.9999999999999787 @ 2026-08-04
- `15:48:10` ✅   G0 D_BUND_EA_AAA          last=0.7437903500000065 @ 2026-08-14
- `15:48:10` ✅   G0 D_BTP_BUND             last=76.39999999999998 @ 2026-06-01
- `15:48:10` ✅   G0 WREPOFOR               last=349641.0 @ 2026-08-12
- `15:48:10` ✅   G0 SRF_TAKEUP             last=0.0 @ 2026-08-14
- `15:48:10` ✅   G0 DTCC-TREASURY-FAILS    last=34843330834.03 @ 2026-08-14
# 1. function + settle

- `15:48:11` ✅ marker settled (attempt 1)
# 2. daily schedule (10:45 UTC, pre-risk-gate)

- `15:48:11` ✅ schedule justhodl-plumbing-composite-daily created -> cron(45 10 * * ? *)
# 3. Event-invoke + poll (<=5 min)

- `15:48:23` ✅ fresh doc in 12s  runtime_ms=488
# 4. truths

- `15:48:23` ✅   status LIVE v1.0.0  composite=-0.295 posture=PLUMBING_CALM
- `15:48:23` ✅   leg fails      live stress_z=+0.38
- `15:48:23` ✅   leg sofr_iorb  live stress_z=+0.29
- `15:48:23` ✅   leg dispersion live stress_z=+0.16
- `15:48:23` ✅   leg scarcity   live stress_z=-0.78
- `15:48:23` ✅   leg haircuts   live stress_z=+0.00
- `15:48:23` ✅   leg fima       live stress_z=-1.92
- `15:48:23` ✅   leg periphery  live stress_z=-1.62 (age 77d)
- `15:48:23` ✅   sftr honestly deferred: deferred_insufficient(bank n=1<26w)
- `15:48:23` ✅   sofr_iorb == independent recompute (+0.292)
- `15:48:23` ✅   scarcity  == independent recompute (-0.779, polarity inverted)
- `15:48:23` ✅   haircut breadth == independent board count (24/58 widening, mode=provisional_thresholds(banked n=0<60))
- `15:48:23` ✅   composite == full independent weighted recompute (-0.295 incl SRF +0.00)
- `15:48:23` ✅   SRF escalator consistent (takeup=0.0 -> +0.00)
- `15:48:23` ✅   bank row appended for 2026-08-17 (n=1)
- `15:48:23` ✅   output size 3 KB
# 5. readout -- what risk-gate will inherit

- `15:48:23`   fima        -1.92  [WREPOFOR]
- `15:48:23`   periphery   -1.62  [D_BTP_BUND]
- `15:48:23`   scarcity    -0.78  [D_BUND_EA_AAA]
- `15:48:23`   fails       +0.38  [DTCC-TREASURY-FAILS]
- `15:48:23`   sofr_iorb   +0.29  [D_SOFR_IORB]
- `15:48:23`   dispersion  +0.16  [D_SOFR_P75_P25]
- `15:48:23`   haircuts    +0.00  [board]
- `15:48:23`   excluded sftr       deferred_insufficient(bank n=1<26w)
- `15:48:23`   risk-gate today: posture=RISK_OFF funding=-2.0 sizing=0.45  <- plumbing_adj wiring is the next op
# 6. verdict

- `15:48:23` ✅ justhodl-plumbing-composite LIVE -- the repo master board now reaches the decision layer as sized context, never selection
