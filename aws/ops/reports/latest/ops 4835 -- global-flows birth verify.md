# G0. BCRP re-confirm at birth

**Status:** success  
**Duration:** 14.4s  
**Finished:** 2026-08-17T17:24:51+00:00  

## Data

| state |
|---|
| Active |

## Log
- `17:24:38` ✅ BCRP 4 series, 57 quarters, last T1.26
# 1. function + settle + schedule

- `17:24:39` ✅ marker settled (attempt 1)
- `17:24:39` ✅ schedule created -> cron(0 12 ? * MON *)
# 2. Event-invoke + poll (<=4 min)

- `17:24:50` ✅ fresh in 10s
# 3. truths

- `17:24:50` ✅   LIVE; peru T1.26
- `17:24:50` ✅   portfolio_total == independent BCRP refetch (+258.1M @ T1.26)
- `17:24:50` ✅   gov_bonds_nonresident == independent BCRP refetch (+712.6M @ T1.26)
- `17:24:50` ✅   bank PN39285BQ n=57 (Deny-Delete zone)
- `17:24:50` ✅   bank PN39286BQ n=57 (Deny-Delete zone)
- `17:24:51` ✅   bank PN39287BQ n=57 (Deny-Delete zone)
- `17:24:51` ✅   bank PN39414FQ n=57 (Deny-Delete zone)
- `17:24:51` ✅   five deferrals named with unlock reasons
- `17:24:51` ✅   composites honestly null
# 4. readout

- `17:24:51`   portfolio_total              +258.1M  4Q   +1149.3  z=-0.08  since T1.12
- `17:24:51`   portfolio_equity              +57.6M  4Q    +147.0  z=0.87  since T1.12
- `17:24:51`   portfolio_fixed_income       +200.5M  4Q   +1002.3  z=-0.21  since T1.12
- `17:24:51`   gov_bonds_nonresident        +712.6M  4Q   +3394.7  z=0.34  since T1.12
# 5. verdict

- `17:24:51` ✅ justhodl-global-flows LIVE -- Peru measured, the world map honest about what unlocks next
