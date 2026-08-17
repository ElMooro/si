# G0. CBC + TWSE live contracts

**Status:** failure  
**Duration:** 94.6s  
**Finished:** 2026-08-17T17:46:52+00:00  

## Error

```
SystemExit: 1
```

## Log
- `17:45:20` ✅ CBC: labels @161/164/182, width 403==402+1, last 2026Q1
- `17:45:21` ✅ TWSE: stat OK date=20260817 foreign rows=2 net=45.45bn NT$
# 1. settle + schedule swap

- `17:45:22` ✅ marker settled (attempt 1)
- `17:45:22` ✅ daily schedule created (10:00 UTC)
- `17:45:23` ✅ weekly schedule retired
# 2. invoke backfill(90) + poll (<=6 min)

- `17:46:49` ✅ fresh in 86s
# 3. truths

- `17:46:49` ✅   taiwan LIVE; macro 2026Q1
- `17:46:51` ✅   macro total == in-op CBC refetch (-25340.0M @ 2026Q1)
- `17:46:51` ✗   hot_money thin: n=35 sums_ok=False
- `17:46:52` ✅   sampled 20260814 ledger == refetch (45.35bn)
- `17:46:52` ✅   peru untouched LIVE (T1.26)
- `17:46:52` ✅   deferred == korea/chile/imf_layer
- `17:46:52` ✅   bank portfolio_liab_total.json n=162
- `17:46:52` ✅   bank bfi82u-foreign.json n=35
# 4. readout

- `17:46:52`   TW portfolio_liab_total    -25340.0M  4Q   -21071.0  z=-4.0
- `17:46:52`   TW portfolio_liab_equity   -25661.0M  4Q   -23217.0  z=-4.0
- `17:46:52`   TW portfolio_liab_debt       +321.0M  4Q    +2146.0  z=0.26
- `17:46:52`   TW hot money: 45.45 @ 20260817 | 5d 199.84 | 20d 154.84 | 60d None NT$bn z=1.12
# 5. verdict

- `17:46:52` ✗ HARD FAILS: ['hm']
