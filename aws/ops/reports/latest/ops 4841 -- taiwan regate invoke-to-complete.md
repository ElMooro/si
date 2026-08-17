# G0. CBC + TWSE live contracts

**Status:** success  
**Duration:** 253.9s  
**Finished:** 2026-08-17T18:04:49+00:00  

## Log
- `18:00:38` ✅ CBC: labels @161/164/182, width 403==402+1, last 2026Q1
- `18:00:39` ✅ TWSE: stat OK date=20260817 foreign rows=2 net=45.45bn NT$
# 1. settle + schedule swap

- `18:00:40` ✅ marker settled (attempt 1)
- `18:00:40` ✅ daily schedule exists
- `18:00:40` weekly schedule already gone
# 2. invoke-to-complete backfill rounds

- `18:02:55` ✅ round 1: ledger=54 attempts=45 new=19
- `18:04:46` ✅ round 2: ledger=62 attempts=36 new=8
# 3. truths

- `18:04:46` ✅   taiwan LIVE; macro 2026Q1
- `18:04:47` ✅   macro total == in-op CBC refetch (-25340.0M @ 2026Q1)
- `18:04:47` ✅   hot_money ledger=62 days; 5/20/60d present
- `18:04:49` ✅   sampled 20260814 ledger == refetch (45.35bn)
- `18:04:49` ✅   peru untouched LIVE (T1.26)
- `18:04:49` ✅   deferred == korea/chile/imf_layer
- `18:04:49` ✅   bank portfolio_liab_total.json n=162
- `18:04:49` ✅   bank bfi82u-foreign.json n=62
# 4. readout

- `18:04:49`   TW portfolio_liab_total    -25340.0M  4Q   -21071.0  z=-4.0
- `18:04:49`   TW portfolio_liab_equity   -25661.0M  4Q   -23217.0  z=-4.0
- `18:04:49`   TW portfolio_liab_debt       +321.0M  4Q    +2146.0  z=0.26
- `18:04:49`   TW hot money: 45.45 @ 20260817 | 5d 199.84 | 20d 154.84 | 60d -821.28 NT$bn z=0.96
# 5. verdict

- `18:04:49` ✅ Taiwan LIVE -- semiconductor specialist wired: quarterly CBC macro + daily TWSE hot money with backfilled ledger
