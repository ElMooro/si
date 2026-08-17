# G0. ledger continuity precondition

**Status:** success  
**Duration:** 19.2s  
**Finished:** 2026-08-17T21:44:53+00:00  

## Log
- `21:44:33` ✅ ledger n_before=62
# 1. settle + schedule

- `21:44:44` ✅ marker settled (attempt 2)
- `21:44:44` ✅ schedule daily 09:50 UTC
# 2. invoke + poll

- `21:44:53` ✅ fresh in 8s
# 3. truths

- `21:44:53` ✅   taiwan LIVE; ledger 62 -> 62 (UNION, nothing lost)
- `21:44:53` ✅   sums == independent full-ledger recompute (latest +45.45, 5d +199.84, 60d -821.28)
- `21:44:53` ✅   korea deferral named
# 4. verdict

- `21:44:53` ✅ hot-money engine LIVE -- the fast layer has its own house; the ledger survived the move intact
