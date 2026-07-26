# ops 3909 — verify the served pages behind the conviction-inversion finding

**Status:** success  
**Duration:** 0.6s  
**Finished:** 2026-07-26T17:46:33+00:00  

## Data

| headline | maturity | served_n_observations |
|---|---|---|
| Conviction labels are inverted — HIGH RISK leads at 70.4% while STRONG OPPORTUNITY trails at 57.4%; the buy labels are not predictive yet. | MATURE | 52354 |

## Log
## 1. the feed itself — is the SERVED signal-backtest.json the fresh post-fix copy

## 2. each page — serves, and contains its render path

- `17:46:32` ✅   scorecard.html: 15,364 bytes, markers={'by_verdict': 2, 'ai_analysis': 1}
- `17:46:32` ✅   proof.html: 14,053 bytes, markers={'by_verdict': 2}
- `17:46:33` ✅   track-public.html: 9,452 bytes, markers={'by_verdict': 1}
- `17:46:33` ✅ PASS_ALL — pages live, render paths present, feed is the fresh post-fix copy
