# ops 3580 — historical backfill (Polygon 90d window)

**Status:** success  
**Duration:** 59.6s  
**Finished:** 2026-07-20T18:00:51+00:00  

## Error

```
SystemExit: 0
```

## Log
- `18:00:04` PASS  G1_settled_3111 — markers ok=True mem=1024 timeout=900
- `18:00:35` FAIL  G2_hist_backfill — hist90: prs=16798 deals=28 ledger=104 filled=45 · ledger=104 · base_rates: contract_win: n5=24 med5=-5.71% n21=7 med21=5.15% hit21=57.1% | govt_contract: n5=3 med5=-5.3% n21=1 med21=1.83% hit21=100.0% | partnership: n5=1 med5=-7.74% n21=0 med21=None% hit21=None% | licensing_supply: n5=8 med5=-0.26% n21=8 med21=-6.82% hit21=37.5% | equity_investment: n5=1 med5=-5.0% n21=0 med21=None% hit21=None% | other: n5=7 med5=0.17% n21=0 med21=None% hit21=None%
- `18:00:51` PASS  G3_feed_base_rates — feed base_rate types=['contract_win', 'govt_contract', 'partnership', 'licensing_supply', 'equity_investment', 'other'] history.n=104
- `18:00:51` VERDICT: GAPS: G2_hist_backfill
