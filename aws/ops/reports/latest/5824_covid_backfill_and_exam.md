# ops 5824 -- COVID-2020 drills: backfill, freeze, exam

**Status:** failure  
**Duration:** 1042.6s  
**Finished:** 2026-09-18T23:42:58+00:00  

## Error

```
SystemExit: 1
```

## Log
## 1. Backfill (FMP -> etf-history prefix)

- `23:25:40` {"symbols": {"SPY": 103, "QQQ": 103, "IWM": 103, "TLT": 103, "GLD": 103}, "sessions_with_every_symbol": 103, "first": ["2020-01-02"], "last": ["2020-05-29"]}
- `23:25:40` {"written": 103, "existed": 0, "prefix": "data/warm/etf-history/grouped/"}
## 2. Freeze the new drills

- `23:28:45`   "written": 30,
- `23:28:45`   "exists": 320,
- `23:28:45`   "errors": [],
- `23:28:45`   "per_block": {
- `23:28:45`     "covid-2020": 30,
- `23:28:45`     "svb-2023": 15,
- `23:28:45`     "yen-carry-2024": 10,
- `23:28:45`     "hikes-2022": 250,
- `23:28:45`     "gilt-ldi-2022": 25,
- `23:28:45`     "tariff-2025": 20
- `23:28:45`   },
- `23:28:45`   "manifest_write": "exists"
- `23:28:45` }
- `23:28:45` {"counts": {"holdout": 55, "train": 295}, "frozen_at": "2026-09-18T23:28:45.234259Z"}
## 3. Holdout exam on the enlarged set

- `23:42:58` {"split": "holdout", "drills": 55, "endpoint": "jh-owned-coder-async"}
- `23:42:58` {"ok": true, "run_id": "holdout-20260918T232847Z", "model": {"n": 0}, "baselines": {"momentum": {"n": 55, "score": 0.4873, "direction_acc": 0.5091, "regime_acc": 0.3636, "crisis_acc": 0.6182, "crisis_brier": 0.236, "missed_crises": 21}, "prior": {"n": 55, "score": 0.5127, "direction_acc": 0.5273, "regime_acc": 0.4182, "crisis_acc": 0.6182, "crisis_brier": 0.236, "missed_crises": 21}}, "unanswered": 55}
- `23:42:58` ✗ scripts/factory_market_exam.py exited 1: 
- `23:42:58` ✗ inventory tick queued; exam rc=1
