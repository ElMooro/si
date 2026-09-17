# ops 5641 -- freeze the anonymized market drills, then sit the holdout exam

**Status:** failure  
**Duration:** 161.1s  
**Finished:** 2026-09-17T19:58:40+00:00  

## Log
## 1. Freeze

- `19:58:39` {
- `19:58:39`   "status": "written",
- `19:58:39`   "written": 315,
- `19:58:39`   "exists": 5,
- `19:58:39`   "errors": [
- `19:58:39`     "SPY:session_file_missing:2020-02-18",
- `19:58:39`     "QQQ:session_file_missing:2020-02-18",
- `19:58:39`     "IWM:session_file_missing:2020-02-18",
- `19:58:39`     "TLT:session_file_missing:2020-02-18",
- `19:58:39`     "GLD:session_file_missing:2020-02-18"
- `19:58:39`   ],
- `19:58:39`   "per_block": {
- `19:58:39`     "covid-2020": 0,
- `19:58:39`     "svb-2023": 15,
- `19:58:39`     "yen-carry-2024": 10,
- `19:58:39`     "hikes-2022": 250,
- `19:58:39`     "gilt-ldi-2022": 25,
- `19:58:39`     "tariff-2025": 20
- `19:58:39`   },
- `19:58:39`   "manifest_write": "exists"
- `19:58:39` }
- `19:58:39` {"counts": {"holdout": 25, "train": 295}, "frozen_at": "2026-09-17T19:58:39.172540Z"}
- `19:58:39` ✅ market drills frozen (or already present)
## 2. holdout exam

- `19:58:39` {"split": "holdout", "drills": 0, "endpoint": "jh-owned-coder-async"}
- `19:58:39` {"ok": false, "error": "no drills under factory/holdout/drills/holdout/ -- run scripts/factory_holdout.py freeze first"}
- `19:58:39` ✗ holdout exam exited 2: 
## 2. train exam

- `19:58:40` {"split": "train", "drills": 0, "endpoint": "jh-owned-coder-async"}
- `19:58:40` {"ok": false, "error": "no drills under factory/holdout/drills/train/ -- run scripts/factory_holdout.py freeze first"}
- `19:58:40` ✗ train exam exited 2: 
## 3. Page projection

- `19:58:40` ✅ inventory tick queued: data/ai.json.market_exam refreshes within a minute
