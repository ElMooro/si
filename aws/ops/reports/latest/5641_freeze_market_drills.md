# ops 5641 -- freeze the anonymized market drills, then sit the holdout exam

**Status:** success  
**Duration:** 390.6s  
**Finished:** 2026-09-17T20:30:53+00:00  

## Log
## 1. Freeze

- `20:29:28` {
- `20:29:28`   "status": "written",
- `20:29:28`   "written": 0,
- `20:29:28`   "exists": 320,
- `20:29:28`   "errors": [
- `20:29:28`     "SPY:session_file_missing:2020-02-18",
- `20:29:28`     "QQQ:session_file_missing:2020-02-18",
- `20:29:28`     "IWM:session_file_missing:2020-02-18",
- `20:29:28`     "TLT:session_file_missing:2020-02-18",
- `20:29:28`     "GLD:session_file_missing:2020-02-18"
- `20:29:28`   ],
- `20:29:28`   "per_block": {
- `20:29:28`     "covid-2020": 0,
- `20:29:28`     "svb-2023": 15,
- `20:29:28`     "yen-carry-2024": 10,
- `20:29:28`     "hikes-2022": 250,
- `20:29:28`     "gilt-ldi-2022": 25,
- `20:29:28`     "tariff-2025": 20
- `20:29:28`   },
- `20:29:28`   "manifest_write": "exists"
- `20:29:28` }
- `20:29:28` {"counts": {"holdout": 25, "train": 295}, "frozen_at": "2026-09-17T20:29:27.529337Z"}
- `20:29:28` ✅ market drills frozen (or already present)
## 2. holdout exam

- `20:29:59` {"split": "holdout", "drills": 25, "endpoint": "jh-owned-coder-async"}
- `20:29:59` {"ok": true, "run_id": "holdout-20260917T202930Z", "model": {"n": 25, "score": 0.48, "direction_acc": 0.52, "regime_acc": 0.2, "crisis_acc": 0.8, "crisis_brier": 0.18, "missed_crises": 5}, "baselines": {"momentum": {"n": 25, "score": 0.464, "direction_acc": 0.44, "regime_acc": 0.28, "crisis_acc": 0.8, "crisis_brier": 0.16, "missed_crises": 5}, "prior": {"n": 25, "score": 0.552, "direction_acc": 0.52, "regime_acc": 0.44, "crisis_acc": 0.8, "crisis_brier": 0.16, "missed_crises": 5}}, "unanswered": 0}
- `20:29:59` ✅ holdout exam written to factory/exams/market/
## 2. train exam

- `20:30:53` {"split": "train", "drills": 60, "endpoint": "jh-owned-coder-async"}
- `20:30:53` {"ok": true, "run_id": "train-20260917T203004Z", "model": {"n": 60, "score": 0.525, "direction_acc": 0.5, "regime_acc": 0.35, "crisis_acc": 0.85, "crisis_brier": 0.122, "missed_crises": 9}, "baselines": {"momentum": {"n": 60, "score": 0.5333, "direction_acc": 0.5167, "regime_acc": 0.35, "crisis_acc": 0.85, "crisis_brier": 0.1275, "missed_crises": 9}, "prior": {"n": 60, "score": 0.5383, "direction_acc": 0.5167, "regime_acc": 0.3667, "crisis_acc": 0.85, "crisis_brier": 0.1275, "missed_crises": 9}}, "unanswered": 0}
- `20:30:53` ✅ train exam written to factory/exams/market/
## 3. Page projection

- `20:30:53` ✅ inventory tick queued: data/ai.json.market_exam refreshes within a minute
