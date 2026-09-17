# ops 5641 -- freeze the anonymized market drills, then sit the holdout exam

**Status:** success  
**Duration:** 1036.0s  
**Finished:** 2026-09-17T20:17:14+00:00  

## Log
## 1. Freeze

- `20:04:49` {
- `20:04:49`   "status": "written",
- `20:04:49`   "written": 0,
- `20:04:49`   "exists": 320,
- `20:04:49`   "errors": [
- `20:04:49`     "SPY:session_file_missing:2020-02-18",
- `20:04:49`     "QQQ:session_file_missing:2020-02-18",
- `20:04:49`     "IWM:session_file_missing:2020-02-18",
- `20:04:49`     "TLT:session_file_missing:2020-02-18",
- `20:04:49`     "GLD:session_file_missing:2020-02-18"
- `20:04:49`   ],
- `20:04:49`   "per_block": {
- `20:04:49`     "covid-2020": 0,
- `20:04:49`     "svb-2023": 15,
- `20:04:49`     "yen-carry-2024": 10,
- `20:04:49`     "hikes-2022": 250,
- `20:04:49`     "gilt-ldi-2022": 25,
- `20:04:49`     "tariff-2025": 20
- `20:04:49`   },
- `20:04:49`   "manifest_write": "exists"
- `20:04:49` }
- `20:04:49` {"counts": {"holdout": 25, "train": 295}, "frozen_at": "2026-09-17T20:04:48.329405Z"}
- `20:04:49` ✅ market drills frozen (or already present)
## 2. holdout exam

- `20:16:07` {"split": "holdout", "drills": 25, "endpoint": "jh-owned-coder-async"}
- `20:16:07` {"ok": true, "run_id": "holdout-20260917T200451Z", "model": {"n": 25, "score": 0.48, "direction_acc": 0.52, "regime_acc": 0.2, "crisis_acc": 0.8, "crisis_brier": 0.1672, "missed_crises": 5}, "baselines": {"momentum": {"n": 25, "score": 0.464, "direction_acc": 0.44, "regime_acc": 0.28, "crisis_acc": 0.8, "crisis_brier": 0.16, "missed_crises": 0}, "prior": {"n": 25, "score": 0.552, "direction_acc": 0.52, "regime_acc": 0.44, "crisis_acc": 0.8, "crisis_brier": 0.16, "missed_crises": 0}}, "unanswered": 0}
- `20:16:07` ✅ holdout exam written to factory/exams/market/
## 2. train exam

- `20:17:13` {"split": "train", "drills": 60, "endpoint": "jh-owned-coder-async"}
- `20:17:13` {"ok": true, "run_id": "train-20260917T201612Z", "model": {"n": 60, "score": 0.53, "direction_acc": 0.5, "regime_acc": 0.3667, "crisis_acc": 0.85, "crisis_brier": 0.1225, "missed_crises": 9}, "baselines": {"momentum": {"n": 60, "score": 0.5333, "direction_acc": 0.5167, "regime_acc": 0.35, "crisis_acc": 0.85, "crisis_brier": 0.1275, "missed_crises": 0}, "prior": {"n": 60, "score": 0.5383, "direction_acc": 0.5167, "regime_acc": 0.3667, "crisis_acc": 0.85, "crisis_brier": 0.1275, "missed_crises": 0}}, "unanswered": 0}
- `20:17:13` ✅ train exam written to factory/exams/market/
## 3. Page projection

- `20:17:14` ✅ inventory tick queued: data/ai.json.market_exam refreshes within a minute
