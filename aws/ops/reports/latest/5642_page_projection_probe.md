# ops 5642 -- data/ai.json as the page reads it

**Status:** success  
**Duration:** 0.3s  
**Finished:** 2026-09-18T15:15:43+00:00  

## Data

| calls_ledgered_while_advisory | calls_made | calls_this_read | decision_status | generated_at | n_blockers | read_path | stances | voice |
|---|---|---|---|---|---|---|---|---|
| True | 8 | 4 | ADVISORY_ONLY | 2026-09-18T15:10:16.147263+00:00 | 4 | owned | {"stocks": "SELECTIVE", "bonds": "NEUTRAL", "metals": "HOLD", "crypto": "AVOID"} | online (owned model owned:qwen2-5-coder-7b-instruct@c03e6d358207, in this account, with your notes) |

## Log
- `15:15:43` holdout: run=holdout-20260917T202930Z n=25 model={"n": 25, "score": 0.48, "direction_acc": 0.52, "regime_acc": 0.2, "crisis_acc": 0.8, "crisis_brier": 0.18, "missed_crises": 5} baselines={"momentum": {"n": 25, "score": 0.464, "direction_acc": 0.44, "regime_acc": 0.28, "crisis_acc": 0.8, "crisis_brier": 0.16, "missed_crises": 5}, "prior": {"n": 25, "score": 0.552, "direction_acc": 0.52, "regime_acc": 0.44, "crisis_acc": 0.8, "crisis_brier": 0.16, "missed_crises": 5}}
- `15:15:43` train: run=train-20260917T203004Z n=60 model={"n": 60, "score": 0.525, "direction_acc": 0.5, "regime_acc": 0.35, "crisis_acc": 0.85, "crisis_brier": 0.122, "missed_crises": 9} baselines={"momentum": {"n": 60, "score": 0.5333, "direction_acc": 0.5167, "regime_acc": 0.35, "crisis_acc": 0.85, "crisis_brier": 0.1275, "missed_crises": 9}, "prior": {"n": 60, "score": 0.5383, "direction_acc": 0.5167, "regime_acc": 0.3667, "crisis_acc": 0.85, "crisis_brier": 0.1275, "missed_crises": 9}}
- `15:15:43` ✅ market_exam present on the page projection
