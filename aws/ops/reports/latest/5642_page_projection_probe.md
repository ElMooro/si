# ops 5642 -- data/ai.json as the page reads it

**Status:** success  
**Duration:** 0.1s  
**Finished:** 2026-09-18T23:48:26+00:00  

## Data

| calls_ledgered_while_advisory | calls_made | calls_this_read | decision_status | generated_at | n_blockers | read_path | stances | voice |
|---|---|---|---|---|---|---|---|---|
| True | 9 | 2 | ADVISORY_ONLY | 2026-09-18T23:46:42.884149+00:00 | 4 | owned | {"stocks": "SELECTIVE", "bonds": "NEUTRAL", "metals": "ACCUMULATE", "crypto": "AVOID"} | online (owned model owned:qwen2-5-coder-7b-instruct@c03e6d358207, in this account, with your notes) |

## Log
- `23:48:26` holdout: run=holdout-20260918T234556Z n=55 model={"n": 55, "score": 0.4982, "direction_acc": 0.5636, "regime_acc": 0.3091, "crisis_acc": 0.6182, "crisis_brier": 0.3105, "missed_crises": 21} baselines={"momentum": {"n": 55, "score": 0.4873, "direction_acc": 0.5091, "regime_acc": 0.3636, "crisis_acc": 0.6182, "crisis_brier": 0.236, "missed_crises": 21}, "prior": {"n": 55, "score": 0.5127, "direction_acc": 0.5273, "regime_acc": 0.4182, "crisis_acc": 0.6182, "crisis_brier": 0.236, "missed_crises": 21}}
- `23:48:26` train: run=train-20260917T203004Z n=60 model={"n": 60, "score": 0.525, "direction_acc": 0.5, "regime_acc": 0.35, "crisis_acc": 0.85, "crisis_brier": 0.122, "missed_crises": 9} baselines={"momentum": {"n": 60, "score": 0.5333, "direction_acc": 0.5167, "regime_acc": 0.35, "crisis_acc": 0.85, "crisis_brier": 0.1275, "missed_crises": 9}, "prior": {"n": 60, "score": 0.5383, "direction_acc": 0.5167, "regime_acc": 0.3667, "crisis_acc": 0.85, "crisis_brier": 0.1275, "missed_crises": 9}}
- `23:48:26` ✅ market_exam present on the page projection
