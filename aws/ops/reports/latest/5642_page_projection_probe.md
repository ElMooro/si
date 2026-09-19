# ops 5642 -- data/ai.json as the page reads it

**Status:** success  
**Duration:** 0.2s  
**Finished:** 2026-09-19T14:41:11+00:00  

## Data

| calls_ledgered_while_advisory | calls_made | calls_this_read | decision_status | generated_at | n_blockers | read_path | stances | voice |
|---|---|---|---|---|---|---|---|---|
| True | 9 | 0 | ADVISORY_ONLY | 2026-09-19T14:40:14.482048+00:00 | 4 | owned | {"stocks": "SELECTIVE", "bonds": "NEUTRAL", "metals": "HOLD", "crypto": "AVOID"} | online (owned model owned:qwen2-5-coder-7b-instruct@c03e6d358207, in this account, with your notes) |

## Log
- `14:41:11` holdout: run=holdout-20260918T234556Z n=55 model={"n": 55, "score": 0.4982, "direction_acc": 0.5636, "regime_acc": 0.3091, "crisis_acc": 0.6182, "crisis_brier": 0.3105, "missed_crises": 21} baselines={"momentum": {"n": 55, "score": 0.4873, "direction_acc": 0.5091, "regime_acc": 0.3636, "crisis_acc": 0.6182, "crisis_brier": 0.236, "missed_crises": 21}, "prior": {"n": 55, "score": 0.5127, "direction_acc": 0.5273, "regime_acc": 0.4182, "crisis_acc": 0.6182, "crisis_brier": 0.236, "missed_crises": 21}}
- `14:41:11` train: run=train-20260917T203004Z n=60 model={"n": 60, "score": 0.525, "direction_acc": 0.5, "regime_acc": 0.35, "crisis_acc": 0.85, "crisis_brier": 0.122, "missed_crises": 9} baselines={"momentum": {"n": 60, "score": 0.5333, "direction_acc": 0.5167, "regime_acc": 0.35, "crisis_acc": 0.85, "crisis_brier": 0.1275, "missed_crises": 9}, "prior": {"n": 60, "score": 0.5383, "direction_acc": 0.5167, "regime_acc": 0.3667, "crisis_acc": 0.85, "crisis_brier": 0.1275, "missed_crises": 9}}
- `14:41:11` reasoning: run=reasoning-20260919T143733Z families={"gsm8k": {"n": 40, "answered": 40, "correct": 34, "accuracy": 0.85}, "code_reading": {"n": 20, "answered": 20, "correct": 4, "accuracy": 0.2}}
- `14:41:11` coding: base=0.8232 learning_pts=0.0 verdict=No learning yet: 8 candidates scored 81.1%, 81.7%, 82.3%, 82.3% against the base 82.3% -- each trained on the same 570 tasks, and the same data cannot move a 7B
- `14:41:11` ✅ market_exam present on the page projection
