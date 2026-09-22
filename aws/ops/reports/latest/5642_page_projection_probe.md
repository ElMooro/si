# ops 5642 -- data/ai.json as the page reads it

**Status:** success  
**Duration:** 0.3s  
**Finished:** 2026-09-22T02:34:59+00:00  

## Data

| calls_ledgered_while_advisory | calls_made | calls_this_read | decision_status | generated_at | n_blockers | read_path | stances | voice |
|---|---|---|---|---|---|---|---|---|
| True | 13 | 2 | ADVISORY_ONLY | 2026-09-22T02:30:15.984277+00:00 | 4 | owned | {"stocks": "SELECTIVE", "bonds": "NEUTRAL", "metals": "ACCUMULATE", "crypto": "REDUCE"} | online (owned model owned:qwen2-5-coder-7b-instruct@c03e6d358207, in this account, with your notes) |

## Log
- `02:34:59` holdout: run=holdout-20260920T132643Z n=55 model={"n": 55, "score": 0.4836, "direction_acc": 0.5455, "regime_acc": 0.2909, "crisis_acc": 0.6182, "crisis_brier": 0.3109, "missed_crises": 21} baselines={"momentum": {"n": 55, "score": 0.4873, "direction_acc": 0.5091, "regime_acc": 0.3636, "crisis_acc": 0.6182, "crisis_brier": 0.236, "missed_crises": 21}, "prior": {"n": 55, "score": 0.5127, "direction_acc": 0.5273, "regime_acc": 0.4182, "crisis_acc": 0.6182, "crisis_brier": 0.236, "missed_crises": 21}}
- `02:34:59` train: run=train-20260917T203004Z n=60 model={"n": 60, "score": 0.525, "direction_acc": 0.5, "regime_acc": 0.35, "crisis_acc": 0.85, "crisis_brier": 0.122, "missed_crises": 9} baselines={"momentum": {"n": 60, "score": 0.5333, "direction_acc": 0.5167, "regime_acc": 0.35, "crisis_acc": 0.85, "crisis_brier": 0.1275, "missed_crises": 9}, "prior": {"n": 60, "score": 0.5383, "direction_acc": 0.5167, "regime_acc": 0.3667, "crisis_acc": 0.85, "crisis_brier": 0.1275, "missed_crises": 9}}
- `02:34:59` reasoning: run=reasoning-20260920T135243Z families={"gsm8k": {"n": 40, "answered": 40, "correct": 33, "accuracy": 0.825}, "code_reading": {"n": 20, "answered": 20, "correct": 4, "accuracy": 0.2}}
- `02:34:59` coding: base=0.8232 learning_pts=0.0 verdict=No learning yet: 9 candidates scored 81.7%, 82.3%, 82.3%, 80.5% against the base 82.3% -- each trained on the same 570 tasks, and the same data cannot move a 7B
- `02:34:59` gear_b: {"status": "armed", "dataset": {"generation": 9, "kept": 570, "floor": 500, "missing_rows": null, "licenses": {"CC-BY-4.0": 464, "MIT": 106}, "kinds": {"public_benchmark_train": 570}, "families": 2, "ok": true, "reason": null, "at": "2026-09-17T01:13:57.010163Z"}, "champion": {"generation": 0, "note": "base model; no weights promoted"}}
- `02:34:59` coding candidates: [{"generation": "gen-13", "score": 0.8171, "passed": 134, "n": 164, "at": "2026-09-18T06:36:45Z"}, {"generation": "gen-14", "score": 0.811, "passed": 133, "n": 164, "at": "2026-09-18T12:31:52Z"}, {"generation": "gen-17", "score": 0.8171, "passed": 134, "n": 164, "at": "2026-09-19T07:43:07Z"}, {"generation": "gen-18", "score": 0.8232, "passed": 135, "n": 164, "at": "2026-09-19T09:21:57Z"}, {"generation": "gen-19", "score": 0.8232, "passed": 135, "n": 164, "at": "2026-09-19T12:29:31Z"}, {"generati
- `02:34:59` ✅ market_exam present on the page projection
