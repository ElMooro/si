# ops 5559 -- learning scorecard: supply (rows/receipts) vs learning (exam delta), from objects only

**Status:** success  
**Duration:** 220.2s  
**Finished:** 2026-09-14T21:57:36+00:00  

## Data

| eligible_rows | floor | learning_pct | stages_done | supply_readiness_pct |
|---|---|---|---|---|
| 570 | 1500 | 0.0 | 4/7 | 38.0 |

## Log
- `21:57:34` ✅ verified rows on disk: 2102 (listing truncated=False); by checker: {"factory-code-exam:a340d8ebd559": 464, "factory-trace-verify:network-less-container": 1529, "factory-code-exam:caf2b53d3151": 106, "factory-code-verify:v3-supervisor-judge": 3}
- `21:57:34` ✅ ELIGIBLE through the curator (receipt resolved, hashes bound, supervisor judge, cases>=1): 570 rows over 570 distinct tasks; families {"mbpp": 464, "apps-introductory": 106}
- `21:57:35` ✅ bursts launched: 6; failed-attempt records on disk: 1454
- `21:57:35` ✅ Gear B training jobs (deduplicated): 0; champion: none (base model)
- `21:57:36` ✅ BASE EXAM (frozen HumanEval, greedy, base model): 0/164 passed = 0.0%; critical failures 21; partial-judge passes 0; evaluation_id humaneval-frozen-848505e1dd021797
- `21:57:36` ✅ frozen holdout exam: prompts present; exam result objects: 2; base exam: {"pass_rate": 0.0, "tasks": null, "at": "2026-09-14T21:48:21Z"}; promoted candidate exam: none
- `21:57:36` ✅ owned serving endpoint: absent; chat control enabled=True
- `21:57:36` ✅ pipeline stages: weights staged=yes, bursts produced candidates=yes, independent judge admitted rows=yes, supply floor reached=no, base exam measured=yes, adapter trained=no, candidate examined and promoted=no -> 4/7
- `21:57:36` ✅ SUPPLY READINESS 38.0% (570/1500 eligible rows) | LEARNING 0.0% (exam delta; 0 until an adapter is trained and examined)
- `21:57:36` ✅ GREEN -- scorecard recorded
