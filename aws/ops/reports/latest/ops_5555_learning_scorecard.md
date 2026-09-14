# ops 5555 -- learning scorecard: supply (rows/receipts) vs learning (exam delta), from objects only

**Status:** success  
**Duration:** 96.3s  
**Finished:** 2026-09-14T17:22:36+00:00  

## Data

| eligible_rows | floor | learning_pct | stages_done | supply_readiness_pct |
|---|---|---|---|---|
| 570 | 1500 | 0.0 | 3/7 | 38.0 |

## Log
- `17:22:35` ✅ verified rows on disk: 2102 (listing truncated=False); by checker: {"factory-code-exam:a340d8ebd559": 464, "factory-trace-verify:network-less-container": 1529, "factory-code-exam:caf2b53d3151": 106, "factory-code-verify:v3-supervisor-judge": 3}
- `17:22:35` ✅ ELIGIBLE through the curator (receipt resolved, hashes bound, supervisor judge, cases>=1): 570 rows over 570 distinct tasks; families {"mbpp": 464, "apps-introductory": 106}
- `17:22:35` ✅ bursts launched: 3; failed-attempt records on disk: 1454
- `17:22:36` ✅ Gear B training jobs (deduplicated): 0; champion: none (base model)
- `17:22:36` ✅ frozen holdout exam: prompts MISSING; exam result objects: 0; base exam: not run yet; promoted candidate exam: none
- `17:22:36` ✅ owned serving endpoint: absent; chat control enabled=None
- `17:22:36` ✅ pipeline stages: weights staged=yes, bursts produced candidates=yes, independent judge admitted rows=yes, supply floor reached=no, base exam measured=no, adapter trained=no, candidate examined and promoted=no -> 3/7
- `17:22:36` ✅ SUPPLY READINESS 38.0% (570/1500 eligible rows) | LEARNING 0.0% (exam delta; 0 until an adapter is trained and examined)
- `17:22:36` ✅ GREEN -- scorecard recorded
