# ops 5562 -- learning scorecard: supply (rows/receipts) vs learning (exam delta), from objects only

**Status:** success  
**Duration:** 141.6s  
**Finished:** 2026-09-14T22:31:11+00:00  

## Data

| eligible_rows | floor | learning_pct | stages_done | supply_readiness_pct |
|---|---|---|---|---|
| 570 | 1500 | 0.0 | 4/7 | 38.0 |

## Log
- `22:31:09` ✅ verified rows on disk: 2102 (listing truncated=False); by checker: {"factory-code-exam:a340d8ebd559": 464, "factory-trace-verify:network-less-container": 1529, "factory-code-exam:caf2b53d3151": 106, "factory-code-verify:v3-supervisor-judge": 3}
- `22:31:09` ✅ ELIGIBLE through the curator (receipt resolved, hashes bound, supervisor judge, cases>=1): 570 rows over 570 distinct tasks; families {"mbpp": 464, "apps-introductory": 106}
- `22:31:10` ✅ bursts launched: 6; failed-attempt records on disk: 1454
- `22:31:10` ✅ Gear B training jobs (deduplicated): 0; champion: none (base model)
- `22:31:11`   exam result base.json: gen=gen-0 passed=0/164 score=0.0 critical=21 missing=0 run=34900737918
- `22:31:11`   exam result gen-0-34900737918.json: gen=gen-0 passed=0/164 score=0.0 critical=21 missing=0 run=34900737918
- `22:31:11`   exam result gen-0-34902101944.json: gen=gen-0 passed=0/164 score=0.0 critical=21 missing=0 run=34902101944
- `22:31:11`   exam result gen-0-34903812485.json: gen=gen-0 passed=0/164 score=0.0 critical=157 missing=0 run=34903812485
- `22:31:11` ✅ BASE EXAM (frozen HumanEval, greedy, base model): 0/164 passed = 0.0%; critical failures 21; partial-judge passes 0; evaluation_id humaneval-frozen-848505e1dd021797
- `22:31:11` ✅ frozen holdout exam: prompts present; exam result objects: 4; base exam: {"pass_rate": 0.0, "tasks": null, "at": "2026-09-14T21:48:21Z"}; promoted candidate exam: none
- `22:31:11` ✅ owned serving endpoint: absent; chat control enabled=True
- `22:31:11` ✅ pipeline stages: weights staged=yes, bursts produced candidates=yes, independent judge admitted rows=yes, supply floor reached=no, base exam measured=yes, adapter trained=no, candidate examined and promoted=no -> 4/7
- `22:31:11` ✅ SUPPLY READINESS 38.0% (570/1500 eligible rows) | LEARNING 0.0% (exam delta; 0 until an adapter is trained and examined)
- `22:31:11` ✅ GREEN -- scorecard recorded
