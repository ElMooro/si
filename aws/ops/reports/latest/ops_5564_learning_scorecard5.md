# ops 5564 -- learning scorecard: supply (rows/receipts) vs learning (exam delta), from objects only

**Status:** success  
**Duration:** 339.7s  
**Finished:** 2026-09-14T23:20:05+00:00  

## Data

| eligible_rows | floor | learning_pct | stages_done | supply_readiness_pct |
|---|---|---|---|---|
| 2097 | 1500 | 0.0 | 5/7 | 100.0 |

## Log
- `23:20:03` ✅ verified rows on disk: 3629 (listing truncated=False); by checker: {"factory-code-verify:v4-supervisor-judge": 1527, "factory-code-exam:a340d8ebd559": 464, "factory-trace-verify:network-less-container": 1529, "factory-code-exam:caf2b53d3151": 106, "factory-code-verify:v3-supervisor-judge": 3}
- `23:20:03` ✅ ELIGIBLE through the curator (receipt resolved, hashes bound, supervisor judge, cases>=1): 2097 rows over 570 distinct tasks; families {"mbpp": 1777, "apps-introductory": 320}
- `23:20:04` ✅ bursts launched: 6; failed-attempt records on disk: 2795
- `23:20:04` ✅ Gear B training jobs (deduplicated): 0; champion: none (base model)
- `23:20:04`   exam result base.json: gen=gen-0 passed=135/164 score=0.8232 critical=0 missing=0 run=34905651137
- `23:20:05`   exam result gen-0-34900737918.json: gen=gen-0 passed=0/164 score=0.0 critical=21 missing=0 run=34900737918
- `23:20:05`   exam result gen-0-34902101944.json: gen=gen-0 passed=0/164 score=0.0 critical=21 missing=0 run=34902101944
- `23:20:05`   exam result gen-0-34903812485.json: gen=gen-0 passed=0/164 score=0.0 critical=157 missing=0 run=34903812485
- `23:20:05`   exam result gen-0-34905498719.json: gen=gen-0 passed=0/164 score=0.0 critical=157 missing=0 run=34905498719
- `23:20:05`   exam result gen-0-34905651137.json: gen=gen-0 passed=135/164 score=0.8232 critical=0 missing=0 run=34905651137
- `23:20:05` ✅ BASE EXAM (frozen HumanEval, greedy, base model): 135/164 passed = 82.3%; critical failures 0; partial-judge passes 11; evaluation_id humaneval-frozen-848505e1dd021797
- `23:20:05` ✅ frozen holdout exam: prompts present; exam result objects: 6; base exam: {"pass_rate": 0.8232, "tasks": null, "at": "2026-09-14T22:45:56Z"}; promoted candidate exam: none
- `23:20:05` ✅ owned serving endpoint: InService; chat control enabled=True
- `23:20:05` ✅ pipeline stages: weights staged=yes, bursts produced candidates=yes, independent judge admitted rows=yes, supply floor reached=yes, base exam measured=yes, adapter trained=no, candidate examined and promoted=no -> 5/7
- `23:20:05` ✅ SUPPLY READINESS 100.0% (2097/1500 eligible rows) | LEARNING 0.0% (exam delta; 0 until an adapter is trained and examined)
- `23:20:05` ✅ GREEN -- scorecard recorded
