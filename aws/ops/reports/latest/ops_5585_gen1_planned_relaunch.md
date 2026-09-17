# ops 5585 -- generation 1 relaunch: bundle re-pinned with the planned-steps + time-budget trainer, launcher proven, Gear B tick launch=true

**Status:** failure  
**Duration:** 3.6s  
**Finished:** 2026-09-17T01:05:57+00:00  

## Data

| head |
|---|
| ff20a970c5 |

## Log
- `01:05:54` ✅ bundle re-pinned: train-0dbdf02a4a83d435.tar.gz -> train-2ae73dfc475bcb1b.tar.gz; recipe plans steps + stops on the budget=True; bundle recipe == checkout=True; pin epochs=3 max_steps(ceiling)=400; image kept=True
- `01:05:54` ✅ justhodl-ai receipt commit=0cbcace (source identical to HEAD ff20a97), code_sha256 matches live, run=35169096430
- `01:05:54` ✅ gearb control: {"max_family_share": 0.85, "min_sft_rows": 500, "max_jobs_per_day": 3, "daily_budget_usd": 20.0, "season_cap_usd": 600.0} -> max_jobs_per_day 4 for today (dated copy kept; daily $20.0 and season $600.0 caps untouched)
- `01:05:57` ✅ tick status=200: {"refusal": null, "built": null, "launched": null, "polled": null, "error": null, "reason": null}
- `01:05:57` ✅ tick result: refusal=a job is still running built=null
- `01:05:57` ✗ no training job launched -- refusal chain: {"ok": true, "action": "/gearb/tick", "result": {"at": "2026-09-17T01:05:57.145527Z", "polled": [{"job_name": "jh-gearb-gen8-20260917-000739", "status": "InProgress"}], "built": null, "launched": null, "refusal": "a job is still running"}, "at": "2026-09-17T01:05:57.589949+00:00"}
