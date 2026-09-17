# ops 5586 -- stop the doomed gen8 job (old bundle) and relaunch generation 1 on the planned-steps + time-budget bundle

**Status:** failure  
**Duration:** 2.6s  
**Finished:** 2026-09-17T01:27:11+00:00  

## Data

| head |
|---|
| a40877eed6 |

## Log
- `01:27:08` ✅ pinned bundle: train-b64e3b4bc006dde1.tar.gz (epochs=3); InProgress gearb jobs: ['jh-gearb-gen9-20260917-011357']
- `01:27:09` ✅ jh-gearb-gen9-20260917-011357: secondary=Starting started=None bundle=ng/bundles/train-23b4c7807e7682f8.tar.gz time_budget_s=9600 -> new recipe, leaving it alone
- `01:27:09` ✅ gearb control: max_jobs_per_day=4 daily=$20.0 season=$600.0 (unchanged by this op)
- `01:27:11` ✅ tick status=200: {"refusal": null, "built": null, "launched": null, "polled": null, "error": null, "reason": null}
- `01:27:11` ✅ tick result: refusal=a job is still running built=null
- `01:27:11` ✗ no training job launched -- refusal chain: {"ok": true, "action": "/gearb/tick", "result": {"at": "2026-09-17T01:27:10.536779Z", "polled": [{"job_name": "jh-gearb-gen9-20260917-011357", "status": "InProgress"}], "built": null, "launched": null, "refusal": "a job is still running"}, "at": "2026-09-17T01:27:11.030509+00:00"}
