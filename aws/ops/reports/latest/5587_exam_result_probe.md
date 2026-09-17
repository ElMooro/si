# ops 5587 -- exam result probe (read-only)

**Status:** success  
**Duration:** 2.5s  
**Finished:** 2026-09-17T13:54:45+00:00  

## Data

| LEARNING | ai_generated_at | base_evaluation | base_passed | base_score | candidate_generation | candidate_passed | candidate_score | critical_failures | evaluations | exam_job | gear_b | graded_at | missing_completions | scoreboard |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  | humaneval-frozen-848505e1dd021797 | 135/164 | 82.3% |  |  |  |  | 7 |  |  |  |  |  |
| -0.6 pts |  |  |  | 82.3% | gen-10 | 134/164 | 81.7% | 0 |  | jh-exam-gen10-20260917-070748 |  | 2026-09-17T13:52:33Z | 0 |  |
|  | 2026-09-17T13:50:16.579780+00:00 |  |  |  |  |  |  |  |  |  | {"version": "gear-b.1", "status": "armed", "refusal": null, "enabled": true, "model_card": "qwen2-5-coder-7b-instruct", "instance_type": "ml.g5.2xlarge", "budget": {"daily_usd": 20.0, "season_cap_usd": 600.0, "committed_today_usd": 18.18, "committed_season_usd": 45.45}, "holdout": {"frozen": true, "frozen_at": "2026-09-14T00:41:49.456835Z", "digest": "b926ab5e305c6ce5764adfcbc93ea5f89bc596424f31e26a4cdea54066781fd2"}, "dataset": {"generation": 9, "kept": 570, "floor": 500, "missing_rows": null, "licenses": {"CC-BY-4.0": 464, "MIT": 106}, "kinds": {"public_benchmark_train": 570}, "families": 2, |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  | {"voice": "offline (deterministic desk read, no calls): primary voice silent; second voice: glm failed: HTTP Error 429: Too Many Requests", "read_path": "deterministic"} |

## Log
## 1. Exam evaluations on disk (factory/exams/code/results/)

- `13:54:43` gen-0-34900737918.json  gen=gen-0 score=0.0% passed=0/164 critical=21 missing=0 burst=jh-exam-gen0-20260914-213415 at=2026-09-14T21:48:21Z (LastModified 09-14 21:48 UTC)
- `13:54:43` gen-0-34902101944.json  gen=gen-0 score=0.0% passed=0/164 critical=21 missing=0 burst=jh-exam-gen0-20260914-213415 at=2026-09-14T22:03:44Z (LastModified 09-14 22:03 UTC)
- `13:54:44` gen-0-34903812485.json  gen=gen-0 score=0.0% passed=0/164 critical=157 missing=0 burst=jh-exam-gen0-20260914-213415 at=2026-09-14T22:23:30Z (LastModified 09-14 22:23 UTC)
- `13:54:44` gen-0-34905498719.json  gen=gen-0 score=0.0% passed=0/164 critical=157 missing=0 burst=jh-exam-gen0-20260914-213415 at=2026-09-14T22:43:58Z (LastModified 09-14 22:43 UTC)
- `13:54:44` base.json  gen=gen-0 score=82.3% passed=135/164 critical=0 missing=0 burst=jh-exam-gen0-20260914-213415 at=2026-09-14T22:45:56Z (LastModified 09-14 22:45 UTC)
- `13:54:44` gen-0-34905651137.json  gen=gen-0 score=82.3% passed=135/164 critical=0 missing=0 burst=jh-exam-gen0-20260914-213415 at=2026-09-14T22:45:56Z (LastModified 09-14 22:45 UTC)
- `13:54:44` gen-10-35229787292.json  gen=gen-10 score=81.7% passed=134/164 critical=0 missing=0 burst=jh-exam-gen10-20260917-070748 at=2026-09-17T13:52:33Z (LastModified 09-17 13:52 UTC)
## 2. LEARNING (coding) from the newest candidate evaluation

- `13:54:44` ⚠ LEARNING = -0.6 pts (candidate 81.7% - base 82.3%)
## 3. Gear B decisions / champion

- `13:54:44` champion.json: {"_error": "An error occurred (NoSuchKey) when calling the GetObject operation: The specified key does not exist."}
## 4. Training / exam jobs (SageMaker) since 2026-09-16

- `13:54:44` jh-exam-gen11-20260917-110748  Completed/Completed  billable=288s  created=09-17 11:07  
- `13:54:44` jh-gearb-gen11-20260917-081226  Completed/Completed  billable=444s  created=09-17 08:12  
- `13:54:45` jh-exam-gen10-20260917-070748  Completed/Completed  billable=287s  created=09-17 07:07  
- `13:54:45` jh-gearb-gen10-20260917-061216  Completed/Completed  billable=388s  created=09-17 06:12  
- `13:54:45` jh-gearb-gen9-20260917-011357  Stopped/MaxWaitTimeExceeded  billable=0s  created=09-17 01:13  
- `13:54:45` jh-gearb-gen8-20260917-000739  Stopped/Stopped  billable=0s  created=09-17 00:07  
- `13:54:45` jh-gearb-gen7-20260916-091227  Stopped/MaxWaitTimeExceeded  billable=6386s  created=09-16 09:12  
- `13:54:45` jh-gearb-gen6-20260916-041229  Stopped/MaxRuntimeExceeded  billable=7446s  created=09-16 04:12  
- `13:54:45` jh-gearb-gen5-20260916-000739  Stopped/MaxRuntimeExceeded  billable=7797s  created=09-16 00:07  
## 5. What data/ai.json publishes right now

- `13:54:45` ✅ probe complete (no writes, no launches)
