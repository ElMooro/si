# ops 5584 -- AI engine learning progress: what ai.html shows, what is measured, what is blocked (objects only)

**Status:** failure  
**Duration:** 381.8s  
**Finished:** 2026-09-16T19:31:58+00:00  

## Data

| admitted_rows | at | base_exam_pct | blockers | exam_armed | head | learning_coding_pts | stages_done | supply_floor | supply_pct | supply_unique_tasks |
|---|---|---|---|---|---|---|---|---|---|---|
|  | 2026-09-16T19:25:36+00:00 |  |  |  | aa12c30774 |  |  |  |  |  |
| 2100 |  | 82.3 | 2 | None |  | None | 5/8 | 500 | 100.0 | 570 |

## Log
## A. data/ai.json -- the read model behind ai.html

- `19:25:37` ✅ data/ai.json last written 2026-09-16 19:20:18+00:00 (0.1 h ago); top-level keys: ['access', 'brain_dataset', 'catalog', 'cost', 'definitions', 'elapsed_s', 'engine', 'fleet_inputs', 'gear_b', 'generated_at', 'inventory_summary', 'market_read', 'pipeline', 'pipeline_verdict', 'policy', 'region', 'scoreboard', 'tiers', 'version']
- `19:25:37` ✅ scoreboard: {"as_of": "2026-09-16T19:20:16.852630+00:00", "calls_graded": 0, "calls_made": 0, "categories_excluded": ["lesson", "macro"], "categories_learned": ["philosophy", "thesis"], "coin_flip_loss": 0.693, "hit_rate_by_window": {"21": {"hit_rate": null, "hits": 0, "n": 0}, "5": {"hit_rate": null, "hits": 0, "n": 0}, "63": {"hit_rate": null, "hits": 0, "n": 0}}, "last_read_at": "2026-09-16T05:46:38.540377+00:00", "latest_validation_loss": 0.05178999900817871, "lessons_carried": 0, "lessons_updated_at": null, "loss_history": [0.13564999401569366, 0.05178999900817871, 0.05178999900817871], "notes_studied": 13051, "retrains": 3, "trend": "stable", "understanding_score": 0.925, "voice": "online"}
- `19:25:37` ✅ market_read (public): {"generated_at": "2026-09-16T05:46:38.540377+00:00", "n_calls_this_read": 0, "n_opportunities": 0, "parse_error": false, "performance": {"by_window": {"21": {"hit_rate": null, "hits": 0, "n": 0}, "5": {"hit_rate": null, "hits": 0, "n": 0}, "63": {"hit_rate": null, "hits": 0, "n": 0}}, "n_calls": 0}, "playbook_available": true, "read_id": "20260916T054638Z", "stances": {"bonds": "NEUTRAL", "crypto": "HOLD", "metals": "HOLD", "stocks": "SELECTIVE"}}
- `19:25:37` ✅ board sources: 15 fresh ['bonds', 'bottom', 'brain', 'btc_cycle', 'crisis', 'crypto', 'fortress', 'fusion', 'gbc', 'katlin', 'khalid_risk', 'metals', 'regime_composite', 'risk_gate', 'scorecard']; 0 stale/missing []
- `19:25:37` ✅ Brain pipeline: status=done stage=done finished_at=2026-09-10T19:45:45.595972+00:00 classifier={"train:mlogloss": 0.010700000450015068, "validation:mlogloss": 0.05178999900817871} error=None
- `19:25:37` ✅ Brain learning block: curves=0 classifier_runs=0
- `19:25:37` ✅ gear_b (public): {"budget": {"committed_season_usd": 27.27, "committed_today_usd": 13.635, "daily_usd": 20.0, "season_cap_usd": 600.0}, "champion": {"generation": 0, "note": "base model; no weights promoted"}, "dataset": {"at": "2026-09-16T14:12:31.132641Z", "families": 2, "floor": 500, "generation": 8, "kept": 570, "kinds": {"public_benchmark_train": 570}, "licenses": {"CC-BY-4.0": 464, "MIT": 106}, "missing_rows": null, "ok": true, "reason": null}, "enabled": true, "holdout": {"digest": "b926ab5e305c6ce5764adfcbc93ea5f89bc596424f31e26a4cdea54066781fd2", "frozen": true, "frozen_at": "2026-09-14T00:41:49.456835Z"}, "instance_type": "ml.g5.2xlarge", "jobs": {"by_status": {"Failed": 2, "Stopped": 4}, "total": 6}, "model_card": "qwen2-5-coder-7b-instruct", "refusal": null, "status": "armed", "version": "gear-b.1"}
- `19:25:37` ✅ health/voice: {}
## B. student tick, chain of command, Monday-Friday wall

- `19:25:37` ✅ student-state (public): generated_at=2026-09-16T19:25:33+00:00 (0.0 h ago) gen=1 outer_status={"funding": {"dvp_rate": {"checked_at": "2026-09-16T19:19:33+00:00", "data_unavailable": false, "observation_age_seconds": 242373.036072, "observed_at": "2026-09-14T00:00:00+00:00", "raw_sha256": "ecc...(4334 chars) health={"discipline": "factory-doctrine.v1", "elapsed_seconds": 0.87, "errors": [], "general_coding": "blocked_no_verified_generative_model", "last_tick_at": "2026-09-16T19:25:33+00:00", "lease_fence": 4891, "protected_exam": "independent_grader_role", "state_authority": "private_conditional_s3", "status": "running"} ranks={"cards": {"coder": {"co": null, "errors_window": 0, "graded_window": 0, "kind": "builtin", "last_graded_at": null, "last_verdict": "hold:min_graded_tasks", "last_verdict_at": "2026-09-16T19:15:33+00:00", "pass_rate": null, "prior_pass_rate": null, "rank": "recruit", "since": "2026-09-13T17:59:28+00:00", "status": "active", "verdicts": 0}, "deployer": {"co": null, "errors_window": 0, "graded_window": 0, "kind": "builtin", "last_graded_at": null, "last_verdict": "hold:min_graded_tasks", "last_ver...(2040 chars)
- `19:25:37` ✅ data/ai-factory.json: {"agents": [{"id": "student", "name": "Student", "purpose": "Select bounded experiments and preserve verified skills", "status": "active"}, {"id": "coder", "name": "Coder", "purpose": "Generate restricted candidate programs and repair proposals", "status": "bounded_program_search"}, {"id": "researcher", "name": "Researcher", "purpose": "Validate approved external sources and provenance", "status": "active"}, {"id": "investor", "name": "Investor", "purpose": "Submit research forecasts; no orders", "status": "research_baseline"}, {"id": "deployer", "name": "Deployer", "purpose": "Queue exact art...(6413 chars)
- `19:25:37` ✅ data/factory-public.json: {"agents": [{"id": "student", "name": "Student", "purpose": "Select bounded experiments and preserve verified skills", "status": "active"}, {"id": "coder", "name": "Coder", "purpose": "Generate restricted candidate programs and repair proposals", "status": "bounded_program_search"}, {"id": "researcher", "name": "Researcher", "purpose": "Validate approved external sources and provenance", "status": "active"}, {"id": "investor", "name": "Investor", "purpose": "Submit research forecasts; no orders", "status": "research_baseline"}, {"id": "deployer", "name": "Deployer", "purpose": "Queue exact art...(6413 chars)
- `19:25:37` ✅ wall ledger: season=season-2026-09-14 keys=['entries', 'final_weeks', 'polls', 'schema_version', 'season', 'updated_at'] entries=0 by_status={}
- `19:25:38` ✅ official prints on disk (weeks): []
- `19:25:38` ✅ salon objects: ['chat/owner.json']
## C. coding lane -- supply (verified rows) and bursts

- `19:25:38` ✅ gearb control: {"approved_at": "2026-09-13T19:39:40.169947Z", "daily_budget_usd": 20.0, "enabled": true, "max_family_share": 0.85, "max_jobs_per_day": 3, "min_sft_rows": 500, "model_id": "qwen2-5-coder-7b-instruct", "model_source": "own", "model_version": "c03e6d358207e414f1eca0bb1891e29f1db0e242", "season_cap_usd": 600.0}
- `19:25:38` ✅ base weights: {"file_count": null, "license": "apache-2.0", "repo": "Qwen/Qwen2.5-Coder-7B-Instruct", "revision": "c03e6d358207e414f1eca0bb1891e29f1db0e242", "total_bytes": 15242805878}
- `19:31:54` ✅ verified rows on disk: 3629; by checker: {"factory-code-verify:v4-supervisor-judge": 1527, "factory-code-exam:a340d8ebd559": 464, "factory-trace-verify:network-less-container": 1529, "factory-code-exam:caf2b53d3151": 106, "factory-code-verify:v3-supervisor-judge": 3}
- `19:31:54` ✅ ADMITTED by the current checker rule (supervisor judge v3+, runner-verified, passed): 2100 rows over 570 distinct tasks; families {"mbpp": 1780, "apps-introductory": 320}; newest row at None (None h ago)
- `19:31:55` ✅ burst/exam job records: 6 {'burst': 5, 'exam': 1}; newest: {"job_name": "jh-burst-gen0b2-20260914-214444", "kind": "burst", "launched_at": "2026-09-14T21:47:56.194525+00:00", "status": "launching"}
## D. coding lane -- training jobs, candidate, frozen exam, champion

- `19:31:56`   gearb job jh-gearb-gen1-20260915-024201 gen=1 status=Failed launched=2026-09-15T02:42:01 cap=$4.545 reason=
- `19:31:56`   gearb job jh-gearb-gen3-20260915-031242 gen=3 status=Failed launched=2026-09-15T03:12:42 cap=$4.545 reason=
- `19:31:56`   gearb job jh-gearb-gen4-20260915-071207 gen=4 status=Stopped launched=2026-09-15T07:12:07 cap=$4.545 reason=
- `19:31:56`   gearb job jh-gearb-gen5-20260916-000739 gen=5 status=Stopped launched=2026-09-16T00:07:39 cap=$4.545 reason=
- `19:31:56`   gearb job jh-gearb-gen6-20260916-041229 gen=6 status=Stopped launched=2026-09-16T04:12:29 cap=$4.545 reason=
- `19:31:56`   gearb job jh-gearb-gen7-20260916-091227 gen=7 status=Stopped launched=2026-09-16T09:12:27 cap=$4.545 reason=
- `19:31:56` ✅ NEWEST TRAINING JOB jh-gearb-gen7-20260916-091227: status=Stopped secondary=MaxWaitTimeExceeded started=2026-09-16 10:36:07.349000+00:00 ended=2026-09-16 13:14:59.363000+00:00 train_s=9527 billable_s=6386 reason=
- `19:31:56`     2026-09-16 09:12:27 -> Starting: Preparing the instances for training
- `19:31:56`     2026-09-16 10:36:07 -> Downloading: Downloading the training image
- `19:31:56`     2026-09-16 10:39:03 -> Training: Training image download completed. Training in progress.
- `19:31:56`     2026-09-16 13:14:41 -> Stopping: Stopping the training job
- `19:31:56`     2026-09-16 13:14:46 -> Uploading: Uploading generated training model
- `19:31:56`     2026-09-16 13:14:59 -> MaxWaitTimeExceeded: Training job wait time exceeded MaxWaitTimeInSeconds provided
- `19:31:56` ✅ candidates: {}
- `19:31:56` ✅ weights champion: none (base model serves)
- `19:31:56` ✅ extracted adapters: {}
- `19:31:57`   exam result base.json: gen=gen-0 passed=135/164 score=0.8232 critical=0 missing=0 at=2026-09-14T22:45:56Z
- `19:31:57`   exam result gen-0-34900737918.json: gen=gen-0 passed=0/164 score=0.0 critical=21 missing=0 at=2026-09-14T21:48:21Z
- `19:31:57`   exam result gen-0-34902101944.json: gen=gen-0 passed=0/164 score=0.0 critical=21 missing=0 at=2026-09-14T22:03:44Z
- `19:31:57`   exam result gen-0-34903812485.json: gen=gen-0 passed=0/164 score=0.0 critical=157 missing=0 at=2026-09-14T22:23:30Z
- `19:31:57`   exam result gen-0-34905498719.json: gen=gen-0 passed=0/164 score=0.0 critical=157 missing=0 at=2026-09-14T22:43:58Z
- `19:31:57`   exam result gen-0-34905651137.json: gen=gen-0 passed=135/164 score=0.8232 critical=0 missing=0 at=2026-09-14T22:45:56Z
- `19:31:57` ✅ BASE EXAM: 135/164 = 82.3% (critical 0, evaluation humaneval-frozen-848505e1dd021797)
- `19:31:57` ✅ LEARNING (coding, frozen holdout): no trained adapter has been examined yet -> null
## E. owned serving endpoint and the ai.html chat

- `19:31:57` ✅ owned endpoint jh-owned-coder-async: InService (created 2026-09-14 22:48:19); chat control enabled=True model=qwen2-5-coder-7b-instruct; chat requests on disk: 3 (newest 2026-09-14 23:14:05+00:00, 44.2 h ago)
## F. market read -- the AI's own calls and their grading

- `19:31:58` ✅ latest read: at=2026-09-16T05:46:38.540377+00:00 (13.6 h ago) voice=None parse_error=None overall="gate:ok authority:ok sizing:ok table:ok plumbing:ok credit:ok dollar:ok vol:ok constitution:ok" calls=0 lessons_carried=None; lessons on file=0
- `19:31:58` ✅ graded performance (public scoreboard): {"by_window": {"21": {"hit_rate": null, "hits": 0, "n": 0}, "5": {"hit_rate": null, "hits": 0, "n": 0}, "63": {"hit_rate": null, "hits": 0, "n": 0}}, "n_calls": 0}
## G. the one blocked step -- gen-1 adapter exam

## training job container log tail

- `19:31:58`     42%|████▎     | 170/400 [2:13:13<3:00:13, 47.01s/it]
- `19:31:58`     43%|████▎     | 171/400 [2:14:00<2:59:26, 47.02s/it]
- `19:31:58`     43%|████▎     | 172/400 [2:14:47<2:58:40, 47.02s/it]
- `19:31:58`     43%|████▎     | 173/400 [2:15:34<2:57:54, 47.02s/it]
- `19:31:58`     44%|████▎     | 174/400 [2:16:21<2:57:07, 47.02s/it]
- `19:31:58`     44%|████▍     | 175/400 [2:17:08<2:56:21, 47.03s/it]
- `19:31:58`     44%|████▍     | 176/400 [2:17:55<2:55:33, 47.02s/it]
- `19:31:58`     44%|████▍     | 177/400 [2:18:42<2:54:45, 47.02s/it]
- `19:31:58`     44%|████▍     | 178/400 [2:19:29<2:53:59, 47.02s/it]
- `19:31:58`     45%|████▍     | 179/400 [2:20:16<2:53:13, 47.03s/it]
- `19:31:58`     45%|████▌     | 180/400 [2:21:03<2:52:25, 47.02s/it]
- `19:31:58`     {'loss': 0.0066, 'grad_norm': 0.09099508821964264, 'learning_rate': 6.044837815156377e-05, 'epoch': 42.32}
- `19:31:58`     45%|████▌     | 180/400 [2:21:03<2:52:25, 47.02s/it]
- `19:31:58`     45%|████▌     | 181/400 [2:21:50<2:51:38, 47.02s/it]
- `19:31:58`     46%|████▌     | 182/400 [2:22:37<2:50:50, 47.02s/it]
- `19:31:58`     46%|████▌     | 183/400 [2:23:24<2:50:05, 47.03s/it]
- `19:31:58`     46%|████▌     | 184/400 [2:24:11<2:49:17, 47.03s/it]
- `19:31:58`     46%|████▋     | 185/400 [2:24:58<2:48:29, 47.02s/it]
- `19:31:58`     46%|████▋     | 186/400 [2:25:45<2:47:42, 47.02s/it]
- `19:31:58`     47%|████▋     | 187/400 [2:26:32<2:46:53, 47.01s/it]
- `19:31:58`     47%|████▋     | 188/400 [2:27:19<2:46:07, 47.02s/it]
- `19:31:58`     47%|████▋     | 189/400 [2:28:06<2:45:21, 47.02s/it]
- `19:31:58`     48%|████▊     | 190/400 [2:28:53<2:44:34, 47.02s/it]
- `19:31:58`     {'loss': 0.0054, 'grad_norm': 0.035148486495018005, 'learning_rate': 5.645940686977033e-05, 'epoch': 44.65}
- `19:31:58`     48%|████▊     | 190/400 [2:28:53<2:44:34, 47.02s/it]
- `19:31:58`     48%|████▊     | 191/400 [2:29:40<2:43:47, 47.02s/it]
- `19:31:58`     48%|████▊     | 192/400 [2:30:27<2:43:01, 47.02s/it]
- `19:31:58`     48%|████▊     | 193/400 [2:31:14<2:42:14, 47.02s/it]
- `19:31:58`     48%|████▊     | 194/400 [2:32:01<2:41:26, 47.02s/it]
- `19:31:58`     49%|████▉     | 195/400 [2:32:48<2:40:39, 47.02s/it]
## H. scorecard

- `19:31:58` ✅ stages: weights staged=yes, bursts produced candidates=yes, independent judge admitted rows=yes, supply floor reached (unique tasks)=yes, base exam trusted=yes, adapter trained=no, candidate examined=no, candidate promoted=no -> 5/8
- `19:31:58` ⚠ BLOCKER: no official prints written yet -- wall entries can never grade (factory-official-prints.yml runs Sat/Sun)
- `19:31:58` ⚠ BLOCKER: training job jh-gearb-gen7-20260916-091227 Stopped: 
- `19:31:58` ✅ LEARNING (coding) not measured yet | SUPPLY 100.0% (570 unique tasks / 500 floor) | BASE 82.3%
- `19:31:58` ✗ RED -- the trained-adapter step failed; see the log tail; nothing examined
