# ops 5524 (re-arm of 5522) -- arm Gear B: budget, coder card, owner control, holdout freeze dispatch, refusal chain

**Status:** success  
**Duration:** 1208.4s  
**Finished:** 2026-09-13T19:59:47+00:00  

## Data

| daily_usd | head | season_usd |
|---|---|---|
| 20.0 | 7f20e60517 | 600.0 |

## Log
- `19:39:39` ✅ justhodl-ai receipt commit=6d1617d run=34776809605: source == checkout, code_sha256 == live
- `19:39:39` ✅ cost-guard policy.daily_budget_usd=20.00 (training_max_runtime_s=3600, spot=True)
- `19:39:39` ⚠ catalog has no qwen+coder card; probing default ids ['huggingface-llm-qwen2-5-coder-7b-instruct', 'huggingface-llm-qwen2-5-coder-7b']
- `19:39:39` card huggingface-llm-qwen2-5-coder-7b-instruct: no training recipe (False)
- `19:39:40` card huggingface-llm-qwen2-5-coder-7b: no training recipe (None)
- `19:39:40` ✅ wrote factory/control/gearb.json (enabled=False model=unresolved)
- `19:39:41` gearb pre-freeze: {"at": "2026-09-13T19:39:41.009999Z", "polled": [], "built": null, "launched": null, "refusal": "gearb.enabled is false"}
- `19:39:41` ✅ pre-freeze refusal is the right one: gearb.enabled is false
- `19:39:42` ✅ dispatched factory-code-exam.yml freeze_holdout=true http=200
- `19:59:47` ⚠ holdout manifest not frozen within 20 min -- check the factory-code-exam.yml run; the hourly tick keeps refusing until it exists
- `19:59:47` ✅ data/ai.json gear_b: status=off budget={'daily_usd': 20.0, 'season_cap_usd': 600.0, 'committed_today_usd': 0.0, 'committed_season_usd': 0.0} leak=False
- `19:59:47` ✗ RED -- coder-card
