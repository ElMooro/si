# ops 3149 — governance probe → premortem final gate

**Status:** failure  
**Duration:** 62.6s  
**Finished:** 2026-07-12T05:59:29+00:00  

## Error

```
SystemExit: 1
```

## Data

| caps_engines | daily_budget_usd | llm_path_open | mode | n_fails | n_warns | premortem_cap | rich | row_errors | spent_today_usd | theses | usage_rows | verdict |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 9 | 8.0 |  | on_demand |  |  | None |  |  | 0.0 |  | 2 |  |
|  |  | True |  |  |  |  | 0 | 15 |  | 15 |  |  |
|  |  |  |  | 1 | 0 |  |  |  |  |  |  | FAIL |

## Log
## 1. Live governance state (verbatim)

## 2. Narrow restores

- `05:58:28` ✅ mode on_demand with only $0.00/8 spent — stuck switch, restored to normal
## 3. Premortem invoke + gate

- `05:58:28` async invoke fired
- `05:59:29` error sample: {"symbol": "NVDA", "error": "empty", "raw": null}
- `05:59:29` ✗ LLM path open but only 0 rich theses — errors above name the residual
