## 2. Regenerate (Event invoke + S3 poll)

**Status:** failure  
**Duration:** 21.5s  
**Finished:** 2026-07-08T21:33:51+00:00  

## Error

```
SystemExit: 1
```

## Data

| age_min | comp_with_systems | kit_has_guard | n_claude_fail | n_claude_ok | n_compound | n_fails | n_long_theses | n_mu_grade | n_real_candidates | n_theses | n_warns | n_with_fundamentals | page_has_compound_block | sample_errors | tickers | verdict |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0.2 | 4 |  | 7 | 0 | 1 |  | 0 | 3 | 6 | 7 |  | 6 |  | ["all LLM paths returned empty", "all LLM paths returned empty"] | ['INTC', 'GOOGL', 'NVDA', 'GOOG', 'TSM', 'UPS', 'LDOS'] |  |
|  |  | True |  |  |  |  |  |  |  |  |  |  | True |  |  |  |
|  |  |  |  |  |  | 2 |  |  |  |  | 0 |  |  |  |  | FAIL |

## Log
## 3. Verify the board

## 4. Live page checks (warn-level, pages CDN lag)

## verdict

- `21:33:51` FAIL: LLM still failing: ok=0 fail=7 errs=['all LLM paths returned empty', 'all LLM paths returned empty']
- `21:33:51` FAIL: theses too short/empty (0 long of 7)
