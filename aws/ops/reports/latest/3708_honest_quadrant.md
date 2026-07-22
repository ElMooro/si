# ops 3708 — backlog reverse-engineering, stage 1

**Status:** failure  
**Duration:** 38.2s  
**Finished:** 2026-07-22T16:21:07+00:00  

## Error

```
SystemExit: 1
```

## Log
- `16:20:30` G1_shipped True
- `16:20:51` G2_backlog_asof True
- `16:21:06` G3_invoke True
- `16:21:07` G4_flow_conservation True
- `16:21:07` G5_join_live False
- `16:21:07` G7_no_unearned_twice True
- `16:21:07` G8_rpo_denominator_sane True
- `16:21:07` G6_unbooked_test True
- `16:21:07` VERDICT: GAPS: G5_join_live
