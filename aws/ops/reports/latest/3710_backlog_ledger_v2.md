# ops 3710 — backlog reverse-engineering, stage 1

**Status:** failure  
**Duration:** 644.0s  
**Finished:** 2026-07-22T16:45:00+00:00  

## Error

```
SystemExit: 1
```

## Log
- `16:34:17` G1_shipped True
- `16:44:45` G2b_ledger_accumulates False
- `16:44:45` G2_backlog_asof True
- `16:45:00` G3_invoke True
- `16:45:00` G4_flow_conservation True
- `16:45:00` G5_join_live True
- `16:45:00` G7_no_unearned_twice True
- `16:45:00` G8_rpo_denominator_sane True
- `16:45:00` G6_unbooked_test True
- `16:45:00` VERDICT: GAPS: G2b_ledger_accumulates
