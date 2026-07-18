# ops 3446 — cannibals family

**Status:** success  
**Duration:** 3.9s  
**Finished:** 2026-07-18T04:32:22+00:00  

## Error

```
SystemExit: 0
```

## Log
- `04:32:19` PASS  G1_deployed_scheduled — settled=True sched=exists
- `04:32:20` FAIL  G2_feed_live — seen=None fresh=None logged=0 fresh_rows=[]
- `04:32:22` PASS  G3_signals_match — ddb_today=0 feed.logged=0 (0/0 valid if no fresh actionable auction)
- `04:32:22` VERDICT: GAPS: G2_feed_live
