# ops 5203 -- KATLIN launch

**Status:** failure  
**Duration:** 883.0s  
**Finished:** 2026-09-04T18:54:39+00:00  

## Error

```
SystemExit: 1
```

## Log
- `18:39:56`    env keys from justhodl-equity-research: ['FMP_KEY', 'POLYGON_API_KEY']
- `18:39:57`   Lambda exists — updating
- `18:40:00` ✅   ✓ updated justhodl-katlin
- `18:40:05`    function state Active / Successful, 8192MB / 900s
- `18:40:06`    invoked async (prior generated_at=None); polling data/katlin.json
## schedules (EventBridge Scheduler, UTC)

- `18:54:38` ✅    justhodl-katlin-daily updated cron(10 4 ? * TUE-SAT *)
- `18:54:39` ✅    justhodl-katlin-backtest-weekly updated cron(30 9 ? * SUN *)
## page

- `18:54:39`    katlin.html carries marker KATLIN_DESK_V1 at the edge: True
- `18:54:39` ✗    no fresh data/katlin.json after 872s (check CloudWatch /aws/lambda/justhodl-katlin)
