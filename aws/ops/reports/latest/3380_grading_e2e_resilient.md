# ops 3380 — grading E2E, throttle-resilient

**Status:** success  
**Duration:** 56.7s  
**Finished:** 2026-07-17T05:28:50+00:00  

## Error

```
SystemExit: 0
```

## Log
- `05:27:54` PASS  G0_stray_cleaned — 3379 stray removed=True
- `05:27:54` PASS  G0b_schedules_codified — [{"rule_name": "justhodl-outcome-checker-4h", "cron": "cron(29 21 * * ? *)"}, {"rule_name": "justhodl-outcome-checker-daily", "cron": "cron(0 23 * * ? *)"}]
- `05:28:34` PASS  G2_legacy_row_scored — outcomes=['day_21', 'day_5'] status=complete
- `05:28:34` PASS  G3_cleanup — deleted=True
- `05:28:35` PASS  G4_emitters_deployed — jsi 1.9.1 + hot-money ts markers
- `05:28:50` PASS  G5_jsi_191_healthy — signal_state={"regime_prev": "NORMAL", "regime_changed": false, "escalated": false, "flare": false}
- `05:28:50` VERDICT: PASS_ALL
