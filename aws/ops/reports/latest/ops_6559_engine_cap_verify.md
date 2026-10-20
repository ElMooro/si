# ops 6559 - governor v1.3.0: per-engine $50/month kill switch, verified live

**Status:** success  
**Duration:** 30.1s  
**Finished:** 2026-10-20T19:42:49+00:00  

## Data

| cap | capped | engine_cap_usd_month | held_by_cap | meter_reads | mode | overrides | stage | version |
|---|---|---|---|---|---|---|---|---|
| 600.0 | [] | 50.0 |  |  | enforce |  | FREEZE | 1.3.0 |
|  |  |  | [["justhodl-event-coordinator", 431.7289, "2026-10-20T19:42:48.856487+00:00"], ["justhodl-miss-calibrator", 1551.2815, "2026-10-20T19:42:48.856487+00:00"]] | 1 |  | {} |  |  |

## Log
- `19:42:48` meter: ran=True window 475.0 h reconciled=True active=871 reads=1 fleet $103.867/day
- `19:42:48` month-to-date top: [["justhodl-miss-calibrator", 1551.28151], ["justhodl-event-coordinator", 431.728894], ["justhodl-symdir", 19.599732], ["justhodl-ai", 4.26166], ["justhodl-tradingview", 3.310218], ["justhodl-sdmx-walker", 2.762237], ["justhodl-provider-catalog", 2.251262], ["justhodl-polygon-options-flow", 2.248129], ["justhodl-calls-research-audit", 2.064944], ["justhodl-news-velocity", 1.901319]]
- `19:42:48` meter: reconciled 475.0 h window, 871 functions ran, fleet $103.87/day; month-to-date top: justhodl-miss-calibrator $1551.28, justhodl-event-coordinator $431.73, justhodl-symdir $19.60
- `19:42:49` cap_selftest: {"ok": true, "function": "justhodl-spend-governor-canary", "before": 0, "held": 0, "ledger_stage": "CAP", "release": {"function": "justhodl-spend-governor-canary", "status": "handed back to FREEZE (the fleet is in FREEZE)", "mtd_at_release": 51.0}, "after": 0}
- `19:42:49` ✅ kill switch proven on the canary: held at 0 under CAP, released only by release_cap (handed back to FREEZE (the fleet is in FREEZE))
- `19:42:49` ✅ v1.3.0 live: every engine is metered; $50/month of Lambda cost switches it off until Khalid releases it
