# ops 3415 — joins wave 2

**Status:** success  
**Duration:** 72.1s  
**Finished:** 2026-07-17T23:06:03+00:00  

## Error

```
SystemExit: 0
```

## Log
- `23:04:53` PASS  G0_all_settled — 5 engines settled
- `23:04:53` PASS  G1_registry_schedule — exists
- `23:05:33` PASS  G2_registry — feeds=785 stale=105 worst=['data/feedback-summary.json', 'data/history-api-url.json', 'data/user-trades-stats.json', 'data/user-trades.json']
- `23:05:36` PASS  G3_squeeze_guard — book=6 excluded=[] risk_tagged=0
- `23:05:56` PASS  G4_jsi_src — jsi_pctile=29.728588024078572 src=jsi-history.self scale=1.0
- `23:06:03` PASS  G5_setups_context — sector_ctx=24/25 walls=0/25 playbook_rules=3 sample_pb=[{"id": "tv-0a62c4e914bbe3e3", "symbol": "UNTAGGED", "family": "TIMING", "text": "[TV:UNTAGGED] ECONOMY CRASH LAGS YIELD
- `23:06:03` VERDICT: PASS_ALL
