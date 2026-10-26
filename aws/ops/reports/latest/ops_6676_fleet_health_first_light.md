# ops 6676 - fleet-health fixes: first light (one synchronous run each)

**Status:** success  
**Duration:** 855.1s  
**Finished:** 2026-10-26T16:16:04+00:00  

## Log
## 0. deployed versions

- `16:01:49` justhodl-analytics-snapshot: 1024 MB / 600 s, modified 2026-10-26T15:54:18
- `16:01:49` justhodl-earnings-whisper: 1536 MB / 180 s, modified 2026-10-26T15:54:46
- `16:01:49` justhodl-event-flow-monitor: 1024 MB / 300 s, modified 2026-10-26T15:55:11
- `16:01:50` justhodl-signal-genealogy: 2048 MB / 600 s, modified 2026-10-26T15:59:37
- `16:01:50` live fan-out manifest: treasury-auto-updater in ticks [] (expected [])
## 1. invocations (parallel, synchronous)

- `16:16:02` justhodl-analytics-snapshot: status 200 error None in 332.8s | {"statusCode": 200, "body": "{\"n_research_rows\": 20005, \"n_edgar_rows\": 4953, \"n_critique_rows\": 76, \"n_etf_flow_rows\": 0, \"research_skipped\": 0, \"edgar_skipped\": 0, \"critique_skipped\": 0, \"complete\": true, \"pending\": 0, \"read_stats\": {\"research_critique\": {\"listed\": 76, \"reused\": 0, \"read\": 76, \"errors\": 0, \"pending\": 0, \"dropped\": 0}, \"edgar_insiders\": {\"listed\": 4953, \"reused\": 0, \"read\": 4953, \"errors\": 0, \"pending\": 0, \"dropped\": 0}, \"equity_research\": {\"listed\": 20005, \"reused\": 20005, \"read\": 0, \"errors\": 0, \"pending\": 0, \"dropped\": 0}}, \"written\": [{\"key\": \"analytics/research_critique_flat.json\", \"n_rows\": 76, \"si
- `16:16:02` justhodl-earnings-whisper: status 200 error None in 3.6s | {"statusCode": 200, "body": "{\"ok\": true, \"n_upcoming\": 83, \"tier_counts\": {\"S\": 0, \"A\": 0, \"B\": 0, \"C\": 83}, \"duration_s\": 2.81}"}
- `16:16:02` justhodl-event-flow-monitor: status 200 error None in 12.2s | {"statusCode": 200, "body": "{\"ok\": true, \"pulse\": \"RED\", \"events_today\": 0, \"anomalies\": 1, \"alerts_sent\": 1}"}
- `16:16:02` justhodl-signal-genealogy: status None error ConnectionClosedError: Connection was closed before we received a valid response from endpoint URL: "https://lambda.us-east-1.amazonaws.com/2015-03-31/functions/justhodl-signal-genealogy/invocations". in 852.0s | 
## 2. REPORT lines

- `16:16:02` justhodl-analytics-snapshot: Duration: 46996.08 ms Max Memory Used: 274 MB Init Duration: 526.06 ms
- `16:16:02` justhodl-earnings-whisper: Duration: 2888.18 ms Max Memory Used: 329 MB Init Duration: 489.80 ms
- `16:16:03` justhodl-event-flow-monitor: Duration: 11037.86 ms Max Memory Used: 101 MB Init Duration: 687.36 ms
## 3. published outputs

- `16:16:03` analytics/manifest.json: generated 2026-10-26T16:07:16.378515+00:00 complete True pending {'research_critique': 0, 'edgar_insiders': 0, 'equity_research': 0} read_stats {"research_critique": {"listed": 76, "reused": 0, "read": 76, "errors": 0, "pending": 0, "dropped": 0}, "edgar_insiders": {"listed": 4953, "reused": 0, "read": 4953, "errors": 0, "pending": 0, "dropped": 0}, "equity_research": {"listed": 20005, "reused": 20005, "read": 0, "errors": 0, "pending": 0, "dropped": 0}}
- `16:16:03` analytics/equity_research_flat.json: n_rows 20005 complete True pending 0 generated 2026-10-26T16:07:16.378515+00:00
- `16:16:04` analytics/edgar_insiders_flat.json: n_rows 4953 complete True pending 0 generated 2026-10-26T16:07:16.378515+00:00
- `16:16:04` analytics/research_critique_flat.json: n_rows 76 complete True pending 0 generated 2026-10-26T16:07:16.378515+00:00
- `16:16:04` data/earnings-whisper.json: as_of 2026-10-26T16:01:53+00:00 engine_version 1.1.0 n_upcoming 83 tiers {'S': 0, 'A': 0, 'B': 0, 'C': 83} duration 2.81 input_bytes {"data/earnings-tracker.json": 37290, "data/eps-revision-velocity.json": 55061180, "data/options-flow-scanner.json": 19421817, "data/insider-trades.json": 1040340, "data/8k-filings.json": 5500804, "data/revenue-acceleration.json": 4112112, "data/smart-money-clusters.json": 26635}
- `16:16:04` data/event-flow-health.json: generated 2026-10-26T16:01:51.364068+00:00 engine_version 1.1.0 days_read 14/14 present 4 pulse {"status": "RED", "last_event_age_minutes": null, "last_event_ts": null, "last_event_type": null, "reason": "no events today"} anomalies 1 alerts {"sent": ["pulse-red"], "skipped_cooldown": []}
- `16:16:04` signal-genealogy current.json: {"generated_at": "2026-10-26T16:06:36.098768+00:00", "version": "2.0.0", "contract": "genealogy-research-head.v1", "authority": {"calls_eligible": false, "execution_eligible": false, "forecast_qualified": false, "sizing_eligible": false}, "replay": {"bytes": 1048074, "key": "data/signal-genealogy-research/runs/ed5a2eead235d1324a84cff750ee6a0cdefe0eeeb7d397c89b7a9a74db6c391c.json", "sha256": "ed5a2eead235d1324a84cff750ee6a0cdefe0eeeb7d397c89b7a9a74db6c391c"}}
- `16:16:04` ⚠ 1 of 4 invocations did not return clean: justhodl-signal-genealogy
- `16:16:04` ✅ ops 6676 complete
