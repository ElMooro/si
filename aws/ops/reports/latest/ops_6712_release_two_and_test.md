# ops 6712 - release macro-financial-intelligence + treasury-api from the operator hold and run each once

**Status:** success  
**Duration:** 8.3s  
**Finished:** 2026-11-02T16:14:52+00:00  

## Log
## A. governor release (operator holds)

- `16:14:45` ✅ macro-financial-intelligence: {"function": "macro-financial-intelligence", "status": "restored", "mtd_at_release": 0.0} | reserved concurrency now None
- `16:14:46` ✅ treasury-api: {"function": "treasury-api", "status": "restored", "mtd_at_release": 0.0} | reserved concurrency now None
- `16:14:46` ofrapi stays held (synthetic data): reserved concurrency 0
## B. treasury-api: scheduled update + one API answer

- `16:14:47` treasury-api {"source": "aws.events"} -> HTTP 200  in 1.4 s | payload {"statusCode": 200, "body": "Historical data updated"}
- `16:14:47`    log tail: Scheduled auto-update triggered | Error fetching comprehensive auction data: HTTP Error 400: Bad Request | No instrument data to store | REPORT RequestId: 32265fdc-bd90-47fb-bb55-046417fd2510	Duration: 398.54 ms	Billed Duration: 925 ms	Memory Size: 512 MB	Max Memory Used: 95 MB	Init Duration: 526.18 ms | XRAY TraceId: 1-6ae8b6f6-1b8f741d46b0c214560beeb7	SegmentId: 4b62ac7b995c00a6	Sampled: true
- `16:14:48` ⚠ history file: written 2026-05-28T10:00:06 (was 2026-05-28T10:00:06), 1682886 B, total_updates 81, last_updated 2026-05-28T10:00:05.105602+00:00, instruments 7, latest auction dates {"2-Year": "2026-05-26", "3-Year": "2026-05-11", "5-Year": "2026-05-27", "7-Year": "2026-04-28", "10-Year": "2026-05-12", "20-Year": "2026-05-20", "30-Year": "2026-05-13"}
- `16:14:48` treasury-api {"rawPath": "/auctions/current"} -> HTTP 200  in 0.4 s | payload {"statusCode": 200, "headers": {"Content-Type": "application/json"}, "body": "{\"message\": \"No complete auction data available\"}"}
- `16:14:48`    log tail: Error fetching comprehensive auction data: HTTP Error 400: Bad Request | REPORT RequestId: b491477e-371c-41bc-9ac7-6c4d3df16a03	Duration: 314.79 ms	Billed Duration: 315 ms	Memory Size: 512 MB	Max Memory Used: 95 MB | XRAY TraceId: 1-6ae8b6f8-72962eab71c1c57b0b209ff6	SegmentId: 9fad0f095528c3cc	Sampled: true
- `16:14:48`    /auctions/current: keys ['message'] | auction {"auction_date": null, "security_type": null, "security_term": null, "high_yield": null, "bid_to_cover_ratio": null}
## C. macro-financial-intelligence: one scheduled run, its brief, its route to the SSM FRED key

- `16:14:51` macro-financial-intelligence {} -> HTTP 200  in 2.4 s | payload {"statusCode": 200, "body": "{\"status\": \"success\", \"key\": \"daily_briefs/2026-11-02_brief.json\"}"}
- `16:14:51`    log tail: REPORT RequestId: ac78e40f-b12b-4e35-b7dd-225b7a4dc805	Duration: 1577.43 ms	Billed Duration: 2154 ms	Memory Size: 512 MB	Max Memory Used: 90 MB	Init Duration: 575.77 ms | XRAY TraceId: 1-6ae8b6f8-744ab8ab2adc3dc6792adb61	SegmentId: ad5aeaeb1fcf9819	Sampled: true
- `16:14:51` latest_brief.json: written 2026-11-02T16:14:52, 535 B: {"ts": "2026-11-02T16:14:51.249734+00:00", "fails": {"current_b": 1883.52, "avg_b": 1910.01, "z": -1.77, "alert": "NORMAL", "trend": "DOWN", "points": 13}, "inds": {"fed_funds": {"val": 3.88, "date": "2026-10-29"}, "curve": {"val": 0.38, "date": "2026-10-30"}, "t10y": {"val": 4.98, "date": "2026-10-29"}, "t2y": {"val": 4.61, "date": "2026-10-29"}}, "health": 7.5, "liq": "NORMAL", "alert": "NORMAL"}
- `16:14:51` ✅ FRED series filled: True (inds 4, fails ok)
- `16:14:51` role MacroFinancialIntelligenceRole ssm:GetParameter on /justhodl/fred/api-key: [('ssm:GetParameter', 'implicitDeny')]
- `16:14:52` role policies: attached ['justhodl-deny-rogue-compute', 'justhodl-deny-fleet-self-modify', 'AWSLambdaBasicExecutionRole'] | inline ['justhodl-dlq-send', 'MacroFinancialIntelligencePolicy', 'SESEmailPolicy']
- `16:14:52` ✅ ops 6712 complete
