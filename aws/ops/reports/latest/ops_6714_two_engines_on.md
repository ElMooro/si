# ops 6714 - treasury-api + macro-financial-intelligence: deployed fixes, triggers, one run each

**Status:** success  
**Duration:** 25.8s  
**Finished:** 2026-11-02T16:37:51+00:00  

## Log
## A. deployed code

- `16:37:27` ✅ treasury-api: fix in deployed code True | modified 2026-11-02T16:29:36 | reserved concurrency None
- `16:37:28` ✅ macro-financial-intelligence: fix in deployed code True | modified 2026-11-02T16:29:11 | reserved concurrency None
## B. triggers + the deployed manifest copy

- `16:37:28` macro-financial-daily-execution before: ENABLED rate(1 day) targets ['macro-financial-intelligence']
- `16:37:28` ✅ macro-financial-daily-execution now ENABLED
- `16:37:29` scheduler role justhodl-scheduler-role may invoke treasury-api: allowed
- `16:37:29` ✅ treasury-api-history-update: ENABLED cron(0 10 ? * MON,THU *) UTC -> treasury-api input {"source":"aws.events"}
- `16:37:31` ✅ deployed manifest: ['macro-financial-daily-execution', 'treasury-api-history-update'] changed (backup backups/schedule-manifest/20261102T163730Z/); 1009 rules
- `16:37:32` schedule reconciler mode: enforce-duplicates (not changed)
## C. one run each, read back

- `16:37:37` treasury-api {"source": "aws.events"} -> 200  | {"statusCode": 200, "body": "Historical data updated"}
- `16:37:37`    log: Scheduled auto-update triggered | Enhanced historical data updated with 7 instruments
- `16:37:38` ✅ history file written 2026-11-02T16:37:38 (was 2026-05-28T10:00:06), 82 updates kept, 7 instruments: {"2-Year": {"auction_date": "2026-09-22", "bid_to_cover_ratio": 2.63, "high_yield": null}, "3-Year": {"auction_date": "2026-10-06", "bid_to_cover_ratio": 2.62, "high_yield": null}, "5-Year": {"auction_date": "2026-09-23", "bid_to_cover_ratio": 2.21, "high_yield": null}, "7-Year": {"auction_date": "2026-09-24", "bid_to_cover_ratio": 2.42, "high_yield": null}, "10-Year": {"auction_date": "2026-08-12", "bid_to_cover_ratio": 2.53, "high_yield": null}, "20-Year": {"auction_date": "2026-08-19", "bid_to_cover_ratio": 2.53, "high_yield": null}, "30-Year": {"auction_date": "2026-08-13", "bid_to_cover_ratio": 2.39, "high_yield": null}}
- `16:37:43` treasury-api {"rawPath": "/auctions/current"} -> 200  | {"statusCode": 200, "headers": {"Content-Type": "application/json"}, "body": "{\"latest_auction\": {\"auction_date\": \"2026-10-08\", \"security_type\": \"Note\", \"security_term\": \"29-Year 10-Month\", \"cusip\": \"912810UW6\", \"bid_to_cover_ratio\": 2.54, \"total_accepted\": 22522535300.0, \"total_tendered\": 56435875700.0, \"high_rate\": 5.618, \"median_rate\": 5.567, \"low_rate\": 5.0, \"inv
- `16:37:43`    log: 
- `16:37:43` ✅ treasury-api /auctions/current answers real data: {"latest_auction": {"auction_date": "2026-10-08", "security_type": "Note", "security_term": "29-Year 10-Month", "cusip": "912810UW6", "bid_to_cover_ratio": 2.54, "total_accepted": 22522535300.0, "total_tendered": 56435875700.0, "high_rate": 5.618, "median_rate": 5.567, "low_rate": 5.0, "investment_rate": 0, "price": 0, "alloted_at_high": 0.0, "yield_range": 61.8, "primary_dealer_pct": 6.62, "indirect_bidder_pct": 70.45, "direct_bidder_pct": 20.35, "foreign_participation": 70.45, "tail": 5.1, "ab
- `16:37:47` treasury-api Function URL /auctions/current: HTTP 200 {"latest_auction": {"auction_date": "2026-10-08", "security_type": "Note", "security_term": "29-Year 10-Month", "cusip": "912810UW6", "bid_to_cover_ratio": 2.54, "total_accepted": 22522535300.0, "total_tendered": 56435875700.0, "high_rate": 5.618, "median_rate": 5.567, "low_rate": 5.0, "investment_r
- `16:37:50` macro-financial-intelligence {} -> 200  | {"statusCode": 200, "body": "{\"status\": \"success\", \"key\": \"daily_briefs/2026-11-02_brief.json\"}"}
- `16:37:50`    log: 
- `16:37:50` ✅ macro brief: {"ts": "2026-11-02T16:37:50.575073+00:00", "fails": {"current_b": 1883.52, "avg_b": 1910.01, "z": -1.77, "alert": "NORMAL", "trend": "DOWN", "points": 13}, "inds": {"fed_funds": {"val": 3.88, "date": "2026-10-29"}, "curve": {"val": 0.38, "date": "2026-10-30"}, "t10y": {"val": 4.98, "date": "2026-10-29"}, "t2y": {"val": 4.61, "date": "2026-10-29"}}, "alert": "NORMAL", "series": {"fails": "WSHOMCB - Fed MBS holdings, Wednesday level, $B (90-day z-score drives alert)", "fed_funds": "DFF", "curve": "T10Y2Y", "t10y": "DGS10", "t2y": "DGS2"}, "source": "FRED, Federal Reserve Bank of St. Louis", "version": "2.0.1"}
- `16:37:51` ✅ macro Function URL page: HTTP 200, 3010 chars, honest labels True | text: Macro Financial Intelligence 📊 Macro Financial Intelligence Daily read of FRED series (Federal Reserve Bank of St. Louis) Alert Level NORMAL Fed MBS holdings, 90-day z-score 🏦 Fed MBS holdings (FRED WSHOMCB, $B) { "current_b": 1883.52, "avg_b": 1910.01, "z": -1.77, "alert": "NORMAL", "trend": "DOWN", "points": 13 } 📈 Key Market Indicators { "fed_funds": { "val": 3.88, "date": "2026-10-29" }, "curve": { "val": 0.38, "date": "2026-10-30" }, "t10y": { "val": 4.98, "date": "2026-10-29" }, "t2y": { "val": 4.61, "date": "2026-10-29" } } Last Updated: 2026-11-02T16:37:50.575073+00:00 🔄 Refresh Data
- `16:37:51` ✅ ops 6714 complete
