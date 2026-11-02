# ops 6713 - Treasury FiscalData auctions_query: which form answers (read-only, public)

**Status:** success  
**Duration:** 7.0s  
**Finished:** 2026-11-02T16:17:08+00:00  

## Log
- `16:17:01` ⚠ current code: HTTP 400 {"error":"Invalid Query Param","message":"Invalid query parameter: Operator 'Invalid Filter Format' is not supported. For more information, please see the documentation."}
- `16:17:03` ✅ no null filter, limit: HTTP 200, 500 rows (total-count 2114, total-pages 5), 109 with all five auction fields; newest {"auction_date": "2026-10-22", "security_type": "Note", "security_term": "5-Year", "high_yield": "null", "bid_to_cover_ratio": "null"}
- `16:17:03`    fields present of the required: ['bid_to_cover_ratio', 'total_accepted', 'total_tendered', 'high_yield', 'avg_med_yield', 'security_type', 'security_term', 'auction_date'] | missing []
- `16:17:03`    newest complete: {"auction_date": "2026-10-08", "security_type": "Bond", "security_term": "29-Year 10-Month", "high_yield": "5.6180", "bid_to_cover_ratio": "2.540000", "total_tendered": "56435875700", "total_accepted": "22522535300", "avg_med_yield": "5.567000"}
- `16:17:04` ✅ no null filter, page[size]: HTTP 200, 500 rows (total-count 2114, total-pages 5), 109 with all five auction fields; newest {"auction_date": "2026-10-22", "security_type": "Note", "security_term": "5-Year", "high_yield": "null", "bid_to_cover_ratio": "null"}
- `16:17:04`    fields present of the required: ['bid_to_cover_ratio', 'total_accepted', 'total_tendered', 'high_yield', 'avg_med_yield', 'security_type', 'security_term', 'auction_date'] | missing []
- `16:17:04`    newest complete: {"auction_date": "2026-10-08", "security_type": "Bond", "security_term": "29-Year 10-Month", "high_yield": "5.6180", "bid_to_cover_ratio": "2.540000", "total_tendered": "56435875700", "total_accepted": "22522535300", "avg_med_yield": "5.567000"}
- `16:17:08` ✅ page[size] 10000: HTTP 200, 2114 rows (total-count 2114, total-pages 1), 476 with all five auction fields; newest {"auction_date": "2026-10-22", "security_type": "Note", "security_term": "5-Year", "high_yield": "null", "bid_to_cover_ratio": "null"}
- `16:17:08`    fields present of the required: ['bid_to_cover_ratio', 'total_accepted', 'total_tendered', 'high_yield', 'avg_med_yield', 'security_type', 'security_term', 'auction_date'] | missing []
- `16:17:08`    newest complete: {"auction_date": "2026-10-08", "security_type": "Bond", "security_term": "29-Year 10-Month", "high_yield": "5.6180", "bid_to_cover_ratio": "2.540000", "total_tendered": "56435875700", "total_accepted": "22522535300", "avg_med_yield": "5.567000"}
- `16:17:08` ✅ ops 6713 complete (read-only)
