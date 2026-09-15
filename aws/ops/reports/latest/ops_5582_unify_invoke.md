# ops 5582 — invoke unify engines (already deployed)

**Status:** success  
**Duration:** 354.1s  
**Finished:** 2026-09-15T16:12:37+00:00  

## Log
## Event (long)

- `16:06:44` ✅ justhodl-13f-positions Event status=202
- `16:06:44` ✅ justhodl-flow-lookthrough Event status=202
- `16:06:44` ✅ justhodl-earnings-tracker Event status=202
- `16:06:44` ✅ justhodl-financial-secretary Event status=202
## RequestResponse (short)

- `16:06:50` ✅ justhodl-estimate-revisions status=200 body={"statusCode": 200, "body": "{\"status\": \"LIVE\", \"n_tracked\": 403, \"n_fmp_enriched\": 266, \"n_with_history\": 403, \"n_up\": 12, \"n_down\": 21, \"n_strength_leaders\": 40, \"n_picks\": 15, \"elapsed_s\": 4.2}"}
- `16:06:51` ✅ justhodl-boom-radar status=200 body={"statusCode": 200, "body": "{\"dimensions_loaded\": {\"BEAT\": 10, \"ANALYST\": 59, \"ESTIMATE\": 65, \"FLOW\": 3134, \"SQUEEZE\": 11, \"BREAKOUT\": 5}, \"n_scanned\": 3176, \"n_2way\": 36, \"n_3way\": 2, \"n_4way_plus\": 0, \"n_picks\": 2
- `16:06:57` ✅ justhodl-best-ideas status=200 body={"statusCode": 200, "body": "{\"ok\": true, \"n_total\": 177, \"titans\": 45, \"high\": 49}"}
- `16:07:04` ✅ justhodl-master-ranker status=200 body={"statusCode": 200, "body": "{\"ok\": true, \"n_tickers\": 25, \"n_macro\": 10, \"n_tier_3_plus\": 189, \"n_tier_5_plus\": 72, \"regime\": \"SLOWING\", \"duration_s\": 5.41}"}
- `16:07:05` ✅ justhodl-massive-signals status=200 body={"statusCode": 200, "body": "{\"ok\": true, \"tickers\": 40, \"top_prepump\": 26, \"fut_ok\": false}"}
## S3 poll (13f-desk + by_ticker + boom 1.2)

- `16:07:06` poll 13f-desk.json=miss 13f-by-ticker.json=miss flow-lookthrough.json=ok estimate-revisions.json=ok boom-radar.json=ok
- `16:07:27` poll 13f-desk.json=miss 13f-by-ticker.json=miss flow-lookthrough.json=ok estimate-revisions.json=ok boom-radar.json=ok
- `16:07:47` poll 13f-desk.json=miss 13f-by-ticker.json=miss flow-lookthrough.json=ok estimate-revisions.json=ok boom-radar.json=ok
- `16:08:08` poll 13f-desk.json=miss 13f-by-ticker.json=miss flow-lookthrough.json=ok estimate-revisions.json=ok boom-radar.json=ok
- `16:08:29` poll 13f-desk.json=miss 13f-by-ticker.json=miss flow-lookthrough.json=ok estimate-revisions.json=ok boom-radar.json=ok
- `16:08:49` poll 13f-desk.json=miss 13f-by-ticker.json=miss flow-lookthrough.json=ok estimate-revisions.json=ok boom-radar.json=ok
- `16:09:10` poll 13f-desk.json=miss 13f-by-ticker.json=miss flow-lookthrough.json=ok estimate-revisions.json=ok boom-radar.json=ok
- `16:09:31` poll 13f-desk.json=miss 13f-by-ticker.json=miss flow-lookthrough.json=ok estimate-revisions.json=ok boom-radar.json=ok
- `16:09:51` poll 13f-desk.json=miss 13f-by-ticker.json=miss flow-lookthrough.json=ok estimate-revisions.json=ok boom-radar.json=ok
- `16:10:12` poll 13f-desk.json=miss 13f-by-ticker.json=miss flow-lookthrough.json=ok estimate-revisions.json=ok boom-radar.json=ok
- `16:10:32` poll 13f-desk.json=miss 13f-by-ticker.json=miss flow-lookthrough.json=ok estimate-revisions.json=ok boom-radar.json=ok
- `16:10:53` poll 13f-desk.json=miss 13f-by-ticker.json=miss flow-lookthrough.json=ok estimate-revisions.json=ok boom-radar.json=ok
- `16:11:14` poll 13f-desk.json=miss 13f-by-ticker.json=miss flow-lookthrough.json=ok estimate-revisions.json=ok boom-radar.json=ok
- `16:11:34` poll 13f-desk.json=miss 13f-by-ticker.json=miss flow-lookthrough.json=ok estimate-revisions.json=ok boom-radar.json=ok
- `16:11:55` poll 13f-desk.json=miss 13f-by-ticker.json=miss flow-lookthrough.json=ok estimate-revisions.json=ok boom-radar.json=ok
- `16:12:16` poll 13f-desk.json=miss 13f-by-ticker.json=miss flow-lookthrough.json=ok estimate-revisions.json=ok boom-radar.json=ok
- `16:12:36` poll 13f-desk.json=ok 13f-by-ticker.json=ok flow-lookthrough.json=ok estimate-revisions.json=ok boom-radar.json=ok
## Harvest shape (counts only)

- `16:12:37` ✅ data/13f-desk.json bytes=381456 lm=2026-09-15T16:12:20+00:00 by_fund_n=18 most_bought_n=25 accumulating_n=15
- `16:12:37` ✅ data/13f-by-ticker.json bytes=1648708 lm=2026-09-15T16:12:20+00:00 tickers_n=7138
- `16:12:37` ✅ data/flow-lookthrough.json bytes=128476 lm=2026-09-15T00:11:05+00:00
- `16:12:37` ✅ data/estimate-revisions.json bytes=221882 lm=2026-09-15T16:06:51+00:00 by_ticker_n=266 AAPL=no upward_revisions_n=12
- `16:12:37` ✅ data/boom-radar.json bytes=9142 lm=2026-09-15T16:06:52+00:00 version=1.2.0 gated_off=['ESTIMATE', 'BREAKOUT'] top_picks_n=2 methodology_n=5
- `16:12:37` ✅ elapsed_s=354.1
