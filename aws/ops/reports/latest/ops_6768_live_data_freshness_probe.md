# ops 6768 - live-data freshness probe 2026-11-10T14:52+00:00 (09:52 ET)

**Status:** success  
**Duration:** 6.6s  
**Finished:** 2026-11-10T14:52:25+00:00  

## Log
## 1. the chart's bar lane: Worker /ohlc (warehouse) and the forming-session tail

- `14:52:19` SPY  /ohlc: HTTP 503 source None X-Source None count None | last bar None close None | warehouse_key None
- `14:52:19` SPY  /ohlc tail=1: HTTP 404 source None bars []
- `14:52:19` QQQ  /ohlc: HTTP 404 source None X-Source None count None | last bar None close None | warehouse_key None
- `14:52:19` QQQ  /ohlc tail=1: HTTP 404 source None bars []
- `14:52:19` AAPL /ohlc: HTTP 404 source None X-Source None count None | last bar None close None | warehouse_key None
- `14:52:20` AAPL /ohlc tail=1: HTTP 404 source None bars []
- `14:52:20` TLT  /ohlc: HTTP 404 source None X-Source None count None | last bar None close None | warehouse_key None
- `14:52:20` TLT  /ohlc tail=1: HTTP 404 source None bars []
- `14:52:20` GLD  /ohlc: HTTP 404 source None X-Source None count None | last bar None close None | warehouse_key None
- `14:52:20` GLD  /ohlc tail=1: HTTP 404 source None bars []
## 2. the desks' quote lane: Worker /quotes (Polygon snapshot)

- `14:52:20` /quotes HTTP 404 X-Cache MISS ts None (None min ago)
- `14:52:20`   body: {"_text": "<!DOCTYPE html>\n<html>\n  <head>\n    <meta http-equiv=\"Content-type\" content=\"text/html; charset=utf-8\">\n    <meta http-equiv=\"Content-Security-Policy\" content=\"default-src 'none'; style-src 'unsafe-inline'; img-src data:; connect-src 'self'\">\n    <title>Page not found &middot; GitHub Pages</title>\n "}
## 3. the banks in S3

- `14:52:20` polygon-full manifest: last_session None generated None sessions 1308
- `14:52:21`   SPY  data/warm/tv-bars/universe/US__SPY.json.gz: modified 2026-11-09T22:55+00:00 | 8503 rows | last 2026-11-09T14:30+00:00 close 752.69 | span None source mixed-bank:unqualified
- `14:52:21`   SPY  data/warm/us-equities-daily/SPY.json.gz: NoSuchKey
- `14:52:21`   SPY  data/warm/polygon-full/eod/SPY.json.gz: modified 2026-11-10T03:11+00:00 | 1307 rows | last 2026-11-09T21:00+00:00 close 752.69 | span None source polygon grouped-daily (justhodl-polygon-full), transposed by justhodl-polygon-eod v1.0.0
- `14:52:21`   QQQ  data/warm/tv-bars/universe/US__QQQ.json.gz: modified 2026-11-10T03:54+00:00 | 6961 rows | last 2026-11-09T14:30+00:00 close 679.35 | span None source mixed-bank:unqualified
- `14:52:21`   QQQ  data/warm/us-equities-daily/QQQ.json.gz: NoSuchKey
- `14:52:21`   QQQ  data/warm/polygon-full/eod/QQQ.json.gz: modified 2026-11-10T03:10+00:00 | 1307 rows | last 2026-11-09T21:00+00:00 close 679.35 | span None source polygon grouped-daily (justhodl-polygon-full), transposed by justhodl-polygon-eod v1.0.0
- `14:52:21`   AAPL data/warm/tv-bars/universe/US__AAPL.json.gz: modified 2026-11-09T22:54+00:00 | 11739 rows | last 2026-11-09T14:30+00:00 close 350.69 | span None source mixed-bank:unqualified
- `14:52:21`   AAPL data/warm/us-equities-daily/AAPL.json.gz: NoSuchKey
- `14:52:21`   AAPL data/warm/polygon-full/eod/AAPL.json.gz: modified 2026-11-10T03:10+00:00 | 1307 rows | last 2026-11-09T21:00+00:00 close 350.69 | span None source polygon grouped-daily (justhodl-polygon-full), transposed by justhodl-polygon-eod v1.0.0
- `14:52:22`   TLT  data/warm/tv-bars/universe/US__TLT.json.gz: modified 2026-11-10T03:55+00:00 | 6110 rows | last 2026-11-09T14:30+00:00 close 81.96 | span None source mixed-bank:unqualified
- `14:52:22`   TLT  data/warm/us-equities-daily/TLT.json.gz: NoSuchKey
- `14:52:22`   TLT  data/warm/polygon-full/eod/TLT.json.gz: modified 2026-11-10T03:11+00:00 | 1307 rows | last 2026-11-09T21:00+00:00 close 81.96 | span None source polygon grouped-daily (justhodl-polygon-full), transposed by justhodl-polygon-eod v1.0.0
- `14:52:22`   GLD  data/warm/tv-bars/universe/US__GLD.json.gz: modified 2026-11-10T03:53+00:00 | 5528 rows | last 2026-11-09T14:30+00:00 close 379.53 | span None source mixed-bank:unqualified
- `14:52:22`   GLD  data/warm/us-equities-daily/GLD.json.gz: NoSuchKey
- `14:52:22`   GLD  data/warm/polygon-full/eod/GLD.json.gz: modified 2026-11-10T03:11+00:00 | 1307 rows | last 2026-11-09T21:00+00:00 close 379.53 | span None source polygon grouped-daily (justhodl-polygon-full), transposed by justhodl-polygon-eod v1.0.0
- `14:52:22` data/strength.json: version 1.0.12 as_of 2026-11-09 generated 2026-11-10T03:40:10+00:00 SPY px 752.69
- `14:52:23` justhodl-tv-bars: modified 2026-11-03T06:07:47 timeout 600 memory 1024
- `14:52:23` justhodl-polygon-eod: modified 2026-10-25T22:08:51 timeout 900 memory 3008
- `14:52:23` justhodl-polygon-full: modified 2026-11-03T06:00:28 timeout 780 memory 1024
- `14:52:23` justhodl-symdir: modified 2026-11-03T06:06:16 timeout 900 memory 6144
## 4. Polygon itself (SSM key, never printed)

- `14:52:24` marketstatus/now: HTTP 200 market open exchanges {"nasdaq": "open", "nyse": "open", "otc": "open"} serverTime 2026-11-10T09:52:24-05:00
- `14:52:24` snapshot SPY: HTTP 200 status OK | updated 15.0 min ago | lastTrade p None t None min ago | min c 749.03 t 16.4 min ago | day o 747.94 h 749.16 l 747.05 c 749.115 v 3594789.0 | prevDay c 752.69 | todaysChangePerc -0.5300987126174134
- `14:52:24` snapshot AAPL: HTTP 200 status OK | updated 15.0 min ago | lastTrade p None t None min ago | min c 350.74 t 16.4 min ago | day o 351.35 h 353.87 l 350 c 350.67 v 2670219.0 | prevDay c 350.69 | todaysChangePerc 0.04850437708517232
- `14:52:24` SPY 5-minute aggs today: HTTP 200 resultsCount 0 | latest []
- `14:52:24` SPY daily agg today (the forming bar): HTTP 200 [('2026-11-10T05:00+00:00', 747.94, 749.115, 3594789.03019)]
- `14:52:25` /v2/last/trade/SPY: HTTP 403 status NOT_AUTHORIZED (entitlement) {"status": "NOT_AUTHORIZED", "request_id": "b570fd72f4ee4858be60151447261ca5", "message": "You are not entitled to this data. Please upgrade your plan at https:
- `14:52:25` ALL-tickers snapshot: HTTP 200 count 13254 in 0.7s | updated age min: newest 15.0 median 15.4 oldest 352.3 | with lastTrade 0 | with day.c 9200
- `14:52:25` ✅ ops 6768 complete (read-only)
