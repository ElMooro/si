# ops 6768 - live-data freshness probe 2026-11-10T14:54+00:00 (09:54 ET)

**Status:** success  
**Duration:** 14.0s  
**Finished:** 2026-11-10T14:54:27+00:00  

## Log
## 1. the chart's bar lane: Worker /ohlc (warehouse) and the forming-session tail

- `14:54:15` SPY  /ohlc: HTTP 200 source warehouse X-Source warehouse count 8503 | last bar 2026-11-09T14:30+00:00 close 752.69 | warehouse_key data/warm/tv-bars/universe/US__SPY.json.gz
- `14:54:15` SPY  /ohlc tail=1: HTTP 200 source forming-session tail bars [('2026-11-09T05:00+00:00', 752.69), ('2026-11-10T05:00+00:00', 747.97)]
- `14:54:16` QQQ  /ohlc: HTTP 200 source warehouse X-Source warehouse count 6961 | last bar 2026-11-09T14:30+00:00 close 679.35 | warehouse_key data/warm/tv-bars/universe/US__QQQ.json.gz
- `14:54:16` QQQ  /ohlc tail=1: HTTP 200 source forming-session tail bars [('2026-11-09T05:00+00:00', 679.35), ('2026-11-10T05:00+00:00', 672.59)]
- `14:54:18` AAPL /ohlc: HTTP 200 source warehouse X-Source warehouse count 11739 | last bar 2026-11-09T14:30+00:00 close 350.69 | warehouse_key data/warm/tv-bars/universe/US__AAPL.json.gz
- `14:54:18` AAPL /ohlc tail=1: HTTP 200 source forming-session tail bars [('2026-11-09T05:00+00:00', 350.69), ('2026-11-10T05:00+00:00', 350.985)]
- `14:54:19` TLT  /ohlc: HTTP 200 source warehouse X-Source warehouse count 6110 | last bar 2026-11-09T14:30+00:00 close 81.96 | warehouse_key data/warm/tv-bars/universe/US__TLT.json.gz
- `14:54:19` TLT  /ohlc tail=1: HTTP 200 source forming-session tail bars [('2026-11-09T05:00+00:00', 81.96), ('2026-11-10T05:00+00:00', 81.085)]
- `14:54:21` GLD  /ohlc: HTTP 200 source warehouse X-Source warehouse count 5528 | last bar 2026-11-09T14:30+00:00 close 379.53 | warehouse_key data/warm/tv-bars/universe/US__GLD.json.gz
- `14:54:21` GLD  /ohlc tail=1: HTTP 200 source forming-session tail bars [('2026-11-09T05:00+00:00', 379.53), ('2026-11-10T05:00+00:00', 370.55)]
## 2. the desks' quote lane: Worker /quotes (Polygon snapshot)

- `14:54:21` /quotes HTTP 200 X-Cache MISS ts 1794322461557 (0.0 min ago)
- `14:54:21`   AAPL price 350.97 change 0.28000000000002956 (0.08%) prevClose 350.69 dayClose 350.985 open 351.35 high 353.87 low 350 vol 2801131 | lastTrade None min ago | updated 15.0 min ago
- `14:54:21`   SPY  price 748.034 change -4.656000000000063 (-0.62%) prevClose 752.69 dayClose 747.97 open 747.94 high 749.25 low 747.05 vol 3845176 | lastTrade None min ago | updated 15.0 min ago
- `14:54:21`   QQQ  price 672.639 change -6.711000000000013 (-0.99%) prevClose 679.35 dayClose 672.59 open 672.54 high 674.39 low 669.92 vol 5238439 | lastTrade None min ago | updated 15.0 min ago
- `14:54:21`   GLD  price 370.55 change -8.979999999999961 (-2.37%) prevClose 379.53 dayClose 370.55 open 371.375 high 371.88 low 370.55 vol 1110659 | lastTrade None min ago | updated 15.0 min ago
- `14:54:21`   TLT  price 81.085 change -0.875 (-1.07%) prevClose 81.96 dayClose 81.085 open 81.138 high 81.165 low 81.07 vol 3184851 | lastTrade None min ago | updated 15.0 min ago
## 3. the banks in S3

- `14:54:21` polygon-full manifest: last_session None generated None sessions 1308
- `14:54:22`   SPY  data/warm/tv-bars/universe/US__SPY.json.gz: modified 2026-11-09T22:55+00:00 | 8503 rows | last 2026-11-09T14:30+00:00 close 752.69 | span None source mixed-bank:unqualified
- `14:54:22`   SPY  data/warm/us-equities-daily/SPY.json.gz: NoSuchKey
- `14:54:22`   SPY  data/warm/polygon-full/eod/SPY.json.gz: modified 2026-11-10T03:11+00:00 | 1307 rows | last 2026-11-09T21:00+00:00 close 752.69 | span None source polygon grouped-daily (justhodl-polygon-full), transposed by justhodl-polygon-eod v1.0.0
- `14:54:22`   QQQ  data/warm/tv-bars/universe/US__QQQ.json.gz: modified 2026-11-10T03:54+00:00 | 6961 rows | last 2026-11-09T14:30+00:00 close 679.35 | span None source mixed-bank:unqualified
- `14:54:22`   QQQ  data/warm/us-equities-daily/QQQ.json.gz: NoSuchKey
- `14:54:22`   QQQ  data/warm/polygon-full/eod/QQQ.json.gz: modified 2026-11-10T03:10+00:00 | 1307 rows | last 2026-11-09T21:00+00:00 close 679.35 | span None source polygon grouped-daily (justhodl-polygon-full), transposed by justhodl-polygon-eod v1.0.0
- `14:54:22`   AAPL data/warm/tv-bars/universe/US__AAPL.json.gz: modified 2026-11-09T22:54+00:00 | 11739 rows | last 2026-11-09T14:30+00:00 close 350.69 | span None source mixed-bank:unqualified
- `14:54:22`   AAPL data/warm/us-equities-daily/AAPL.json.gz: NoSuchKey
- `14:54:23`   AAPL data/warm/polygon-full/eod/AAPL.json.gz: modified 2026-11-10T03:10+00:00 | 1307 rows | last 2026-11-09T21:00+00:00 close 350.69 | span None source polygon grouped-daily (justhodl-polygon-full), transposed by justhodl-polygon-eod v1.0.0
- `14:54:23`   TLT  data/warm/tv-bars/universe/US__TLT.json.gz: modified 2026-11-10T03:55+00:00 | 6110 rows | last 2026-11-09T14:30+00:00 close 81.96 | span None source mixed-bank:unqualified
- `14:54:23`   TLT  data/warm/us-equities-daily/TLT.json.gz: NoSuchKey
- `14:54:23`   TLT  data/warm/polygon-full/eod/TLT.json.gz: modified 2026-11-10T03:11+00:00 | 1307 rows | last 2026-11-09T21:00+00:00 close 81.96 | span None source polygon grouped-daily (justhodl-polygon-full), transposed by justhodl-polygon-eod v1.0.0
- `14:54:23`   GLD  data/warm/tv-bars/universe/US__GLD.json.gz: modified 2026-11-10T03:53+00:00 | 5528 rows | last 2026-11-09T14:30+00:00 close 379.53 | span None source mixed-bank:unqualified
- `14:54:23`   GLD  data/warm/us-equities-daily/GLD.json.gz: NoSuchKey
- `14:54:23`   GLD  data/warm/polygon-full/eod/GLD.json.gz: modified 2026-11-10T03:11+00:00 | 1307 rows | last 2026-11-09T21:00+00:00 close 379.53 | span None source polygon grouped-daily (justhodl-polygon-full), transposed by justhodl-polygon-eod v1.0.0
- `14:54:24` data/strength.json: version 1.0.12 as_of 2026-11-09 generated 2026-11-10T03:40:10+00:00 SPY px 752.69
- `14:54:24` justhodl-tv-bars: modified 2026-11-03T06:07:47 timeout 600 memory 1024
- `14:54:24` justhodl-polygon-eod: modified 2026-10-25T22:08:51 timeout 900 memory 3008
- `14:54:24` justhodl-polygon-full: modified 2026-11-03T06:00:28 timeout 780 memory 1024
- `14:54:25` justhodl-symdir: modified 2026-11-03T06:06:16 timeout 900 memory 6144
## 4. Polygon itself (SSM key, never printed)

- `14:54:25` marketstatus/now: HTTP 200 market open exchanges {"nasdaq": "open", "nyse": "open", "otc": "open"} serverTime 2026-11-10T09:54:25-05:00
- `14:54:25` snapshot SPY: HTTP 200 status OK | updated 15.0 min ago | lastTrade p None t None min ago | min c 748.034 t 16.4 min ago | day o 747.94 h 749.25 l 747.05 c 747.97 v 3845176.0 | prevDay c 752.69 | todaysChangePerc -0.5128273259907815
- `14:54:26` snapshot AAPL: HTTP 200 status OK | updated 15.0 min ago | lastTrade p None t None min ago | min c 350.97 t 16.4 min ago | day o 351.35 h 353.87 l 350 c 350.985 v 2801131.0 | prevDay c 350.69 | todaysChangePerc 0.12546693661068115
- `14:54:26` SPY 5-minute aggs today: HTTP 200 resultsCount 0 | latest []
- `14:54:26` SPY daily agg today (the forming bar): HTTP 200 [('2026-11-10T05:00+00:00', 747.94, 747.97, 3845176.630302)]
- `14:54:26` /v2/last/trade/SPY: HTTP 403 status NOT_AUTHORIZED (entitlement) {"status": "NOT_AUTHORIZED", "request_id": "7c69c404933689cec27990c692657493", "message": "You are not entitled to this data. Please upgrade your plan at https:
- `14:54:27` ALL-tickers snapshot: HTTP 200 count 13254 in 0.8s | updated age min: newest 15.0 median 15.5 oldest 354.3 | with lastTrade 0 | with day.c 9303
- `14:54:27` ✅ ops 6768 complete (read-only)
