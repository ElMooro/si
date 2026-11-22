# ops 6921 - the dollar-shortage document, dry run (nothing written), 2026-11-22T18:59+00:00

**Status:** success  
**Duration:** 1.9s  
**Finished:** 2026-11-22T18:59:13+00:00  

## Data

| bytes | failed_checks | measures | observations | seconds |
|---|---|---|---|---|
| 219119 | 0 | 3 | 10051 | 0.8 |

## Log
## 1. The run

- `18:59:12` receipt: {"published": true, "generated_at": "2026-11-22T18:59:12.299892+00:00", "desk_generated_at": "2026-11-22T16:39:59.412623+00:00", "coverage": {"measures": 3, "observations": 10051, "status": "measured"}, "latest": {"AUX_EURUSD": [1.099, "2026-11-20"], "AUX_FX_EA": [20.193, "2026-11-13"], "AUX_FX_NONEA": [521.892, "2026-11-13"]}, "seconds": 0.8}
- `18:59:12` 0.8s | read from the bucket: ['ciss-sovereign.json', '84b666b398336f6dacbf82bf61f7', '9284342b95606f5b57be9b0ba898', 'cd31e81c2b30ff9bcb94f96cadea'] | kept in memory, not written: ['data/ciss-dollar-shortage.json']
- `18:59:12` document ciss-dollar-shortage.v1 1.0.0 | 219119 bytes, 53476 gzip | stands on the desk document of 2026-11-22T16:39
## 2. The three measures

- `18:59:12` AUX_EURUSD    EUR/USD reference rate | 1.099 usd on 2026-11-20 | 7141 observations 1999-01-04 .. 2026-11-20 (the desk's history keeps 1455) | high [1.599, '2008-07-15'], low [0.8252, '2000-10-26']
- `18:59:12`               changes over {"7 days": -0.0062, "30 days": -0.0244, "365 days": -0.0524} | the desk's own: {"12m": -0.0524, "1m": -0.0225, "1w": -0.0062, "3m": -0.0691}
- `18:59:12`               The ECB's euro foreign exchange reference rate: US dollars for one euro, set each working day. A falling rate is a rising dollar.
- `18:59:12` AUX_FX_EA     Claims on euro-area residents in foreign currency | 20.193 eur_bn on 2026-11-13 | 1455 observations 1999-01-01 .. 2026-11-13 (the desk's history keeps 1455) | high [248.854, '2008-12-05'], low [5.255, '1999-01-08']
- `18:59:12`               changes over {"7 days": 0.411, "30 days": -1.114, "365 days": -2.102} | the desk's own: {"12m": -2.102, "1m": -1.114, "1w": 0.411, "3m": -0.37}
- `18:59:12`               Asset item 3 of the Eurosystem's weekly financial statement, "claims on euro area residents denominated in foreign currency": what the Eurosystem has lent in foreign currency, mostly US dollars, to euro-area banks. Its large rises were the ECB's dollar operations of 2008, 2011-12 and 2020.
- `18:59:12` AUX_FX_NONEA  Claims on non-euro-area residents in foreign currency | 521.892 eur_bn on 2026-11-13 | 1455 observations 1999-01-01 .. 2026-11-13 (the desk's history keeps 1455) | high [552.989, '2022-11-04'], low [131.15, '2008-09-19']
- `18:59:12`               changes over {"7 days": -1.6, "30 days": -2.574, "365 days": 27.316} | the desk's own: {"12m": 27.316, "1m": -2.574, "1w": -1.6, "3m": -3.3}
- `18:59:12`               Asset item 2 of the Eurosystem's weekly financial statement, "claims on non-euro area residents denominated in foreign currency": balances with banks, security investments and loans abroad in foreign currency, and receivables from the IMF.
- `18:59:12` foreign-currency lending to euro-area banks, highest readings: 248.854 bn on 2008-12-05 | 153.774 bn on 2020-05-08 | 142.13 bn on 2009-04-10 | 100.629 bn on 2012-02-10 | 61.288 bn on 2009-08-14
- `18:59:12`   source eurusd           EXR/D.USD.EUR.SP00.A | 7141 observations | received 2026-11-22T16:39 | 84b666b398336f6dacbf
- `18:59:12`   source fx_claims_ea     ILM/W.U2.C.A030000.U2.Z06 | 1455 observations | received 2026-11-22T16:39 | 9284342b95606f5b57be
- `18:59:12`   source fx_claims_nonea  ILM/W.U2.C.A020000.U4.Z06 | 1455 observations | received 2026-11-22T16:39 | cd31e81c2b30ff9bcb94
## 3. The worker the Dollar desk's cards read through

- `18:59:13`   /data/ciss-sovereign.json -> HTTP 200, application/json, allow-origin *, 820800 bytes, JSON True
- `18:59:13`   /data/ciss-dollar-shortage.json -> HTTPError: HTTP Error 403: Forbidden
- `18:59:13` ✅ the dollar-shortage document compiles from the retained ECB responses and equals the desk document; nothing was written
