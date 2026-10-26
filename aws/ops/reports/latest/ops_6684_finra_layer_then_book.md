# ops 6684 - FINRA chain: short-interest layer -> factor book v1.0.6 (one run each)

**Status:** success  
**Duration:** 493.5s  
**Finished:** 2026-10-26T21:07:50+00:00  

## Log
- `20:59:36` justhodl-short-interest deployed: 1024 MB / 600 s, modified 2026-10-26T20:58:29
## 1. justhodl-short-interest

- `20:59:36` justhodl-short-interest: accepted 202 (request cf6b44d1-22b2-43bc-938c-9ceab11bdb6f)
- `21:03:23` justhodl-short-interest: REPORT Duration: 195850.24 ms Max Memory Used: 382 MB Init Duration: 266.19 ms
- `21:03:23`    21:02:53 | [tickers-layer] skipped: ValueError
- `21:03:23` ⚠ data/short-interest-tickers.json: ClientError - the layer did not publish; the book join below will again find 0 FINRA tickers
## 2. justhodl-bank-factors v1.0.6 join

- `21:03:23` justhodl-bank-factors: accepted 202 (request c7b3679a-a142-45df-8a98-55dcc2a79cb3)
- `21:07:49` justhodl-bank-factors: REPORT Duration: 235654.68 ms Max Memory Used: 1137 MB Init Duration: 528.80 ms
- `21:07:49`    21:05:10 | [bank-factors] float 88206, short 3608, short-volume 1872, finra short 0, calendars 10091/4776 (106s)
- `21:07:49` manifest: version 1.0.6 updated 2026-10-26T21:03:24.500591+00:00 | coverage.finra_short 0 (polygon short 3608) | sources.finra_short {"tickers": 0} | as_of.finra-short-interest None
- `21:07:50` AAPL: short_source polygon short_pct_float 0.8289 | finra SI None dtc None change% None vs-polygon% None settlement None
- `21:07:50` GME: short_source None short_pct_float None | finra SI None dtc None change% None vs-polygon% None settlement None
- `21:07:50` TSLA: short_source None short_pct_float None | finra SI None dtc None change% None vs-polygon% None settlement None
- `21:07:50` SOFI: short_source None short_pct_float None | finra SI None dtc None change% None vs-polygon% None settlement None
- `21:07:50` ✅ ops 6684 complete
