# ops 4705 — isolate the real date format stream.ashx wants

**Status:** success  
**Duration:** 6.6s  
**Finished:** 2026-08-15T16:58:55+00:00  

## Log
- `16:58:48`   [YYYY-MM-DD narrow (2022-2023)] status=200 bytes=15 body=b'Dates not valid'
- `16:58:49`   [MM/DD/YYYY] status=200 bytes=15 body=b'Dates not valid'
- `16:58:50`   [MM-DD-YYYY] status=200 bytes=15 body=b'Dates not valid'
- `16:58:50`   [no d1, only d2] status=200 bytes=0 body=b''
- `16:58:51`   [span=10y (no explicit dates)] status=200 bytes=0 body=b''
- `16:58:52`   [span=max (no explicit dates)] status=200 bytes=0 body=b''
- `16:58:54`   [d1 only, YYYY-MM-DD, very recent] status=200 bytes=31395451 body=b'[{"iD":575552,"title":"Agricultural Commodities Updates: Oat Rises by 3.48%","description":"Top commodity gainers are Oat (3.48%) and Wheat (3.26%). Biggest losers are Rice (-1.37%) and Sugar (-1.25%).","url":"/commodities","author":"CALCULATOR","country":"Commodity","category":"Commodity","importan'
- `16:58:54` ✅     ^ looks like real data, not an error!
## verdict

- `16:58:55` ✅ found a working format: d1 only, YYYY-MM-DD, very recent
