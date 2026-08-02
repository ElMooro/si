# ops 4302 -- rehypothecation + trend-reversal, live

**Status:** success  
**Duration:** 17.8s  
**Finished:** 2026-08-02T23:57:51+00:00  

## Log
## 1. treasury-rehypo

- `23:57:39` function: self-created
- `23:57:43` run: {"ok": true, "composite": null, "band": null, "legs": [], "missing": ["ofr: gcf/tri unresolved from candidates", "fred: HTTP Error 400: Bad Request"]}
- `23:57:43` COMPOSITE None (None) · legs: [] · missing: ['ofr: gcf/tri unresolved from candidates', 'fred: HTTP Error 400: Bad Request']
- `23:57:43` catalog picks: {'fails': 6, 'sec_in': 0, 'sec_out': 0, 'net_pos': 0}
- `23:57:44` schedule: created
## 2. trend-reversal

- `23:57:49` function: self-created
- `23:57:51` run: {"ok": true, "n": 0, "hot": 0, "top": []}
- `23:57:51` universe 0 · hot(>=30) 0 · errors ['SPY:HTTP Error 401: Unauthorized', 'IWM:HTTP Error 401: Unauthorized', 'EFA:HTTP Error 401: Unauthorized', 'EEM:HTTP Error 401: Unauthorized', 'HYG:HTTP Error 401: Unauthorized', 'IEF:HTTP Error 401: Unauthorized', 'GLD:HTTP Error 401: Unauthorized', 'SLV:HTTP Error 401: Unauthorized', 'DBC:HTTP Error 401: Unauthorized', 'VNQ:HTTP Error 401: Unauthorized', 'MSFT:HTTP Error 401: Unauthorized', 'TSM:HTTP Error 401: Unauthorized', 'MU:HTTP Error 401: Unauthorized', 'NVO:HTTP Error 401: Unauthorized', 'GILD:HTTP Error 401: Unauthorized', 'NVDA:HTTP Error 401: Unauthorized', 'AMAT:HTTP Error 401: Unauthorized', 'PLTR:HTTP Error 401: Unauthorized', 'AN:HTTP Error 401: Unauthorized', 'ADBE:HTTP Error 401: Unauthorized', 'BSX:HTTP Error 401: Unauthorized', 'EXE:HTTP Error 401: Unauthorized', 'META:HTTP Error 401: Unauthorized', 'AMZN:HTTP Error 401: Unauthorized']
- `23:57:51` schedule: created
## 3. desk v2.3.3 -- both wired + RRG retry

## RESULT

- `23:57:51` ✗   rehypo legs 0 < 3 (missing=['ofr: gcf/tri unresolved from candidates', 'fred: HTTP Error 400: Bad Request'])
- `23:57:51` ✗   reversal universe 0 < 10
