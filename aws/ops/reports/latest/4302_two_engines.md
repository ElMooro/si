# ops 4302 -- rehypothecation + trend-reversal, live

**Status:** success  
**Duration:** 10.7s  
**Finished:** 2026-08-03T00:02:46+00:00  

## Log
## 1. treasury-rehypo

- `00:02:36` function: deployed
- `00:02:44` run: {"ok": true, "composite": null, "band": null, "legs": [], "missing": ["ofr: gcf/tri unresolved from candidates", "fred: HTTP Error 400: Bad Request"]}
- `00:02:44` COMPOSITE None (None) · legs: [] · missing: ['ofr: gcf/tri unresolved from candidates', 'fred: HTTP Error 400: Bad Request']
- `00:02:44` catalog picks: {'fails': 6, 'sec_in': 0, 'sec_out': 0, 'net_pos': 0}
- `00:02:44` schedule: present
## 2. trend-reversal

- `00:02:44` function: deployed
- `00:02:46` run: {"ok": true, "n": 0, "hot": 0, "top": []}
- `00:02:46` universe 0 · hot(>=30) 0 · errors ['SPY:HTTP Error 401: Unauthorized', 'IWM:HTTP Error 401: Unauthorized', 'EFA:HTTP Error 401: Unauthorized', 'EEM:HTTP Error 401: Unauthorized', 'HYG:HTTP Error 401: Unauthorized', 'IEF:HTTP Error 401: Unauthorized', 'GLD:HTTP Error 401: Unauthorized', 'SLV:HTTP Error 401: Unauthorized', 'DBC:HTTP Error 401: Unauthorized', 'VNQ:HTTP Error 401: Unauthorized', 'MSFT:HTTP Error 401: Unauthorized', 'TSM:HTTP Error 401: Unauthorized', 'MU:HTTP Error 401: Unauthorized', 'NVO:HTTP Error 401: Unauthorized', 'GILD:HTTP Error 401: Unauthorized', 'NVDA:HTTP Error 401: Unauthorized', 'AMAT:HTTP Error 401: Unauthorized', 'PLTR:HTTP Error 401: Unauthorized', 'AN:HTTP Error 401: Unauthorized', 'ADBE:HTTP Error 401: Unauthorized', 'BSX:HTTP Error 401: Unauthorized', 'EXE:HTTP Error 401: Unauthorized', 'META:HTTP Error 401: Unauthorized', 'AMZN:HTTP Error 401: Unauthorized']
- `00:02:46` schedule: present
## 3. desk v2.3.3 -- both wired + RRG retry

## RESULT

- `00:02:46` ✗   rehypo legs 0 < 3 (missing=['ofr: gcf/tri unresolved from candidates', 'fred: HTTP Error 400: Bad Request'])
- `00:02:46` ✗   reversal universe 0 < 10
