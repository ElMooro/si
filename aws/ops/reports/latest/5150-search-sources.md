# ops 5150 -- Treasury par curve banked offline + Bundesbank sources; search chips (v1.6.1)

**Status:** failure  
**Duration:** 92.5s  
**Finished:** 2026-09-02T22:46:35+00:00  

## Error

```
SystemExit: 1
```

## Log
## S1 deploy symdir v1.5.0

- `22:45:03`   zip: 140133 bytes
## 1. Lambda

- `22:45:04`   Lambda exists — updating
- `22:45:07` ✅   ✓ updated justhodl-symdir
## S1b Treasury par curve: bank offline, daily schedule

- `22:45:12` ✅ schedule created: justhodl-symdir-ustbank weekdays 21:30 UTC
- `22:45:31`   ustbank: {"ok": true, "n_days": 9175, "first": "1990-01-02", "last": "2026-09-02", "elapsed_s": 18.3, "errors": null}
## S2 resolution + search

- `22:45:40`   direct ustpar:2M: n=1970 first=2018-10-16 last=2026-09-02 src=warehouse:treasury-par (home.treasury.gov daily_treasury_yield_curve) err=
- `22:45:41`   direct ustpar:10Y: n=9174 first=1990-01-02 last=2026-09-02 src=warehouse:treasury-par (home.treasury.gov daily_treasury_yield_curve) err=
- `22:45:42`   direct official-yields:de-10y-bbk: n=7377 first=1997-08-07 last=2026-08-31 src=warehouse:official-yields (Bundesbank BBSIS) err=
- `22:45:45`   TVC:US02MY         n=1970 first=2018-10-16 last=2026-09-02 via=ustpar:2M src=warehouse:treasury-par (home.treasury.gov daily_treasury_yield_curve) (TVC:US02MY → ustpar err= alts=None
- `22:45:46`   ECONOMICS:USINTR   n=866 first=1954-07-01 last=2026-08-01 via=fred:FEDFUNDS src=warehouse:fred-scoped/Interest_Rates (+1 obs tail from FRED, bank healed) err= alts=None
- `22:45:48`   TVC:DE10Y          n=842 first=1956-05-01 last=2026-06-01 via=fred:IRLTLT01DEM156N src=warehouse:fred-scoped/International_Data err= alts=None
- `22:45:50`   TVC:US03MY         n=18157 first=1954-01-04 last=2026-08-31 via=fred:DTB3 src=warehouse:fred-scoped/Interest_Rates err= alts=None
- `22:45:50`   search US02MY: facets=[{'provider': 'tv', 'provider_name': 'TradingView', 'n': 1}] rows=['TVC:US02MY'] tv sources=[{'id': 'ustpar:2M', 'provider': 'ustpar', 'provider_name': 'US Treasury', 'name': 'US Treasury par yield curve, daily (home.treasury.gov)', 'freq': 'D', 'first': '1990', 'last': None, 'ohlc': False, 'note': 'US Treasury par yield curve, daily (home.treasury.gov)', 'banked': True}]
- `22:45:50`   provider filter eurostat: rows=['eurostat:LFSA_URGANEDM', 'eurostat:LFST_R_LFU3RT', 'eurostat:LFSO_14LUNER', 'eurostat:LFSA_URGAEDDL', 'eurostat:LFST_R_LFUR2GAC'] facets=[('bls', 13005), ('fred', 12704), ('tv', 162), ('eurostat', 55), ('statcan', 20), ('worldbank', 19)]
## S3 live page

- `22:46:17`   page fetch /symsearch: {"facets": [{"provider": "tv", "provider_name": "TradingView", "n": 1}], "rows": [{"id": "TVC:US02MY", "sources": [{"id": "ustpar:2M", "provider": "ustpar", "provider_name": "US Treasury", "name": "US Treasury par yield curve, daily (home.treasury.gov)", "freq": "D", "first": "1990", "last": null, "ohlc": false, "note": "US Treasury par yield curve, daily (home.treasury.gov)", "banked": true}]}], "err": null} | HeaderSearch._facets=null
- `22:46:17`   dropdown: {"chips": [], "rows": [{"id": "TVC:US02MY", "src": null}]}
- `22:46:26`   TVC:US02MY chart: {"active": "TVC:US02MY", "meta": "US Treasury \u00b7 3.89 Percent +0.00% \u00b7 1,970 obs \u00b7 2018-10-16\u21922026-09-02 \u00b7 D \u00b7 warehouse", "loading": "Loading TVC:US02MY \u2014 full history\u2026"}
- `22:46:35`   TVC:US10Y legend: {"legend": "TVC:US10Y O 4.78 H 4.81 L 4.77 C 4.80 +0.42% Vol \u2014"}
## verdict

- `22:46:35` ✗ TVC:DE10Y did not resolve via official-yields:de-10y-bbk: via=fred:IRLTLT01DEM156N err=None
- `22:46:35` ✗ TVC:US03MY did not resolve via fred:DGS3MO: via=fred:DTB3 err=None
- `22:46:35` ✗ no facet chips in the dropdown
- `22:46:35` ✗ TVC:US02MY row has no source line: {'id': 'TVC:US02MY', 'src': None}
