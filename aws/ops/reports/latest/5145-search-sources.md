# ops 5145 -- search bar shows every source; TradingView-only symbols resolve to the warehouse (re-run)

**Status:** failure  
**Duration:** 100.7s  
**Finished:** 2026-09-02T22:03:36+00:00  

## Error

```
SystemExit: 1
```

## Log
## S1 deploy symdir v1.5.0

- `22:01:55`   zip: 137382 bytes
## 1. Lambda

- `22:01:56`   Lambda exists — updating
- `22:01:59` ✅   ✓ updated justhodl-symdir
## S2 resolution + search

- `22:02:28`   TVC:US02MY         n=None first=None last=None via=None src=None err=no market feed for TVC:US02MY and no warehouse equivalent (RuntimeError: no bars alts=[{'id': 'fred:DGS2MO', 'note': 'US Treasury constant-maturity yield, daily (FRED H.15)'}]
- `22:02:42`   ECONOMICS:USINTR   n=866 first=1954-07-01 last=2026-08-01 via=fred:FEDFUNDS src=warehouse:fred-scoped/Interest_Rates (+1 obs tail from FRED, bank healed) err= alts=None
- `22:02:45`   TVC:DE10Y          n=842 first=1956-05-01 last=2026-06-01 via=fred:IRLTLT01DEM156N src=warehouse:fred-scoped/International_Data err= alts=None
- `22:02:48`   TVC:US03MY         n=18157 first=1954-01-04 last=2026-08-31 via=fred:DTB3 src=warehouse:fred-scoped/Interest_Rates err= alts=None
- `22:02:48`   search US02MY: facets=[{'provider': 'tv', 'provider_name': 'TradingView', 'n': 1}] rows=['TVC:US02MY'] tv sources=[]
- `22:02:48`   provider filter eurostat: rows=['eurostat:LFSA_URGANEDM', 'eurostat:LFST_R_LFU3RT', 'eurostat:LFSO_14LUNER', 'eurostat:LFSA_URGAEDDL', 'eurostat:LFST_R_LFUR2GAC'] facets=[('bls', 13005), ('fred', 12704), ('tv', 162), ('eurostat', 55), ('statcan', 20), ('worldbank', 19)]
## S3 live page

- `22:03:17`   dropdown: {"chips": [], "rows": [{"id": "TVC:US02MY", "src": null}]}
- `22:03:26`   TVC:US02MY chart: {"active": "TVC:US02MY", "meta": "SERIES \u00b7 loading full history\u2026", "loading": "TVC:US02MY has no market feed the warehouse can bank.Same data in your warehouse:fred:DGS2MO \u203a"}
- `22:03:35`   TVC:US10Y legend: {"legend": "TVC:US10Y O 4.78 H 4.81 L 4.77 C 4.80 +0.42% Vol \u2014"}
## verdict

- `22:03:36` ✗ TVC:US02MY did not resolve via fred:DGS2MO: via=None err=no market feed for TVC:US02MY and no warehouse equivalent (RuntimeError: no bars for TVC:US02MY (tv:all endpoints refused: data.tradingview.com/socket.io/webso)
- `22:03:36` ✗ TVC:DE10Y did not resolve via fred:IRLTLT01DEUM156N: via=fred:IRLTLT01DEM156N err=None
- `22:03:36` ✗ TVC:US03MY did not resolve via fred:DGS3MO: via=fred:DTB3 err=None
- `22:03:36` ✗ TVC:US02MY row lacks its warehouse source: {'id': 'TVC:US02MY', 'symbol': 'TVC:US02MY', 'name': 'US02MY (TVC)', 'provider': 'tv', 'provider_name': 'TradingView', 'kind': 'instrument', 'chartable': True, 'unit': None, 'freq': None, 'first': None, 'last': None, 'n': None, 'pop': 0.58, 'ex': 'TVC', 'type': 'unmapped', 'mkt': 'tv', 'src': '', 'score': 239.24, 'sources': []}
- `22:03:36` ✗ no facet chips in the dropdown
- `22:03:36` ✗ TVC:US02MY row has no source line: {'id': 'TVC:US02MY', 'src': None}
- `22:03:36` ✗ TVC:US02MY did not chart: {"active": "TVC:US02MY", "meta": "SERIES \u00b7 loading full history\u2026", "loading": "TVC:US02MY has no market feed the warehouse can bank.Same data in your warehouse:fred:DGS2MO \u203a"}
