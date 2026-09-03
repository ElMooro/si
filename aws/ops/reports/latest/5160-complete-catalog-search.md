# ops 5160 -- every first-class data.html dataset searchable

**Status:** success  
**Duration:** 690.1s  
**Finished:** 2026-09-03T05:09:16+00:00  

## Log
## S1 provider catalog -> 57-provider FTS index

- `04:57:46`   zip: 116842 bytes
## 1. Lambda

- `04:57:46`   Lambda exists — updating
- `04:57:49` ✅   ✓ updated justhodl-provider-catalog
- `05:04:41`   manifest providers=57 documents=816055 index=data/search/index/provider-search-20260903T045753Z-dff969035f18.sqlite.gz bytes=35662411
- `05:04:41`   entities indicator_bus=18741 tradingview_live=5569 coverage={'catalog_datasets': 811962, 'storage_objects': 1922543, 'storage_bytes': 530443675311, 'indexed_assets': 787654, 'indexed_entity_refs': 24310, 'indexed_series_refs': 4034, 'hierarchical_series': {'eurostat': 564204235, 'ecb': 3240832, 'access': 'tier1_prefix'}}
- `05:04:41`   sample provider=gdelt query='2003' id=gdelt:asset:12f5946134bdd2cb
## S2 symdir v1.9.0 -> native + warehouse merged search

- `05:04:42`   zip: 146603 bytes
## 1. Lambda

- `05:04:42`   Lambda exists — updating
- `05:04:45` ✅   ✓ updated justhodl-symdir
- `05:09:05`   symdir docs=1378332 elapsed=255.0 provider_shards={'providers': 57, 'docs': 57, 'manifest_docs': 816055, 'index': 'data/search/index/provider-search-20260903T045753Z-dff969035f18.sqlite.gz', 'generated_at': '2026-09-03T04:57:53+00:00', 's': 0.6}
- `05:09:15`   warm docs=1378332 warehouse_ready=True warehouse_error=None
- `05:09:15`   direct search rows=1 raw=1 more=False error=None
- `05:09:15`   entity search indicator={'id': 'indicator-bus:DXY', 'symbol': 'DXY', 'name': 'DXY', 'provider': 'indicator-bus', 'provider_name': 'Canonical Indicator Bus', 'kind': 'dataset', 'chartable': False, 'browse': False, 'browse_provider': None, 'raw': False, 'key': None, 'src': 'yahoo:DX-Y.NYB yahoo_5d', 'lookup_query': 'DXY', 'bytes': None, 'age_h': None, 'hot': True, 'catalog_kind': 'indicator_ref'} tv={'id': 'tradingview-vault-live:DXY', 'symbol': 'DXY', 'name': 'DXY', 'provider': 'tradingview-vault-live', 'provider_name': 'TradingView Vault (LIVE)', 'kind': 'dataset', 'chartable': False, 'browse': False, 'browse_provider': None, 'raw': False, 'key': None, 'src': 'fx yahoo:DX-Y.NYB yahoo:DX-Y.NYB 1/TVC CAPITALCOM ICEUS TVC The Fed controls monetary policy through interest rates, securities held:\r\n"( look at FRED: Assets: Securities Held Outright: U.S. Treasury Securities: All: Wednesday Level (TREAST), Assets: Securities Held Outright: U.S. Treasury Securities: Notes and Bonds, Nominal: Wednesday Level (WSHONBNL) Treasury and Agency Securities, All Commercial Banks (USGSEC)" , to inject or Drain liquidity. Repo and ', 'lookup_query': 'DXY', 'bytes': None, 'age_h': None, 'hot': True, 'catalog_kind': 'instrument_ref'}
- `05:09:15`   normalized DGS10 rows=7 exact=True facets=[{'provider': 'fred', 'provider_name': 'FRED', 'n': 2}, {'provider': 'tradingview-vault-live', 'provider_name': 'TradingView Vault (LIVE)', 'n': 2}, {'provider': 'te-mirror', 'provider_name': 'Trading Economics — FRED Mirror', 'n': 2}, {'provider': 'indicator-bus', 'provider_name': 'Canonical Indicator Bus', 'n': 1}]
## S3 Worker + live Chart Pro contract

- `05:09:15`   worker rows=1 raw=1
- `05:09:16`   Pages deployment sha=9a9497826734a5b7418646da333ff9083d1653d2 id=6237260587 state=success
## verdict

- `05:09:16` ✅ PASS_ALL: 57/57 providers, every Indicator Bus entity, every LIVE TradingView vault entity, stored assets, normalized series, and hierarchical Eurostat/ECB series are searchable
