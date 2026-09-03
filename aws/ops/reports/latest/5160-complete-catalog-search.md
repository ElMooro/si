# ops 5160 -- every first-class data.html dataset searchable

**Status:** failure  
**Duration:** 1316.6s  
**Finished:** 2026-09-03T04:20:57+00:00  

## Error

```
SystemExit: 1
```

## Log
## S1 provider catalog -> 57-provider FTS index

- `03:59:01`   zip: 116842 bytes
## 1. Lambda

- `03:59:01`   Lambda exists — updating
- `03:59:06` ✅   ✓ updated justhodl-provider-catalog
- `04:06:01`   manifest providers=57 documents=816045 index=data/search/index/provider-search-20260903T035914Z-d8892f7cdeba.sqlite.gz bytes=35657177
- `04:06:01`   entities indicator_bus=18741 tradingview_live=5569 coverage={'catalog_datasets': 811952, 'storage_objects': 1922533, 'storage_bytes': 530440528613, 'indexed_assets': 787644, 'indexed_entity_refs': 24310, 'indexed_series_refs': 4034, 'hierarchical_series': {'eurostat': 564204235, 'ecb': 3240832, 'access': 'tier1_prefix'}}
- `04:06:02`   sample provider=gdelt query='2003' id=gdelt:asset:12f5946134bdd2cb
## S2 symdir v1.9.0 -> native + warehouse merged search

- `04:06:02`   zip: 146468 bytes
## 1. Lambda

- `04:06:02`   Lambda exists — updating
- `04:06:07` ✅   ✓ updated justhodl-symdir
- `04:10:43`   symdir docs=1378332 elapsed=270.1 provider_shards={'providers': 57, 'docs': 57, 'manifest_docs': 816045, 'index': 'data/search/index/provider-search-20260903T035914Z-d8892f7cdeba.sqlite.gz', 'generated_at': '2026-09-03T03:59:14+00:00', 's': 0.6}
- `04:10:52`   warm docs=1378332 warehouse_ready=True warehouse_error=None
- `04:10:52`   direct search rows=1 raw=1 more=False error=None
- `04:10:52`   entity search indicator={'id': 'indicator-bus:DXY', 'symbol': 'DXY', 'name': 'DXY', 'provider': 'indicator-bus', 'provider_name': 'Canonical Indicator Bus', 'kind': 'dataset', 'chartable': False, 'browse': False, 'browse_provider': None, 'raw': False, 'key': None, 'src': 'yahoo:DX-Y.NYB yahoo_5d', 'lookup_query': 'DXY', 'bytes': None, 'age_h': None, 'hot': True, 'catalog_kind': 'indicator_ref'} tv=None
- `04:10:52`   normalized DGS10 rows=7 exact=True facets=[{'provider': 'fred', 'provider_name': 'FRED', 'n': 2}, {'provider': 'tradingview-vault-live', 'provider_name': 'TradingView Vault (LIVE)', 'n': 2}, {'provider': 'te-mirror', 'provider_name': 'Trading Economics — FRED Mirror', 'n': 2}, {'provider': 'indicator-bus', 'provider_name': 'Canonical Indicator Bus', 'n': 1}]
## S3 Worker + live Chart Pro contract

- `04:10:53`   worker rows=1 raw=1
- `04:10:53`   Pages deployment sha=8a6e326424c284309d540f6bf2dab422252af5d6 id=6236681330 state=success
## verdict

- `04:20:57` ✗ TradingView vault entity search contract failed
- `04:20:57` ✗ live build mismatch commit=8a6e326424c284309d540f6bf2dab422252af5d6 target=8a6e326424c284309d540f6bf2dab422252af5d6 page_hash=ec77d15cc9b2f79c1512a54d9287ed001a055d80c23b0c8efb1f0a766eea2985 manifest_hash=6882b7d7694011d3fef6f1849fb154b9ca3755bc56627c37ec9f8d97c9851951
