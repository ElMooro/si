# ops 4677 — Wayback CDX + investing.com for the 2017-2023 ICE hole

**Status:** failure  
**Duration:** 6.6s  
**Finished:** 2026-08-14T22:49:53+00:00  

## Error

```
SystemExit: 1
```

## Log
## LANE A — Wayback CDX index

- `22:49:52`   wayback CDX BAML captures      ERR HTTP Error 498: 
- `22:49:52`   wayback CDX any fredgraph      ERR HTTP Error 498: 
- `22:49:52`   wayback avail BAMLC0A2CAA      116 bytes | {"url": "fred.stlouisfed.org/graph/fredgraph.csv?id=BAMLC0A2CAA", "archived_snapshots": {}, "timestamp": "20230101"}
- `22:49:52`   BAML captures indexed: 0
## LANE A2 — pull the best pre-truncation capture

- `22:49:52`   distinct BAML series with pre-truncation captures: 0
- `22:49:52`   no pre-truncation BAML captures to fetch
## LANE B — investing.com (Khalid's lead)

- `22:49:52`   investing search HY OAS        2615 bytes | {"articles":[{"id":200176142,"url":"/analysis/are-high-yield-etfs-becoming-too-hot-to-handle-200176142","description":"Are High Yield ETFs Becoming Too Hot To Handle?","image":"https://d1-invdn-com.akamaized.net/company_
- `22:49:52`   investing econ indicator page  ERR HTTP Error 403: Forbidden
- `22:49:52`   investing api instruments      ERR HTTP Error 403: Forbidden
- `22:49:53`   (temp probe deleted)
## verdict

- `22:49:53` ✗ Wayback holds no indexed BAML csv captures — lane closed, recorded honestly
