# ops 3992 — v2.2 vault-complete verify

**Status:** failure  
**Duration:** 303.4s  
**Finished:** 2026-07-28T03:16:15+00:00  

## Error

```
SystemExit: 1
```

## Data

| families | per_source | total | vault_walked |
|---|---|---|---|
| 25 | {"FLEET-INTERNAL": 8199, "OTHER": 7323, "FMP": 1470, "POLYGON": 692, "FRED": 650, "BENZINGA-MASSIVE": 132, "CFTC": 95, "YAHOO": 81, "ECB": 41, "COINGECKO": 31, "COINMETRICS": 29, "IMF-PORTWATCH": 24, "OFR": 12, "US-TREASURY": 8, "MOEA-TAIWAN": 4, "EUROSTAT": 3, "HKMA": 3, "SEC-EDGAR": 3, "BOJ": 2, "BOE": 2, "MOF-JAPAN": 1, "NORGES": 1, "BCRP-PERU": 1, "GNEWS": 1, "PBOC": 1} | 18809 |  |
|  |  |  | {"n_paths": 1847, "skipped": null, "size": 391643} |

## Log
- `03:11:12`   [0] data-census v2.1 ops3993 full-lists
- `03:11:42`   [1] data-census v2.1 ops3993 full-lists
- `03:12:12`   [2] data-census v2.1 ops3993 full-lists
- `03:12:43`   [3] data-census v2.1 ops3993 full-lists
- `03:13:13`   [4] data-census v2.1 ops3993 full-lists
- `03:13:43`   [5] data-census v2.1 ops3993 full-lists
- `03:14:14`   [6] data-census v2.1 ops3993 full-lists
- `03:14:44`   [7] data-census v2.1 ops3993 full-lists
- `03:15:14`   [8] data-census v2.1 ops3993 full-lists
- `03:15:45`   [9] data-census v2.1 ops3993 full-lists
- `03:16:15` ✅   v2.2 artifact after ~300s
- `03:16:15`   BOJ              JPLG    -> {"name": "JPLG", "value": 7.07, "live": true, "pulled_from": "bank-of-japan", "engine": "justhodl-tradingview", "artifact": "data/tradingview.json", "
- `03:16:15`   MOF-JAPAN        JP02Y   -> {"name": "JP02Y", "value": 1.531, "live": true, "pulled_from": "mof-japan", "engine": "justhodl-tradingview", "artifact": "data/tradingview.json", "pa
- `03:16:15`   NORGES           NO03Y   -> {"name": "NO03Y", "value": 4.495, "live": true, "pulled_from": "norges-bank", "engine": "justhodl-tradingview", "artifact": "data/tradingview.json", "
- `03:16:15`   BCRP-PERU        PETOT   -> {"name": "PETOT", "value": 182.731, "live": true, "pulled_from": "bcrp-peru", "engine": "justhodl-tradingview", "artifact": "data/tradingview.json", "
- `03:16:15`   ECB              EUBUND  -> {"name": "EUBUND", "value": 3.2247, "live": true, "pulled_from": "ecb:YC", "engine": "justhodl-tradingview", "artifact": "data/tradingview.json", "pat
- `03:16:15`   FRED US10Y -> {"name": "US10Y", "value": 4.71, "live": true, "pulled_from": "fred_alias:DGS10", "engine": "justhodl-tradingview", "artifact": "data/tradingview.json", "path": "symbols[
- `03:16:15` ✅   artifact is v2.2
- `03:16:15` ✗   vault actually walked (>=3000 paths, not skipped)
- `03:16:15` ✅   BOJ family holds JPLG
- `03:16:15` ✅   MOF-JAPAN holds JP02Y
- `03:16:15` ✅   NORGES holds NO03Y
- `03:16:15` ✅   BCRP-PERU holds PETOT
- `03:16:15` ✅   ECB holds EUBUND
- `03:16:15` ✗   OTHER shrunk below 800
- `03:16:15` ✅   US10Y credited to the vault engine (v1.9 canonical)
- `03:16:15` ✅   page v4 still at edge
- `03:16:15` ✗ FAILED: ['vault actually walked (>=3000 paths, not skipped)', 'OTHER shrunk below 800']
