# ops 3992 — v2.1 vault-complete verify

**Status:** failure  
**Duration:** 250.5s  
**Finished:** 2026-07-27T22:54:30+00:00  

## Error

```
SystemExit: 1
```

## Data

| families | per_source | total | vault_walked |
|---|---|---|---|
| 21 | {"FLEET-INTERNAL": 8104, "OTHER": 7323, "FMP": 1306, "POLYGON": 692, "FRED": 530, "BENZINGA-MASSIVE": 132, "CFTC": 95, "ECB": 37, "COINGECKO": 31, "COINMETRICS": 29, "YAHOO": 25, "IMF-PORTWATCH": 24, "OFR": 12, "US-TREASURY": 7, "MOEA-TAIWAN": 4, "HKMA": 3, "SEC-EDGAR": 3, "BOE": 2, "BOJ": 1, "GNEWS": 1, "PBOC": 1} | 18362 |  |
|  |  |  | {"n_paths": 401, "skipped": null, "size": 391643} |

## Log
- `22:50:20`   [0] data-census v2.0 ops3991 vault-complete
- `22:50:52`   [1] data-census v2.0 ops3991 vault-complete
- `22:51:23`   [2] data-census v2.0 ops3991 vault-complete
- `22:51:54`   [3] data-census v2.0 ops3991 vault-complete
- `22:52:25`   [4] data-census v2.0 ops3991 vault-complete
- `22:52:56`   [5] data-census v2.0 ops3991 vault-complete
- `22:53:27`   [6] data-census v2.0 ops3991 vault-complete
- `22:53:58`   [7] data-census v2.0 ops3991 vault-complete
- `22:54:29` ✅   v2.1 artifact after ~240s
- `22:54:29`   BOJ              JPLG    -> {"name": "JPLG", "value": 7.07, "live": true, "pulled_from": "bank-of-japan", "engine": "justhodl-tradingview", "artifact": "data/tradingview.json", "
- `22:54:29`   MOF-JAPAN        JP02Y   -> ABSENT
- `22:54:29`   NORGES           NO03Y   -> ABSENT
- `22:54:29`   BCRP-PERU        PETOT   -> ABSENT
- `22:54:29`   ECB              EUBUND  -> ABSENT
- `22:54:29`   FRED US10Y -> {"name": "US10Y", "value": 4.71, "live": true, "pulled_from": "fred_alias:DGS10", "engine": "justhodl-tradingview", "artifact": "data/tradingview.json", "path": "symbols[
- `22:54:30` ✅   artifact is v2.1
- `22:54:30` ✗   vault actually walked (>=3000 paths, not skipped)
- `22:54:30` ✗   BOJ family holds JPLG
- `22:54:30` ✗   MOF-JAPAN holds JP02Y
- `22:54:30` ✗   NORGES holds NO03Y
- `22:54:30` ✗   BCRP-PERU holds PETOT
- `22:54:30` ✗   ECB holds EUBUND
- `22:54:30` ✗   OTHER shrunk below 800
- `22:54:30` ✅   US10Y credited to the vault engine (v1.9 canonical)
- `22:54:30` ✅   page v4 still at edge
- `22:54:30` ✗ FAILED: ['vault actually walked (>=3000 paths, not skipped)', 'BOJ family holds JPLG', 'MOF-JAPAN holds JP02Y', 'NORGES holds NO03Y', 'BCRP-PERU holds PETOT', 'ECB holds EUBUND', 'OTHER shrunk below 800']
