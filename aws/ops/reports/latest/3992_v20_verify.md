# ops 3992 — v2.0 vault-complete verify

**Status:** failure  
**Duration:** 278.1s  
**Finished:** 2026-07-27T22:44:17+00:00  

## Error

```
SystemExit: 1
```

## Data

| families | per_source | total | vault_walked |
|---|---|---|---|
| 21 | {"FLEET-INTERNAL": 8109, "OTHER": 7349, "FMP": 1296, "POLYGON": 691, "FRED": 491, "BENZINGA-MASSIVE": 132, "CFTC": 95, "ECB": 37, "COINGECKO": 31, "COINMETRICS": 29, "IMF-PORTWATCH": 24, "YAHOO": 20, "OFR": 12, "US-TREASURY": 7, "MOEA-TAIWAN": 4, "HKMA": 3, "SEC-EDGAR": 3, "BOE": 2, "BOJ": 1, "GNEWS": 1, "PBOC": 1} | 18338 |  |
|  |  |  | {"n_paths": 167, "skipped": null, "size": 391643} |

## Log
- `22:39:39`   [0] data-census v1.8 ops3987 clean-values
- `22:40:10`   [1] data-census v1.8 ops3987 clean-values
- `22:40:41`   [2] data-census v1.8 ops3987 clean-values
- `22:41:12`   [3] data-census v1.8 ops3987 clean-values
- `22:41:42`   [4] data-census v1.8 ops3987 clean-values
- `22:42:13`   [5] data-census v1.8 ops3987 clean-values
- `22:42:44`   [6] data-census v1.8 ops3987 clean-values
- `22:43:15`   [7] data-census v1.8 ops3987 clean-values
- `22:43:45`   [8] data-census v1.8 ops3987 clean-values
- `22:44:16` ✅   v2.0 artifact after ~270s
- `22:44:16`   BOJ              JPLG    -> {"name": "JPLG", "value": 7.07, "live": true, "pulled_from": "bank-of-japan", "engine": "justhodl-tradingview", "artifact": "data/tradingview.json", "
- `22:44:16`   MOF-JAPAN        JP02Y   -> ABSENT
- `22:44:16`   NORGES           NO03Y   -> ABSENT
- `22:44:16`   BCRP-PERU        PETOT   -> ABSENT
- `22:44:16`   ECB              EUBUND  -> ABSENT
- `22:44:16`   FRED US10Y -> {"name": "US10Y", "value": 4.71, "live": true, "pulled_from": "fred_alias:DGS10", "engine": "justhodl-tradingview", "artifact": "data/tradingview.json", "path": "symbols[
- `22:44:17` ✅   artifact is v2.0
- `22:44:17` ✗   vault actually walked (>=3000 paths, not skipped)
- `22:44:17` ✗   BOJ family holds JPLG
- `22:44:17` ✗   MOF-JAPAN holds JP02Y
- `22:44:17` ✗   NORGES holds NO03Y
- `22:44:17` ✗   BCRP-PERU holds PETOT
- `22:44:17` ✗   ECB holds EUBUND
- `22:44:17` ✗   OTHER shrunk below 800
- `22:44:17` ✅   US10Y credited to the vault engine (v1.9 canonical)
- `22:44:17` ✅   page v4 still at edge
- `22:44:17` ✗ FAILED: ['vault actually walked (>=3000 paths, not skipped)', 'BOJ family holds JPLG', 'MOF-JAPAN holds JP02Y', 'NORGES holds NO03Y', 'BCRP-PERU holds PETOT', 'ECB holds EUBUND', 'OTHER shrunk below 800']
