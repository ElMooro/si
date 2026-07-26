# ops 3938 — Norges close + BCRP index-code grep

**Status:** failure  
**Duration:** 477.8s  
**Finished:** 2026-07-26T23:45:25+00:00  

## Error

```
SystemExit: 1
```

## Data

| coverage_pct | n_live | statuses |
|---|---|---|
| 80.2 | 450 | {'META': 1, 'LIVE': 450, 'DISCONTINUED': 2, 'NO_FREE_SOURCE': 108} |

## Log
## BCRP monthly ToT index code

- `23:37:39`   PN38915BM;Trminos de intercambio;Trminos de intercambio de comercio exterior (ndice 2007 = 100);ndice de Precios Nominales - Exportaciones;;;;;;BCRP, Sunat, Zofratacna, B
- `23:37:39`   PN38916BM;Trminos de intercambio;Trminos de intercambio de comercio exterior (var% mensual);ndice de Precios Nominales - Exportaciones;;;;;;BCRP, Sunat, Zofratacna, Banco
- `23:37:39`   PN38917BM;Trminos de intercambio;Trminos de intercambio de comercio exterior (var% acumulada);ndice de Precios Nominales - Exportaciones;;;;;;BCRP, Sunat, Zofratacna, Ban
- `23:37:39`   PN38918BM;Trminos de intercambio;Trminos de intercambio de comercio exterior (var% 12 meses);ndice de Precios Nominales - Exportaciones;;;;;;BCRP, Sunat, Zofratacna, Banc
- `23:37:39`   PN38919BM;Trminos de intercambio;Trminos de intercambio de comercio exterior (ndice 2007 = 100);ndice de Precios Nominales - Importaciones;;;;;;BCRP, Sunat, Zofratacna, B
- `23:37:39`   PN38920BM;Trminos de intercambio;Trminos de intercambio de comercio exterior (var% mensual);ndice de Precios Nominales - Importaciones;;;;;;BCRP, Sunat, Zofratacna, Banco
- `23:37:40` ✅   settled attempt 1
- `23:45:25` ✅   refreshed ~450s
- `23:45:25`   NO03Y: NO_FREE_SOURCE value=None src=unresolved_tv_only
- `23:45:25` ✅   v3.2.1 settled
- `23:45:25` ✅   force run wrote
- `23:45:25` ✗   NO03Y LIVE via norges-bank
- `23:45:25` ✗   n_live >= 451
- `23:45:25` ✅   zero bare UNRESOLVED
- `23:45:25` ✗ FAILED: ['NO03Y LIVE via norges-bank', 'n_live >= 451']
