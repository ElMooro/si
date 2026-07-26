# ops 3937 — gov-adapters wave 1

**Status:** failure  
**Duration:** 512.2s  
**Finished:** 2026-07-26T23:35:22+00:00  

## Error

```
SystemExit: 1
```

## Data

| coverage_pct | fred_calls | n_live | statuses |
|---|---|---|---|
| 80.2 | 265 | 450 | {'META': 1, 'LIVE': 450, 'DISCONTINUED': 2, 'NO_FREE_SOURCE': 108} |

## Log
## BCRP Peru — ToT series-code discovery (wire next wave)

- `23:27:01`   PN02574AQ;PBI gasto;Ingreso nacional disponible (millones S/ 2007);PBI - Producto Nacional Bruto - Efecto Trminos de Intercambio;;;;;;INEI, BCRP;Trimestral;2022-02-18;Demanda y ofe
- `23:27:01`   PM04904AA;PBI gasto;Ingreso nacional disponible (millones S/ 2007);Efecto Trminos de Intercambio;;;;;;INEI, BCRP;Anual;2022-03-30;Demanda y oferta global, balanza de pagos, sector 
- `23:27:01`   CD11239DA;Entre 1930 a 1980;Indicadores del comercio exterior, 1930-1980;Terminos de Intercambio;Compendio de Historia Econmica de Per / Entre 1930 a 1980 / Indicadores del comerci
- `23:27:01`   CD11258DA;Entre 1930 a 1980;Trminos de intercambio de comercio exterior, 1950-1980 (1994 = 100);ndice de Precios Nominales - Exportaciones (ndice);Compendio de Historia Econmica de
- `23:27:01`   CD11259DA;Entre 1930 a 1980;Trminos de intercambio de comercio exterior, 1950-1980 (1994 = 100);ndice de Precios Nominales - Exportaciones (var% anual);Compendio de Historia Econmi
- `23:27:01`   CD11260DA;Entre 1930 a 1980;Trminos de intercambio de comercio exterior, 1950-1980 (1994 = 100);ndice de Precios Nominales - Importaciones (ndice);Compendio de Historia Econmica de
- `23:27:01`   CD11261DA;Entre 1930 a 1980;Trminos de intercambio de comercio exterior, 1950-1980 (1994 = 100);ndice de Precios Nominales - Importaciones (var% anual);Compendio de Historia Econmi
- `23:27:01`   CD11262DA;Entre 1930 a 1980;Trminos de intercambio de comercio exterior, 1950-1980 (1994 = 100);Trminos de Intercambio (ndice);Compendio de Historia Econmica de Per / Entre 1930 a 
- `23:27:02` ✅   settled attempt 1
- `23:27:02`   async force fired 2026-07-26T23:27:02.616886+00:00; polling…
- `23:35:22` ✅   artifact refreshed ~480s
- `23:35:22`   US02MY: LIVE value=3.95 src=treasury.gov asof=treasury.gov:07/24/2026
- `23:35:22`   ITGDG: LIVE value=137.1 src=eurostat asof=eurostat:IT:2025
- `23:35:22`   ESGDG: LIVE value=100.7 src=eurostat asof=eurostat:ES:2025
- `23:35:22`   EUGDG: LIVE value=87.8 src=eurostat asof=eurostat:EA20:2025
- `23:35:22`   NO03Y: NO_FREE_SOURCE value=None src=unresolved_tv_only asof=None
- `23:35:22` ✅   v3.2 settled
- `23:35:22` ✅   force run wrote
- `23:35:22` ✅   US02MY LIVE via treasury.gov
- `23:35:22` ✅   ITGDG LIVE via eurostat
- `23:35:22` ✅   ESGDG LIVE via eurostat
- `23:35:22` ✅   EUGDG LIVE via eurostat
- `23:35:22` ✗   NO03Y LIVE via norges-bank
- `23:35:22` ✅   n_live >= 447
- `23:35:22` ✅   zero bare UNRESOLVED
- `23:35:22` ✗ FAILED: ['NO03Y LIVE via norges-bank']
