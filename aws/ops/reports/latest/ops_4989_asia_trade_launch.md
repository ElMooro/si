## G-1 markers

**Status:** success  
**Duration:** 479.1s  
**Finished:** 2026-08-26T16:07:58+00:00  

## Data

| cl | hk | kr_status |
|---|---|---|
| 15 | 37 | awaiting ECOS key (vault item key_hash=ecos, attr api_key) - |

## Log
- `15:59:59`   ok justhodl-asia-trade-full
- `15:59:59`   ok justhodl-provider-catalog
## G0 settle + schedule

- `16:00:00`   justhodl-asia-trade-full settled (0s)
- `16:00:00`   justhodl-provider-catalog settled (0s)
- `16:00:01`   schedule created
## G1 run (sync)

- `16:01:22`   invoke: err=None {"ok": true, "hk": 37, "cl": 15, "kr": 0, "failures": 5, "elapsed_s": 80.3}
- `16:01:22`     fail cl:importaciones-mineras__7eb3d654-d42e-4760-af95-3572c3684d81.csv: thin 124B
- `16:01:22`     fail cl:3149__0dcde026-67a2-4ac4-a806-48ba276583f1.xlsx: URL can't contain control characters. '/uploads/recursos/GOBERNACIONES
- `16:01:22`     fail cl:comercio-exterior1__41069245-78b3-4a46-85f0-3986c799c2db.csv: HTTP Error 404: NOT FOUND
- `16:01:22`     fail cl:comercio-exterior1__5944be9b-cf33-445b-9fa7-c023f905307a.csv: HTTP Error 404: NOT FOUND
- `16:01:22`     fail cl:comercio-exterior1__a132a72c-4c93-4f84-9b78-1c4f131c7e71.csv: HTTP Error 404: NOT FOUND
- `16:01:22`   hk=37 cl=15 kr=0 kr_status=awaiting ECOS key (vault item key_hash=ecos, attr api_key) -- Bank of Korea StatisticSearc
- `16:01:22` G1 PASS
## G2 substance

- `16:01:23`   hk hk-censtatd-tablechart-310-33041__3a938b63-66b8-47 rows~499 18547B
- `16:01:23`   cl 3782__6dbfe596-8f4b-4c8c-9d50-13ca62f040e6.csv.gz rows~30 1612B
- `16:01:23` G2 PASS (2/2)
## G3 cards

- `16:07:58`   hk-data OK trade/industry mirror: 37 resources · ports/manufacturing/exports/imports
- `16:07:58`   cl-datos OK comercio/industria mirror: 15 resources
- `16:07:58`   kr-ecos OK ECOS: 0 series · awaiting ECOS key (vault item key_hash=ecos, attr api_key) -- Bank of Korea Statist
- `16:07:58` G3 PASS (3/3)
- `16:07:58` ops 4989 GREEN -- HK + Chile trade/industry live on data.html; Korea drains the moment an ECOS key lands in the vault
