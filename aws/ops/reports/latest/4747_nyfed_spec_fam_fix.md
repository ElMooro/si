# ops 4746 -- PyYAML parse of markets-api.yml -> validate -> bank

**Status:** success  
**Duration:** 44.4s  
**Finished:** 2026-08-16T15:59:50+00:00  

## Data

| banked | check | earliest | family | latest | n_rows | value |
|---|---|---|---|---|---|---|
|  | spec_status |  |  |  |  | 200 |
|  | spec_bytes |  |  |  |  | 143732 |
|  | server_base |  |  |  |  | https://markets.newyorkfed.org |
|  | paths_total |  |  |  |  | 55 |
|  | templates_ambs |  |  |  |  | 4 |
| True |  | 2013-11-21 | ambs | 2026-08-20 | 2875 |  |
|  | templates_tsy |  |  |  |  | 4 |
| True |  | 2005-08-25 | tsy | 2055-08-15 | 1894 |  |
|  | templates_seclending |  |  |  |  | 4 |
| True |  | 2000-01-03 | seclending | 2026-08-17 | 9302 |  |

## Log
- `15:59:06` ✅ spec itself banked to warm (the contract is data too)
- `15:59:06` TRUE family list from the contract: ambs=4, fxs=4, guidesheets=2, marketshare=2, pd=8, rates=8, rp=5, seclending=4, soma=14, tsy=4
- `15:59:07` ambs: /api/ambs/{operation}/results/{include}/search.{format} subs={'operation': 'all', 'include': 'summary', 'format': 'json'} q={} -> status=200 rows=19
- `15:59:20` ✅ ambs: banked 2875 rows, 2013-11-21 -> 2026-08-20
- `15:59:21` tsy: /api/tsy/{operation}/results/{include}/search.{format} subs={'operation': 'all', 'include': 'summary', 'format': 'json'} q={} -> status=200 rows=18
- `15:59:36` ✅ tsy: banked 1894 rows, 2005-08-25 -> 2055-08-15
- `15:59:36` seclending: /api/seclending/{operation}/results/{include}/search.{format} subs={'operation': 'all', 'include': 'summary', 'format': 'json'} q={} -> status=200 rows=424
- `15:59:50` ✅ seclending: banked 9302 rows, 2000-01-03 -> 2026-08-17
