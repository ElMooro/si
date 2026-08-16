# ops 4746 -- PyYAML parse of markets-api.yml -> validate -> bank

**Status:** success  
**Duration:** 1.0s  
**Finished:** 2026-08-16T15:57:36+00:00  

## Data

| banked | check | family | reason | value |
|---|---|---|---|---|
|  | spec_status |  |  | 200 |
|  | spec_bytes |  |  | 143732 |
|  | server_base |  |  | https://markets.newyorkfed.org |
|  | paths_total |  |  | 55 |
|  | templates_ambs |  |  | 0 |
| False |  | ambs | no_template_validated |  |
|  | templates_tsy |  |  | 0 |
| False |  | tsy | no_template_validated |  |
|  | templates_seclending |  |  | 0 |
| False |  | seclending | no_template_validated |  |

## Log
- `15:57:36` ✅ spec itself banked to warm (the contract is data too)
- `15:57:36` TRUE family list from the contract: api=55
- `15:57:36` ⚠ ambs: every spec template failed even with real enum substitutions -- exact attempts logged above
- `15:57:36` ⚠ tsy: every spec template failed even with real enum substitutions -- exact attempts logged above
- `15:57:36` ⚠ seclending: every spec template failed even with real enum substitutions -- exact attempts logged above
