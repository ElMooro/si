# ops 4745 -- Swagger spec hunt for ambs/tsy/seclending

**Status:** success  
**Duration:** 0.9s  
**Finished:** 2026-08-16T15:55:09+00:00  

## Data

| banked | check | family | reason | value |
|---|---|---|---|---|
|  | shell_status |  |  | 200 |
|  | shell_bytes |  |  | 6404 |
|  | spec_found |  |  | True |
|  | spec_url |  |  | https://markets.newyorkfed.org/static/docs/./markets-api.yml |
|  | spec_kind |  |  | yaml |
|  | templates_total |  |  | 0 |
|  | templates_ambs |  |  | 0 |
|  | templates_tsy |  |  | 0 |
|  | templates_seclending |  |  | 0 |
|  | enums_harvested |  |  | 0 |
| False |  | ambs | no_template_validated |  |
| False |  | tsy | no_template_validated |  |
| False |  | seclending | no_template_validated |  |

## Log
## A. Fetch the Swagger UI shell -- with full logging this time

- `15:55:09` shell first 300 chars: '<!-- HTML for static distribution bundle build -->\r\n<!DOCTYPE html>\r\n<html lang="en">\r\n  <head>\r\n    <meta charset="UTF-8" />\r\n    <title>Markets Data APIs</title>\r\n    <link rel="stylesheet" type="text/css" href="./swagger-ui.css" />\r\n    <link rel="icon" type="image/x-icon" href="./favicon.ico" si'
## B. Locate + fetch the spec file

- `15:55:09` docs/./markets-api.yml -> status=200 bytes=143732 kind=yaml
## C. Extract templates + enums for the locked families

## D. Validate + bank

- `15:55:09` ⚠ ambs: no spec template validated with rows
- `15:55:09` ⚠ tsy: no spec template validated with rows
- `15:55:09` ⚠ seclending: no spec template validated with rows
