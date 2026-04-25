# Probe FMP /stable/scores to find Altman Z field name

**Status:** success  
**Duration:** 4.9s  
**Finished:** 2026-04-25T23:59:03+00:00  

## Log
## Setup probe Lambda

- `23:59:02` ✅   Created justhodl-tmp-fmp-probe
## Test: scores_AAPL_stable

- `23:59:02`   URL: https://financialmodelingprep.com/stable/scores?symbol=AAPL&apikey=***
- `23:59:02` ⚠   ✗ HTTPError: HTTP Error 404: Not Found
## Test: scores_AAPL_v3

- `23:59:02`   URL: https://financialmodelingprep.com/api/v3/score/AAPL?apikey=***
- `23:59:02` ⚠   ✗ HTTPError: HTTP Error 403: Forbidden
## Test: scores_AAPL_v4

- `23:59:02`   URL: https://financialmodelingprep.com/api/v4/score?symbol=AAPL&apikey=***
- `23:59:02` ⚠   ✗ HTTPError: HTTP Error 403: Forbidden
## Test: scores_v3_companyrating

- `23:59:02`   URL: https://financialmodelingprep.com/api/v3/rating/AAPL?apikey=***
- `23:59:03` ⚠   ✗ HTTPError: HTTP Error 403: Forbidden
## Test: financial_growth_v3

- `23:59:03`   URL: https://financialmodelingprep.com/api/v3/financial-growth/AAPL?limit=1&apikey=***
- `23:59:03` ⚠   ✗ HTTPError: HTTP Error 403: Forbidden
## Cleanup

- `23:59:03` ✅   Deleted justhodl-tmp-fmp-probe
- `23:59:03` Done
