# ops 4733 -- locate import-canary's real Census key (read-only, no value printed)

**Status:** success  
**Duration:** 0.2s  
**Finished:** 2026-08-16T04:17:36+00:00  

## Data

| check | value |
|---|---|
| env_var_names_present | CENSUS_API_KEY |
| census_api_key_env_var_set | True |
| last_modified | 2026-07-22T20:37:10.000+0000 |
| runtime | python3.12 |

## Log
- `04:17:36` ✅ CENSUS_API_KEY IS set directly as a Lambda env var (40 chars, non-empty=True) -- value not printed
## Summary

- `04:17:36` Read-only, no secret values written. This determines whether the backfill just needs a wider time range with the existing working key, or whether Khalid needs to register a free Census API key first (census.gov/data/developers.html).
