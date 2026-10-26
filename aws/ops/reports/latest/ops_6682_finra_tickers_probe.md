# ops 6682 - the FINRA tickers artifact as the factor book reads it (read-only)

**Status:** success  
**Duration:** 1.2s  
**Finished:** 2026-10-26T20:51:11+00:00  

## Log
- `20:51:10` ⚠ data/short-interest-tickers.json: ClientError An error occurred (404) when calling the HeadObject operation: Not Found
## the book manifest's notes

- `20:51:10` manifest keys: ['as_of', 'common_stocks', 'composites', 'coverage', 'debt_or_structured', 'engine', 'etfs', 'fields', 'funds', 'last_run', 'matrix', 'notes', 'objects', 'otc', 'pages', 'pages_bytes', 'preferreds', 'prefix', 'ranked', 'read_with', 'schema', 'schema_found', 'shells', 'source_columns', 'sources', 'symbols', 'typed_by_polygon', 'universe', 'updated_at', 'version']
- `20:51:10`    note: finra short interest: data/short-interest-tickers.json unavailable or not the short-interest-tickers.v1 contract
- `20:51:10` sources.finra_short {'tickers': 0} | coverage.finra_short 0 | version 1.0.6
## justhodl-short-interest: 48 h of REPORT and tickers-layer lines

- `20:51:11`    10-25 07:19:51 | [tickers-layer] skipped: ValueError
- `20:51:11`    10-25 07:19:51 | Duration: 253543.12 ms Max Memory Used: 334 MB
- `20:51:11` ✅ ops 6682 complete (read-only)
