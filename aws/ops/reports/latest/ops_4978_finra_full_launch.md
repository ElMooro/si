## G-1 markers

**Status:** failure  
**Duration:** 26.9s  
**Finished:** 2026-08-25T16:11:02+00:00  

## Error

```
SystemExit: 1
```

## Log
- `16:10:35`   ok justhodl-finra-full    'v1.0.0 ops4978'
- `16:10:35`   ok justhodl-provider-catalog 'finra-note-v2'
## P0 credential hunt + runner proof

- `16:10:53`   keys-table env on justhodl-api-keys-admin -> justhodl-api-keys
- `16:10:54`   dynamo scan: 14 items; providers=['?']
- `16:11:02`   fleet credential-shaped env names: ['ADMIN_TOKEN_SSM', 'ALPHAVANTAGE_API_KEY', 'ALPHAVANTAGE_KEY', 'ALPHA_VANTAGE_KEY', 'ANTHROPIC_API_KEY', 'ANTHROPIC_KEY', 'ANTHROPIC_KEY_SSM', 'AV_KEY', 'BEA_API_KEY', 'BEA_KEY', 'BLS_API_KEY', 'BLS_KEY', 'CENSUS_API_KEY', 'CENSUS_KEY', 'CMC_API_KEY', 'CMC_KEY', 'DESK_KEY', 'EIA_API_KEY', 'FEEDBACK_AUTH_TOKEN', 'FMP_API_KEY', 'FMP_KEY', 'FRED_API_KEY', 'FRED_KEY', 'GITHUB_TOKEN_SSM', 'INGEST_TOKEN', 'JUSTHODL_API_KEYS_TABLE', 'NASDAQ_API_KEY', 'NEWSAPI_KEY', 'NEWS_API_KEY', 'NEWS_KEY', 'NQ_PROXY_KEY', 'OUT_KEY', 'PJM_API_KEY', 'POLYGON_API_KEY', 'POLYGON_KEY', 'POLY_KEY', 'S3_FILINGS_KEY', 'S3_INPUT_KEY', 'S3_KEY', 'S3_KEY_DIVERGENCE', 'S3_KEY_HISTORY', 'S3_KEY_NOWCAST', 'S3_KEY_OUT', 'S3_KEY_ROTATION', 'S3_KEY_STATE', 'S3_KEY_STATS', 'S3_KEY_TRADES', 'S3_OUTPUT_KEY', 'SIGNAL_BOARD_KEY', 'SSM_TOKEN_PATH', 'STATE_KEY', 'TELEGRAM_BOT_TOKEN', 'TELEGRAM_TOKEN', 'TE_API_KEY', 'TG_TOKEN_PARAM', 'TIINGO_API_KEY', 'TOKEN', 'TREASURY_KEY']
- `16:11:02`   resolved: client_id=False secret=False apikey=False
- `16:11:02` G0 FAIL: no working FINRA credentials found in fleet envs -- name where the key lives and I wire it in
