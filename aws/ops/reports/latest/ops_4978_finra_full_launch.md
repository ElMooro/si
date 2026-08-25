## G-1 markers

**Status:** failure  
**Duration:** 1243.3s  
**Finished:** 2026-08-25T18:02:17+00:00  

## Error

```
SystemExit: 1
```

## Log
- `17:41:34`   ok justhodl-finra-full    'v1.0.4 ops4978'
- `17:41:34`   ok justhodl-provider-catalog 'finra-note-v2'
## P0 credential hunt + runner proof

- `17:41:38`   donor justhodl-finra-full                ['FINRA_CLIENT_ID', 'FINRA_CRED_NOTE', 'FINRA_CRED_EXPIRES']
- `17:41:50`   keys-table env on justhodl-api-keys-admin -> justhodl-api-keys
- `17:41:50`   dynamo scan: 15 items; providers=['?', 'finra']
- `17:41:50`     vault item: ? (attrs ['created_at', 'key_hash', 'label', 'last_used_at', 'owner_email', 'tier'])
- `17:41:50`     vault item: ? (attrs ['created_at', 'key_hash', 'label', 'last_used_at', 'owner_email', 'revoked_at'])
- `17:41:50`     vault item: ? (attrs ['created_at', 'key_hash', 'label', 'last_used_at', 'owner_email', 'revoked_at'])
- `17:41:50`     vault item: ? (attrs ['created_at', 'key_hash', 'label', 'last_used_at', 'owner_email', 'revoked_at'])
- `17:41:50`     vault item: ? (attrs ['created_at', 'key_hash', 'label', 'last_used_at', 'owner_email', 'revoked_at'])
- `17:41:50`     vault item: ? (attrs ['created_at', 'key_hash', 'label', 'owner_email', 'revoked_at', 'tier'])
- `17:41:50`     vault item: ? (attrs ['created_at', 'key_hash', 'label', 'last_used_at', 'owner_email', 'revoked_at'])
- `17:41:50`     vault item: ? (attrs ['created_at', 'key_hash', 'label', 'last_used_at', 'owner_email', 'revoked_at'])
- `17:41:50`     vault item: finra (attrs ['client_id', 'key_hash', 'note', 'old_client_id', 'provider', 'status'])
- `17:41:50`   FINRA item attrs=['key_hash', 'provider', 'note', 'client_id', 'old_client_id', 'status']
- `17:41:50`     vault item: ? (attrs ['created_at', 'key_hash', 'label', 'last_used_at', 'owner_email', 'revoked_at'])
- `17:41:50`     vault item: ? (attrs ['created_at', 'key_hash', 'label', 'last_used_at', 'owner_email', 'revoked_at'])
- `17:41:50`     vault item: ? (attrs ['created_at', 'key_hash', 'label', 'last_used_at', 'owner_email', 'revoked_at'])
- `17:41:50`     vault item: ? (attrs ['created_at', 'key_hash', 'label', 'last_used_at', 'owner_email', 'revoked_at'])
- `17:41:50`     vault item: ? (attrs ['created_at', 'key_hash', 'label', 'owner_email', 'revoked_at', 'tier'])
- `17:41:50`     vault item: ? (attrs ['created_at', 'key_hash', 'label', 'last_used_at', 'owner_email', 'tier'])
- `17:41:58`   fleet credential-shaped env names: ['ADMIN_TOKEN_SSM', 'ALPHAVANTAGE_API_KEY', 'ALPHAVANTAGE_KEY', 'ALPHA_VANTAGE_KEY', 'ANTHROPIC_API_KEY', 'ANTHROPIC_KEY', 'ANTHROPIC_KEY_SSM', 'AV_KEY', 'BEA_API_KEY', 'BEA_KEY', 'BLS_API_KEY', 'BLS_KEY', 'CENSUS_API_KEY', 'CENSUS_KEY', 'CMC_API_KEY', 'CMC_KEY', 'DESK_KEY', 'EIA_API_KEY', 'FEEDBACK_AUTH_TOKEN', 'FINRA_CLIENT_ID', 'FMP_API_KEY', 'FMP_KEY', 'FRED_API_KEY', 'FRED_KEY', 'GITHUB_TOKEN_SSM', 'INGEST_TOKEN', 'JUSTHODL_API_KEYS_TABLE', 'NASDAQ_API_KEY', 'NEWSAPI_KEY', 'NEWS_API_KEY', 'NEWS_KEY', 'NQ_PROXY_KEY', 'OUT_KEY', 'PJM_API_KEY', 'POLYGON_API_KEY', 'POLYGON_KEY', 'POLY_KEY', 'S3_FILINGS_KEY', 'S3_INPUT_KEY', 'S3_KEY', 'S3_KEY_DIVERGENCE', 'S3_KEY_HISTORY', 'S3_KEY_NOWCAST', 'S3_KEY_OUT', 'S3_KEY_ROTATION', 'S3_KEY_STATE', 'S3_KEY_STATS', 'S3_KEY_TRADES', 'S3_OUTPUT_KEY', 'SIGNAL_BOARD_KEY', 'SSM_TOKEN_PATH', 'STATE_KEY', 'TELEGRAM_BOT_TOKEN', 'TELEGRAM_TOKEN', 'TE_API_KEY', 'TG_TOKEN_PARAM', 'TIINGO_API_KEY', 'TOKEN', 'TREASURY_KEY']
- `17:41:58`   resolved: client_id=True secret=False apikey=False
- `17:41:58`   PUBLIC-tier probe: 200, 2 rows -- proceeding KEYLESS (creds upgrade later)
## G0 settle + env

- `17:41:58`   justhodl-finra-full settled (0s)
- `17:41:59`   justhodl-provider-catalog settled (0s)
- `17:41:59` G0 PASS
## G0b schedules

- `17:41:59`   exists justhodl-finra-full-6h
- `17:41:59`   exists justhodl-finra-full-weekly
## G1 chain-drive (14min)

- `17:41:59`   cleared 19 stale invalid entries
- `17:42:00`   t+   0s DRAIN banked=0 rows=0 q=0 cat=0 inv=0 fail=2
- `17:45:21`   chain restart kick #1
- `17:48:42`   chain restart kick #2
- `17:52:04`   chain restart kick #3
- `17:55:25`   chain restart kick #4
- `17:56:15` G1 FAIL phase=DRAIN banked=0/0 rows=0
## G2 substance: weekly history since <=2016

- `17:56:15`   substance err An error occurred (NoSuchKey) when calling the GetObject operation: The specified key does not exist.
- `17:56:15` G2 FAIL
## G3 finra card (NEW slug)

- `18:02:17` G3 FAIL keys=2 note=FULL Query-API warehouse (finra-full v1): 0/0 datasets · 0 rows since inception · 0MB · phase DRAIN · daily rediscovery
- `18:02:17` ops 4978 RED: G1; G2; G3
