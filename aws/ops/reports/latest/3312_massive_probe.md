## SSM

**Status:** success  
**Duration:** 1.4s  
**Finished:** 2026-07-15T01:18:05+00:00  

## Data

| RESULT | benzinga_ratings | fingerprint | justhodl-analyst-actions | justhodl-benzinga-earnings | justhodl-massive-signals | key_fp | present | ssm_path | testing_key_from |
|---|---|---|---|---|---|---|---|---|---|
|  |  | {'len': 32, 'prefix': 'ch6C', 'suffix': 'ptM'} |  |  |  |  | True | /justhodl/massive-api-key |  |
|  |  |  |  |  | no massive/polygon env var |  |  |  |  |
|  |  |  |  | no massive/polygon env var |  |  |  |  |  |
|  |  |  | no massive/polygon env var |  |  |  |  |  |  |
|  |  |  |  |  |  | {'len': 32, 'prefix': 'ch6C', 'suffix': 'ptM'} |  |  | ssm |
|  | {'http': 403, 'error': '{"status":"NOT_AUTHORIZED","request_id":"861259a29c0b7f590e1263dcdf0f291d","message":"You are not entitled to this data. Please upgrade your plan at https://massive.com/pricing"}'} |  |  |  |  |  |  |  |  |
| FAIL_NOT_ENTITLED |  |  |  |  |  |  |  |  |  |

## Log
- `01:18:04` ✅ SSM param exists
## CONSUMER ENV SCAN

## BENZINGA LIVE TEST

- `01:18:05` ✗ key present but NOT entitled (HTTP 403) — Massive Benzinga add-on lapsed; needs renewal/new key
