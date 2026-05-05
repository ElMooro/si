
# 1) Confirm v2 source is the deployed code

- `18:15:44`     Lambda LastModified: 2026-05-05T18:04:00.000+0000
- `18:15:44`     state: Active  mem=1024MB  timeout=600s
- `18:15:44`     env keys: ['CLUSTER_MIN_INSIDERS', 'FMP_KEY', 'LOOKBACK_DAYS', 'MAX_FILINGS_TO_PARSE', 'MIN_BUY_VALUE_USD', 'N_BUSINESS_DAYS_INDEX', 'N_WORKERS', 'S3_BUCKET', 'S3_KEY', 'SEC_USER_AGENT']
- `18:15:44`     v2 method:    True
- `18:15:44`     MAX_FILINGS:  True
- `18:15:44`     rate-lock:    False

# 2) Set conservative env to ensure completion within timeout budget

- `18:15:46`     ✓ env set: MAX_FILINGS=800, N_BUSINESS_DAYS_INDEX=5, N_WORKERS=8, timeout=600s, mem=1024MB

# 3) Invoke and time it

- `18:15:46`     invoking (expect 3-6 min)...
- `18:20:54`     ❌ invoke threw: Read timeout on endpoint URL: "https://lambda.us-east-1.amazonaws.com/2015-03-31/functions/justhodl-insider-cluster-scanner/invocations"