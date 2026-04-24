# ml-predictions: where do its 7 data sources live in S3?

**Status:** success  
**Duration:** 0.5s  
**Finished:** 2026-04-24T23:52:18+00:00  

## Data

| found | missing | total |
|---|---|---|
| 0 | 7 | 7 |

## Log
## Looking for: fed-liquidity

- `23:52:18` ⚠   Not found in S3 (checked fed-liquidity.json, data/fed-liquidity.json, etc.)
## Looking for: enhanced-repo

- `23:52:18` ⚠   Not found in S3 (checked enhanced-repo.json, data/enhanced-repo.json, etc.)
## Looking for: cross-currency

- `23:52:18` ⚠   Not found in S3 (checked cross-currency.json, data/cross-currency.json, etc.)
## Looking for: volatility-monitor

- `23:52:18` ⚠   Not found in S3 (checked volatility-monitor.json, data/volatility-monitor.json, etc.)
## Looking for: bond-indices

- `23:52:18` ⚠   Not found in S3 (checked bond-indices.json, data/bond-indices.json, etc.)
## Looking for: ai-prediction

- `23:52:18` ⚠   Not found in S3 (checked ai-prediction.json, data/ai-prediction.json, etc.)
## Looking for: global-liquidity

- `23:52:18` ⚠   Not found in S3 (checked global-liquidity.json, data/global-liquidity.json, etc.)
## Summary

- `23:52:18`   Sources with S3 representation: 0/7
- `23:52:18`   Sources missing: ['fed-liquidity', 'enhanced-repo', 'cross-currency', 'volatility-monitor', 'bond-indices', 'ai-prediction', 'global-liquidity']
- `23:52:18` 
- `23:52:18`   If most sources exist as fresh S3 keys, rewrite fetch_all_data()
- `23:52:18`   to read from S3 directly. If sources are missing or stale, the
- `23:52:18`   source-of-truth Lambdas (e.g. fed-liquidity-agent) themselves
- `23:52:18`   may also be broken, which would be a much bigger investigation.
- `23:52:18` Done
