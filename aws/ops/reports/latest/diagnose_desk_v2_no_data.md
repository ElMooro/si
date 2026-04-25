# Diagnose desk-v2.html 'no data' issue

**Status:** success  
**Duration:** 0.9s  
**Finished:** 2026-04-25T22:19:39+00:00  

## Data

| keys_found | keys_missing | keys_total |
|---|---|---|
| 8 | 1 | 9 |

## Log
## A. Bucket CORS configuration

- `22:19:38`   CORS rules: 1
- `22:19:38`   Rule #1:
- `22:19:38`     AllowedOrigins: ['https://justhodl.ai', 'https://elmooro.github.io', 'http://localhost:*', '*']
- `22:19:38`     AllowedMethods: ['GET', 'HEAD']
- `22:19:38`     AllowedHeaders: ['*']
- `22:19:38`     ExposeHeaders:  ['ETag']
- `22:19:38`     MaxAgeSeconds:  300
## B. Bucket public access block

- `22:19:38`   BlockPublicAcls:       False
- `22:19:38`   IgnorePublicAcls:      False
- `22:19:38`   BlockPublicPolicy:     False
- `22:19:38`   RestrictPublicBuckets: False
## C. Bucket policy (controls public read)

- `22:19:38`   PublicReadDataDir:
- `22:19:38`     Effect:    Allow
- `22:19:38`     Principal: *
- `22:19:38`     Action:    s3:GetObject
- `22:19:38`     Resource:  arn:aws:s3:::justhodl-dashboard-live/data/*
- `22:19:38`   PublicReadScreener:
- `22:19:38`     Effect:    Allow
- `22:19:38`     Principal: *
- `22:19:38`     Action:    s3:GetObject
- `22:19:38`     Resource:  arn:aws:s3:::justhodl-dashboard-live/screener/*
- `22:19:38`   PublicReadSentiment:
- `22:19:38`     Effect:    Allow
- `22:19:38`     Principal: *
- `22:19:38`     Action:    s3:GetObject
- `22:19:38`     Resource:  arn:aws:s3:::justhodl-dashboard-live/sentiment/*
- `22:19:38`   PublicReadRootDashboardFiles:
- `22:19:38`     Effect:    Allow
- `22:19:38`     Principal: *
- `22:19:38`     Action:    s3:GetObject
- `22:19:38`     Resource:  arn:aws:s3:::justhodl-dashboard-live/flow-data.json
- `22:19:38`     Resource:  arn:aws:s3:::justhodl-dashboard-live/crypto-intel.json
- `22:19:38`     Resource:  arn:aws:s3:::justhodl-dashboard-live/health.html
- `22:19:38`     Resource:  arn:aws:s3:::justhodl-dashboard-live/_health/*
## D. Per-key status (do they exist?)

- `22:19:38`   ✅ regime/current.json                                    1399B  2026-04-25 20:00
- `22:19:38`   ✅ divergence/current.json                                6280B  2026-04-25 18:56
- `22:19:39`   ✅ cot/extremes/current.json                              8335B  2026-04-25 16:11
- `22:19:39`   ✅ risk/recommendations.json                              9658B  2026-04-25 18:45
- `22:19:39`   ✅ opportunities/asymmetric-equity.json                  13608B  2026-04-25 16:16
- `22:19:39`   ✅ portfolio/pnl-daily.json                               1182B  2026-04-25 22:00
- `22:19:39` ⚠   ❌ investor-debate/_index.json                        404
- `22:19:39`   ✅ intelligence-report.json                               4369B  2026-04-25 12:10
- `22:19:39`   ✅ crypto-intel.json                                     56354B  2026-04-25 22:09
## E. Diagnosis

- `22:19:39`   Found: 8 / 9 keys
- `22:19:39` ⚠   Some keys are missing — Lambdas haven't run yet, OR
- `22:19:39` ⚠   the path is different from what desk-v2 fetches.
- `22:19:39` Done
