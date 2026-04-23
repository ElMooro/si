# Update bucket policy to expose data/* + flow/crypto JSONs

**Status:** success  
**Duration:** 0.6s  
**Finished:** 2026-04-23T16:07:11+00:00  

## Data

| success | task | total |
|---|---|---|
| 5 | s3-policy | 5 |

## Log
## 1. Current bucket policy

- `16:07:10`   Current: {"Version":"2012-10-17","Statement":[{"Sid":"PublicReadReportJson","Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::justhodl-dashboard-live/report.json"},{"Sid":"PublicReadScreener","Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::jus
## 2. Apply new policy

- `16:07:10` ✅   New policy applied
## 3. Verify public HTTPS access

- `16:07:10`   ✓ data/report.json: 200 
- `16:07:10`   ✓ data/secretary-latest.json: 200 
- `16:07:10`   ✓ data/fred-cache.json: 200 
- `16:07:11`   ✓ flow-data.json: 200 
- `16:07:11`   ✓ crypto-intel.json: 200 
- `16:07:11` ✅   ALL 5/5 files now publicly accessible
- `16:07:11` Done
