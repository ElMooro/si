# ops 6626 - refuse Applebot's direct S3 downloads of the data files

**Status:** success  
**Duration:** 30.6s  
**Finished:** 2026-10-23T18:10:10+00:00  

## Data

| applebot | changed | probes | safari |
|---|---|---|---|
| 403 | True | 51 | 200 |

## Log
- `18:09:40` live policy: 11 statements, 18168 bytes (S3 limit 20,480)
- `18:09:53` probe set: 51 objects (40 Allow patterns + the why.html files); Safari before: {200: 44, 403: 7}; Applebot before: 200
- `18:09:54` ✅ rollback copy: s3://justhodl-ops-private-857687956942/backups/bucket-policy/justhodl-dashboard-live-20261023T180953Z.json
- `18:09:58` ✅ put + read-back: 11 statements, 18194 bytes
- `18:10:10` Safari probes after: 51 of 51 unchanged; differing: none
- `18:10:10` data/best-setups.json -> Applebot 403 (want 403), Lightpanda 403 (want 403), Googlebot 200, Safari 200
- `18:10:10` ✅ Applebot's direct downloads are refused (403); every public object still answers a browser exactly as before
