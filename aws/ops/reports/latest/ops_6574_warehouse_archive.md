# ops 6574 - warehouse archive: sealed bucket, replication of the whole live bucket, backfill, proof

**Status:** success  
**Duration:** 32.9s  
**Finished:** 2026-10-20T23:39:48+00:00  

## Data

| archive | backfill | canary_proof | replication | seal |
|---|---|---|---|---|
| justhodl-warehouse-archive-857687956942 | ops 6575 (after the version cleanup) | True | enabled | deny Delete* except root |

## Log
- `23:39:15` archive bucket exists
- `23:39:16` archive lifecycle: keep 3 old copies, abort stray uploads after 7 days
- `23:39:16` archive: versioning Enabled, public access blocked, SSE-S3
- `23:39:17` replication role exists
- `23:39:17` replication rule already in place
- `23:39:17` ✅ archive policy sealed: deletes and weakening denied to everyone but the account root
- `23:39:48` ✅ canary replicated in 30 s (source status COMPLETED)
- `23:39:48` ✅ delete attempt on the archive copy: AccessDenied (the seal holds)
- `23:39:48` ✅ archive in place
