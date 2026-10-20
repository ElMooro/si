# ops 6549 - deny the Lightpanda crawler at the bucket (making room in the policy first)

**Status:** success  
**Duration:** 9.8s  
**Finished:** 2026-10-20T16:49:14+00:00  

## Log
- `16:49:04` live policy: 34 statements, 20366 bytes (S3 limit 20,480)
- `16:49:04` ✅ rollback copy: s3://justhodl-ops-private-857687956942/backups/bucket-policy/justhodl-dashboard-live-20261020T164904Z.json
- `16:49:04` merged 25 Allow statements into 1; permission tuples identical to before: True
- `16:49:04` new policy: 11 statements, 18168 bytes; only difference: +1 Deny tuple
- `16:49:08` probe set: 41 objects covering 40 Allow patterns; before: {200: 34, 403: 7}
- `16:49:11` ✅ put + read-back: 11 statements
- `16:49:14` after: 41 of 41 objects unchanged; differing: none
- `16:49:14` Lightpanda UA -> 403 (want 403); browser UA -> 200 (want 200)
- `16:49:14` ✅ Lightpanda is refused (403); every public object still answers exactly as before
