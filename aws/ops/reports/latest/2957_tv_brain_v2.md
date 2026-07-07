## 1. SSM bootstrap

**Status:** success  
**Duration:** 25.6s  
**Finished:** 2026-07-07T03:30:41+00:00  

## Data

| brain_upserted | cleanup_ok | health_ok | ingest_url | mirror_added | mirror_count | sentinel_in_mirror | sentinel_status | summary | token_ok | uid_ok |
|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  |  |  | True | True |
|  |  |  | https://w4osroryszvlifgk4boofkh7cm0selzf.lambda-url.us-east-1.on.aws |  |  |  |  |  |  |  |
|  |  | True |  |  | 0 |  |  |  |  |  |
| 1 |  |  |  | 1 |  |  | 200 |  |  |  |
|  |  |  |  |  |  | True |  |  |  |  |
|  | True |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  | tv-brain-v2: url=w4osroryszvlifgk4boofkh7cm0sel sentinel_brain=True mirror=True brain_compiler_wired=True |  |  |

## Log
## 2. Deploy tv-notes-ingest

- `03:30:16`   zip: 3228 bytes
## 1. Lambda

- `03:30:16`   Lambda exists — updating
- `03:30:21` ✅   ✓ updated justhodl-tv-notes-ingest
## 3. Publish tv-ingest-config.json

- `03:30:22` ✅ config published
## 4. Wire brain-compiler → TV notes merge

- `03:30:22` ✅ brain-compiler: TV notes merge wired
- `03:30:22`   zip: 5501 bytes
## 1. Lambda

- `03:30:22`   Lambda exists — updating
- `03:30:25` ✅   ✓ updated justhodl-brain-compiler
- `03:30:25` ✅   ✓ Function URL: https://nzoe4a43sjqxv3wovbac6dpvs40kxzgy.lambda-url.us-east-1.on.aws/
## 3. Smoke test

- `03:30:25`   invoking justhodl-brain-compiler…
## 5. Live round-trip

## 6. Status feed

- `03:30:41` ✅ status feed published
- `03:30:41` ✅ TV Notes → Brain v2 pipeline fully live
