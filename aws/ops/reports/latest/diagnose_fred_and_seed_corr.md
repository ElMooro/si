# Diagnose FRED silent failure + re-seed correlation-breaks

**Status:** success  
**Duration:** 10.5s  
**Finished:** 2026-04-26T21:53:13+00:00  

## Log
## 1. Test FRED API directly from AWS

- `21:53:08`   ✅ DGS10               n_obs=5  latest=2026-04-23=4.34
- `21:53:09` ⚠   ✗ WGMMNS              exception: HTTP Error 400: Bad Request
- `21:53:09`   ✅ SOFR                n_obs=5  latest=2026-04-23=3.65
- `21:53:09`   ✅ BUSLOANS            n_obs=5  latest=2026-03-01=2827.6247
- `21:53:09`   ✅ INTGSBJPM193N       n_obs=5  latest=2017-05-01=0.04
- `21:53:10`   ✅ DTWEXBGS            n_obs=5  latest=2026-04-17=118.0795
## 2. CloudWatch tail from latest justhodl-crisis-plumbing invoke

- `21:53:10`   stream: 2026/04/26/[$LATEST]e62510d4a749465c8f41037b32614781
- `21:53:11`   last 73 events:
- `21:53:11`     [FRED] DEXJPUS error: HTTP Error 429: Too Many Requests
- `21:53:11`     [FRED] IR3TBB01EZM156N error: HTTP Error 429: Too Many Requests
- `21:53:11`     [FRED] IORB error: HTTP Error 429: Too Many Requests
- `21:53:11`     [FRED] OBFR error: HTTP Error 429: Too Many Requests
- `21:53:11`     [FRED] BAMLC0A4CMTRIV error: HTTP Error 429: Too Many Requests
- `21:53:11`     [FRED] SOFR error: HTTP Error 429: Too Many Requests
- `21:53:11`     [FRED] T10YIE error: HTTP Error 429: Too Many Requests
- `21:53:11`     [FRED] BAMLH0A0HYM2 error: HTTP Error 429: Too Many Requests
- `21:53:11`     [FRED] DRTSCILM error: HTTP Error 429: Too Many Requests
- `21:53:11`     [FRED] DFII10 error: HTTP Error 429: Too Many Requests
- `21:53:11`     [crisis-plumbing] fetched 30 series in 0.7s
- `21:53:11`     [crisis-plumbing] done: {'status': 'ok', 'elapsed_sec': 0.8, 'composite_signal': 'NO_DATA', 'composite_score': None, 'n_indices': 0, 'n_flagged': 0, 's3_key': 'data/crisis-plumbing.json'}
- `21:53:11`     [crisis-plumbing] starting fetch at 2026-04-26T21:49:59.411745+00:00
- `21:53:11`     [FRED] NFCI error: HTTP Error 429: Too Many Requests
- `21:53:11`     [FRED] KCFSI error: HTTP Error 429: Too Many Requests
- `21:53:11`     [FRED] STLFSI4 error: HTTP Error 429: Too Many Requests
- `21:53:11`     [FRED] ANFCI error: HTTP Error 429: Too Many Requests
- `21:53:11`     [FRED] WGMMNS error: HTTP Error 429: Too Many Requests
- `21:53:11`     [FRED] BUSLOANS error: HTTP Error 429: Too Many Requests
- `21:53:11`     [FRED] WPMMNS error: HTTP Error 429: Too Many Requests
- `21:53:11`     [FRED] DPSACBW027SBOG error: HTTP Error 429: Too Many Requests
- `21:53:11`     [FRED] OFRFSI error: HTTP Error 429: Too Many Requests
- `21:53:11`     [FRED] RRPONTSYD error: HTTP Error 429: Too Many Requests
- `21:53:11`     [FRED] WTMMNS error: HTTP Error 429: Too Many Requests
- `21:53:11`     [FRED] DGS3MO error: HTTP Error 429: Too Many Requests
- `21:53:11`     [FRED] DEXUSEU error: HTTP Error 429: Too Many Requests
- `21:53:11`     [FRED] DGS10 error: HTTP Error 429: Too Many Requests
- `21:53:11`     [FRED] WTREGEN error: HTTP Error 429: Too Many Requests
- `21:53:11`     [FRED] DEXJPUS error: HTTP Error 429: Too Many Requests
- `21:53:11`     [FRED] T10Y2Y error: HTTP Error 429: Too Many Requests
- `21:53:11`     [FRED] T10Y3M error: HTTP Error 429: Too Many Requests
- `21:53:11`     [FRED] OBFR error: HTTP Error 429: Too Many Requests
- `21:53:11`     [FRED] INTGSBJPM193N error: HTTP Error 429: Too Many Requests
- `21:53:11`     [FRED] SOFR error: HTTP Error 429: Too Many Requests
- `21:53:11`     [FRED] IR3TBB01EZM156N error: HTTP Error 429: Too Many Requests
- `21:53:11`     [FRED] IORB error: HTTP Error 429: Too Many Requests
- `21:53:11`     [FRED] DTB3 error: HTTP Error 429: Too Many Requests
- `21:53:11`     [FRED] DTWEXBGS error: HTTP Error 429: Too Many Requests
- `21:53:11`     [FRED] BAMLH0A0HYM2 error: HTTP Error 429: Too Many Requests
- `21:53:11`     [FRED] BAMLC0A4CMTRIV error: HTTP Error 429: Too Many Requests
- `21:53:11`     [FRED] T10YIE error: HTTP Error 429: Too Many Requests
- `21:53:11`     [FRED] DRTSCILM error: HTTP Error 429: Too Many Requests
- `21:53:11`     [FRED] DFII10 error: HTTP Error 429: Too Many Requests
- `21:53:11`     [crisis-plumbing] fetched 30 series in 0.5s
- `21:53:11`     [crisis-plumbing] done: {'status': 'ok', 'elapsed_sec': 0.6, 'composite_signal': 'NO_DATA', 'composite_score': None, 'n_indices': 0, 'n_flagged': 0, 's3_key': 'data/crisis-plumbing.json'}
## 3. Re-invoke justhodl-crisis-plumbing (fresh attempt)

- `21:53:12`   invoke (1.6s): {"statusCode": 200, "body": "{\"status\": \"ok\", \"elapsed_sec\": 1.5, \"composite_signal\": \"NORMAL\", \"composite_score\": 37.0, \"n_indices\": 4, \"n_flagged\": 0, \"s3_key\": \"data/crisis-plumbing.json\"}"}
- `21:53:12`   fresh S3: crisis=4/5  plumbing=4/7  funding+credit=5/6  xcc=3/4
## 4. Re-seed justhodl-correlation-breaks

- `21:53:13`   state: Active  reason: —
- `21:53:13`   ✅ OK (0.4s)
- `21:53:13`   payload: {"statusCode": 200, "body": "{\"status\": \"warming_up\", \"n_dates\": 0}"}
## 5. data/correlation-breaks.json status

- `21:53:13`   bytes: 712
- `21:53:13`   schema_version: 1.0
- `21:53:13`   status: warming_up
- `21:53:13`   warming_up: Insufficient aligned data: 0 dates (need ≥312)
## DONE

- `21:53:13`   Done
