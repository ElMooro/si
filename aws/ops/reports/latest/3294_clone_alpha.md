- `04:42:28`   zip: 78593 bytes
## 1. Lambda
**Status:** success  
**Duration:** 190.0s  
**Finished:** 2026-07-14T04:45:37+00:00  

## Data

| aqr | berkshire | bridgewater | citadel | donor_env_keys | fails | filings | pct | prices_pending | renaissance | scored_n | status | two_sigma | warns |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  | 8 |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  | 221/221 | 100.0 | 0 |  |  | COMPLETE |  | [] |
|  |  |  |  |  |  |  |  |  |  | 17 |  |  |  |
|  | FAMOUS ≠ SKILLED score=0 a=-21.34 hit=0.22 n=9 |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  | FAMOUS ≠ SKILLED score=21 a=-5.93 hit=0.25 n=12 |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  | WORTH CLONING score=93 a=34.2 hit=0.67 n=12 |  |  |  |  |
| FAMOUS ≠ SKILLED score=0 a=-30.07 hit=0.18 n=11 |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  | FAMOUS ≠ SKILLED score=31 a=-31.14 hit=0.58 n=12 |  |
|  |  |  | FAMOUS ≠ SKILLED score=18 a=-19.62 hit=0.33 n=12 |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  | [] |  |  |  |  |  |  |  | [] |

## Log

- `04:42:28`   Lambda missing — creating
- `04:42:33` ✅   ✓ created justhodl-13f-clone-alpha
- `04:42:34` ✅   ✓ Function URL: https://idhtqe5ncgbo7csqz3lxai73uu0mnxev.lambda-url.us-east-1.on.aws/
## 2. Weekly Scheduler

- `04:42:34`   created cron(30 8 ? * MON *) UTC
## 3. Kick backfill + watch convergence

- `04:43:11`   pct 0.0 -> 13.6 (hop 0, filings 30/221)
- `04:43:47`   pct 13.6 -> 27.1 (hop 1, filings 60/221)
- `04:44:24`   pct 27.1 -> 40.7 (hop 2, filings 90/221)
- `04:44:42`   pct 40.7 -> 54.3 (hop 3, filings 120/221)
- `04:45:01`   pct 54.3 -> 67.9 (hop 4, filings 150/221)
- `04:45:19`   pct 67.9 -> 81.4 (hop 5, filings 180/221)
- `04:45:37`   pct 81.4 -> 100.0 (hop 7, filings 221/221)
## 4. 13f.html board live

- `04:45:37`   board markers live
- `04:45:37` OPS 3294 PASS — skill now measurable; famous money no longer rides for free.
