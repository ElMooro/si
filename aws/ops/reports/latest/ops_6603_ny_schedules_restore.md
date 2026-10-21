# ops 6603 - the three New York-zone schedules: today's run accounted for, original times restored, manifest declared

**Status:** success  
**Duration:** 6.6s  
**Finished:** 2026-10-21T23:42:26+00:00  

## Data

| after | before | schedule | tz | verified |
|---|---|---|---|---|
| cron(10 12 ? * MON-FRI *) | cron(51 13 ? * MON-FRI *) | justhodl-auction-desk-buyback | America/New_York | True |
| cron(35 13 ? * MON-FRI *) | cron(56 13 ? * MON-FRI *) | justhodl-bond-warroom-after-auction | America/New_York | True |
| cron(15 16 ? * MON-FRI *) | cron(5 17 ? * MON-FRI *) | justhodl-master-ranker-weekday-close | America/New_York | True |

## Log
## 1. live state and today's firing (read-only)

- `23:42:20` justhodl-auction-desk-buyback: live cron(51 13 ? * MON-FRI *) tz America/New_York ENABLED target justhodl-auction-desk -> real UTC minute 17:51 today: 1 invocations in 6 min
- `23:42:20` justhodl-bond-warroom-after-auction: live cron(56 13 ? * MON-FRI *) tz America/New_York ENABLED target justhodl-bond-warroom -> real UTC minute 17:56 today: 1 invocations in 6 min
- `23:42:20` justhodl-master-ranker-weekday-close: live cron(5 17 ? * MON-FRI *) tz America/New_York ENABLED target justhodl-master-ranker -> real UTC minute 21:05 today: 1 invocations in 6 min
## 2. restore the original expressions

- `23:42:20` ✅ justhodl-auction-desk-buyback: cron(51 13 ? * MON-FRI *) -> cron(10 12 ? * MON-FRI *) (America/New_York; target/role/input/retry unchanged)
- `23:42:20` ✅ justhodl-bond-warroom-after-auction: cron(56 13 ? * MON-FRI *) -> cron(35 13 ? * MON-FRI *) (America/New_York; target/role/input/retry unchanged)
- `23:42:20` ✅ justhodl-master-ranker-weekday-close: cron(5 17 ? * MON-FRI *) -> cron(15 16 ? * MON-FRI *) (America/New_York; target/role/input/retry unchanged)
## 3. deployed manifest := committed

- `23:42:21` S3 1026 entries, repo 1026; added [] removed 0 changed ['justhodl-auction-desk-buyback', 'justhodl-bond-warroom-after-auction', 'justhodl-master-ranker-weekday-close']
- `23:42:21` ✅ manifest declared the restored times (backup backups/schedule-manifest/20261021T234220Z/)
## 4. polygon-futures-curves on the lane (read-only)

- `23:42:24` justhodl-polygon-futures-curves errors (Complete): ["[ERROR] RuntimeError: Native futures research failed; inspect retained request evidence Traceback (most recent call last): \u00a0\u00a0File \"/var/task/lambda_function.py\", line 36, in lambda_handler \u00a0\u00a0\u00a0\u00a0result=run(client,bucket,event.get('request_id') or event.get('id') or execution_id,execution_id, \u00a0\u00a0File \"/", "REPORT RequestId: 0f59d7d1-bba0-4e4f-a350-c6f996fb8a54\tDuration: 226.11 ms\tBilled Duration: 227 ms\tMemory Size: 512 MB\tMax Memory Used: 134 MB\t XRAY TraceId: 1-6ad92210-6ba7d9eb4b03b35452ce709f\tSegmentId: 2d109c8429a9e396\tSampled: true\t "]
- `23:42:26`   per hour (Complete): [["19:00", 6, 1327, 2771, "127000000"], ["20:00", 24, 1180, 2419, "134000000"]]
- `23:42:26`   config: ResourceNotFoundException
- `23:42:26` ✅ 3/3 New York schedules at their original times; manifest in step; futures-curves errors read
