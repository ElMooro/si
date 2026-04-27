# Phase 10 — wire detector for ALWAYS-FRESH Treasury data

**Status:** success  
**Duration:** 6.2s  
**Finished:** 2026-04-27T12:58:48+00:00  

## Log
## 1. Lambda health check

- `12:58:42`   ✅ Lambda exists: justhodl-auction-crisis-detector
- `12:58:42`      CodeSha256:   K+KjNqKTU2hnf54EWbu2K+FpcHEGeMDFx/lES3XbHwU=
- `12:58:42`      LastModified: 2026-04-27T12:58:37.000+0000
- `12:58:42`      State:        Active
- `12:58:42`      Runtime:      python3.12
- `12:58:42`      Timeout:      240s
- `12:58:42`      Memory:       1024MB
## 2. Schedule upgrade — old hourly → smart cron + backstop

- `12:58:42`   Removing old rule: justhodl-auction-crisis-refresh
- `12:58:42`     removed 1 target(s)
- `12:58:42`     ✅ deleted justhodl-auction-crisis-refresh
- `12:58:42`   Creating active-window rule: justhodl-auction-crisis-active
- `12:58:42`     ✅ justhodl-auction-crisis-active cron(0/15 14-22 ? * MON-FRI *) ENABLED
- `12:58:42`   Creating backstop rule: justhodl-auction-crisis-backstop
- `12:58:42`     ✅ justhodl-auction-crisis-backstop rate(4 hours) ENABLED
## 3. IAM permissions: events.amazonaws.com → Lambda

- `12:58:43`     ✅ permission added: AllowEventsActiveWindow
- `12:58:43`     ✅ permission added: AllowEventsBackstop
- `12:58:43`     ✅ removed old AllowEventBridgePhase10 permission
## 4. EB targets

- `12:58:43`     ✅ justhodl-auction-crisis-active → justhodl-auction-crisis-detector
- `12:58:43`     ✅ justhodl-auction-crisis-backstop → justhodl-auction-crisis-detector
## 5. Verify final wiring (read-back)

- `12:58:43`     justhodl-auction-crisis-active:
- `12:58:43`       Schedule: cron(0/15 14-22 ? * MON-FRI *)
- `12:58:43`       State:    ENABLED
- `12:58:43`       Targets:  1 → ['justhodl-auction-crisis-detector']
- `12:58:43`     justhodl-auction-crisis-backstop:
- `12:58:43`       Schedule: rate(4 hours)
- `12:58:43`       State:    ENABLED
- `12:58:43`       Targets:  1 → ['justhodl-auction-crisis-detector']
- `12:58:43`     Lambda permissions: ['AllowEventsActiveWindow', 'AllowEventsBackstop']
## 6. Force a fresh pull — verify Treasury data flowing

- `12:58:47`   ✅ invoke (3.8s)
- `12:58:47`      status:                        ok
- `12:58:47`      regime:                        CALM
- `12:58:47`      composite_score:               6.3
- `12:58:47`      n_recent (14d):                15
- `12:58:47`      latest_auction_date:           2026-04-23
- `12:58:47`      latest_cusip:                  91282CQP9
- `12:58:47`      hours_since_latest_auction:    109.0
- `12:58:47`      is_new_auction_this_run:       False
## 7. S3 output verification

- `12:58:48`   S3 file:          data/auction-crisis.json
- `12:58:48`   LastModified:     2026-04-27 12:58:48+00:00
- `12:58:48`   ContentLength:    11,283 bytes
- `12:58:48`   CacheControl:     max-age=600
- `12:58:48`   Age:              0.0 minutes (should be <2 minutes)
- `12:58:48`   ✅ S3 file fresh from this invoke
- `12:58:48` 
- `12:58:48`   Freshness section in S3 file:
- `12:58:48`     schema_version:               1.1
- `12:58:48`     latest_auction_date:          2026-04-23
- `12:58:48`     latest_cusip:                 91282CQP9
- `12:58:48`     hours_since_latest_auction:   109.0
- `12:58:48`     n_total_auctions_pulled:      483
- `12:58:48`     data_window:                  2025-03-23 → 2026-04-27
- `12:58:48`     fetched_via:                  https://api.fiscaldata.treasury.gov (no-cache headers, format=json)
## FINAL — wiring summary

- `12:58:48`   ✅ Lambda:           justhodl-auction-crisis-detector (Active)
- `12:58:48`   ✅ Active schedule:  cron(0/15 14-22 ? * MON-FRI *)
- `12:58:48`                        = every 15 min, Mon-Fri, 14:00-22:00 UTC
- `12:58:48`                        = covers Treasury bill (15:30 UTC) and note (17:00 UTC)
- `12:58:48`                          publication windows + 2hr post-auction settlement buffer
- `12:58:48`   ✅ Backstop:         rate(4 hours)
- `12:58:48`                        = weekend + off-hours backstop
- `12:58:48`   ✅ Output:           s3://justhodl-dashboard-live/data/auction-crisis.json
- `12:58:48`                        refreshed via no-cache fetch from fiscaldata.treasury.gov
- `12:58:48`                        freshness section shows latest auction date + staleness
- `12:58:48`   ✅ Pages:            /auction-crisis.html + summary on /bonds.html
- `12:58:48`                        both fetch with ?t=Date.now() cachebusting
- `12:58:48` 
- `12:58:48`   Within 15 minutes of any new Treasury auction result publication,
- `12:58:48`   the dashboard will reflect it.
- `12:58:48` Done
