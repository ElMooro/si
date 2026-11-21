# 6885 - World Bank full pass weekly -> monthly (ACTING), 2026-11-21T18:48Z

**Status:** success  
**Duration:** 3.1s  
**Finished:** 2026-11-21T18:48:55+00:00  

## Data

| dry_run | expr_now | manifest | status |
|---|---|---|---|
| False | cron(0 1 1 * ? *) | patched | applied |

## Log
## 1. Live state

- `18:48:52` justhodl-worldbank-full-weekly: rate(7 days) | ENABLED | timezone UTC | target justhodl-worldbank-full | payload {"redrain": true} | window OFF
- `18:48:52` justhodl-worldbank-full-2h (not touched): cron(20 3 * * ? *) | ENABLED | payload {}
- `18:48:53` justhodl-worldbank-full: timeout 850 s, memory 1024 MB, last modified 2026-11-03T06:08:20
## 2. The change

- `18:48:53` retime: 1 items
- `18:48:53` disable: 0 items
- `18:48:53` schedule: rate(7 days) -> cron(0 1 1 * ? *) : [{"kind": "schedule", "name": "justhodl-worldbank-full-weekly", "op": "retime", "status": "applied"}]
- `18:48:54` read-back: cron(0 1 1 * ? *) | ENABLED | target justhodl-worldbank-full | payload {"redrain": true}
## 3. The declaration (config/schedule-manifest.json), this one entry only

- `18:48:54` live manifest: patched (337510 bytes); repo copy declares cron(0 1 1 * ? *)
- `18:48:55` ✅ live manifest declares cron(0 1 1 * ? *) for justhodl-worldbank-full-weekly (backup backups/schedule-manifest/20261121T184853Z/); read-back equal
## 4. What this is worth

- `18:48:55` measured (ops 6876): 240 runs x 677 s at 1,024 MB in a week = one pass = about $2.77 of Lambda; $11.60 a month at one pass a week
- `18:48:55` at one pass a month: about $2.77 a month of Lambda - about $8.80 a month less.  The engine's writes (about $2.50 a month today) fall with it.
- `18:48:55` the price: World Bank series can be up to a month behind the source instead of a week (annual and quarterly data)
- `18:48:55` ✅ World Bank full pass now starts at 01:00 UTC on the 1st of each month; next pass 2026-12-01
