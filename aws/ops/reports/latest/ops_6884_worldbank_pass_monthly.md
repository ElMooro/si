# 6884 - World Bank full pass weekly -> monthly (DRY RUN), 2026-11-21T18:47Z

**Status:** success  
**Duration:** 1.6s  
**Finished:** 2026-11-21T18:47:31+00:00  

## Data

| dry_run | expr_now | manifest | status |
|---|---|---|---|
| True | rate(7 days) | patched | would_apply |

## Log
## 1. Live state

- `18:47:30` justhodl-worldbank-full-weekly: rate(7 days) | ENABLED | timezone UTC | target justhodl-worldbank-full | payload {"redrain": true} | window OFF
- `18:47:30` justhodl-worldbank-full-2h (not touched): cron(20 3 * * ? *) | ENABLED | payload {}
- `18:47:31` justhodl-worldbank-full: timeout 850 s, memory 1024 MB, last modified 2026-11-03T06:08:20
## 2. The change

- `18:47:31` retime: 1 items
- `18:47:31` disable: 0 items
- `18:47:31` schedule: rate(7 days) -> cron(0 1 1 * ? *) : [{"kind": "schedule", "name": "justhodl-worldbank-full-weekly", "op": "retime", "status": "would_apply"}]
- `18:47:31` read-back: rate(7 days) | ENABLED | target justhodl-worldbank-full | payload {"redrain": true}
## 3. The declaration (config/schedule-manifest.json), this one entry only

- `18:47:31` live manifest: patched (337510 bytes); repo copy declares rate(7 days)
## 4. What this is worth

- `18:47:31` measured (ops 6876): 240 runs x 677 s at 1,024 MB in a week = one pass = about $2.77 of Lambda; $11.60 a month at one pass a week
- `18:47:31` at one pass a month: about $2.77 a month of Lambda - about $8.80 a month less.  The engine's writes (about $2.50 a month today) fall with it.
- `18:47:31` the price: World Bank series can be up to a month behind the source instead of a week (annual and quarterly data)
- `18:47:31` ✅ dry run: nothing changed.  ops 6885 applies exactly this plan
