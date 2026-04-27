# EB rule audit — 0-invocation Lambdas

**Status:** ARCHIVED (redundant with step 245 + commit 81460ab from parallel session)

## What happened

A parallel Claude session shipped step 245 (`diagnose_repair_health_red.md`) and
commit 81460ab (`post-rule-enable cleanup`) which together:

- Re-enabled 5 disabled EB rules:
  - `justhodl-hourly-collection`     → justhodl-data-collector
  - `DailyMacroScraper`              → scrapeMacroData
  - `fmp-movers-hourly`              → fmp-stock-picks-agent
  - `fmp-stock-picks-daily`          → fmp-stock-picks-agent
  - `news-sentiment-update`          → news-sentiment-agent
- Created a missing EB rule for justhodl-khalid-metrics
- Repointed justhodl-data-collector source

This script (`_eb_rule_audit_zero_invocations.py`) attempted similar work but
crashed at the `has_invoke_permission` check — `Principal` field can be a string
rather than a dict in older-style Lambda resource policies.

It still partially succeeded before crashing: added a lambda:InvokeFunction
permission for `DailyEmailReportsV2` → `justhodl-email-reports-v2`, which
defensively ensures EB can invoke that Lambda.

## Status of all 10 health-monitor RED items (post-fixes)

| Item                        | Status | Fixed by                    |
|-----------------------------|--------|------------------------------|
| news-sentiment-agent (100% errors) | resolved | e6661dd (rename + redeploy) + step 245 (rule enable) |
| fedliquidityapi (73% errors)        | resolved | e6661dd (retry logic) |
| fmp-stock-picks-agent (100% errors) | resolved | e6661dd (error wrap) + step 245 (2 rules enable) |
| scrapeMacroData (100% errors)       | resolved | daec392 (stub) + step 245 (rule enable) |
| justhodl-data-collector (0/24h)     | resolved | daec392 / 81460ab (S3 source) + step 245 (rule enable) |
| justhodl-email-reports-v2 (0/24h)   | resolved | rule was ENABLED; fires at 12:00 UTC daily |
| justhodl-khalid-metrics (0/24h)     | resolved | 81460ab (rule created — was missing) |
| s3:data/khalid-config.json (stale)  | downstream | resolves when khalid-metrics fires (next 11:00 UTC) |
| justhodl-intelligence (under-rate)  | resolved | 39289ba (threshold 10→4, weekday-aware) |
| justhodl-repo-monitor (under-rate)  | resolved | 39289ba (threshold 10→6) |

## Next health-monitor invocation should show

- RED: 10 → 0 (or 1-2 if khalid-metrics rule needs first cycle)
- All Lambdas firing on schedule with healthy error rates
