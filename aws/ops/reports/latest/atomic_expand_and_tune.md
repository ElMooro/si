# Atomic expand + tune (one CI invocation, no race)

**Status:** success  
**Duration:** 35.2s  
**Finished:** 2026-04-25T01:33:17+00:00  

## Data

| auto_derived | dormant_lambdas | hand_curated | high_error_lambdas | threshold_tuned | total_components |
|---|---|---|---|---|---|
| 49 | 4 | 29 | 7 | 4 | 78 |

## Log
- `01:32:43`   Hand-curated baseline: 29 entries
- `01:32:43`   Lambdas with enabled schedules: 61
## Auto-deriving entries

- `01:32:43`   Auto-derived Lambda entries: 41
- `01:32:44`   Auto-derived S3 entries: 8
## Tuning auto-derived entries against 7d observed metrics

- `01:33:04`   Dormant: 4
- `01:33:04`     justhodl-daily-macro-report
- `01:33:04`     autonomous-ai-processor
- `01:33:04`     justhodl-email-reports
- `01:33:04`     justhodl-valuations-agent
- `01:33:04` 
  HIGH ERROR (100% err 7d): 7
- `01:33:04`     ecb-data-daily-updater: inv=21 err=21
- `01:33:04`     treasury-auto-updater: inv=6 err=6
- `01:33:04`     global-liquidity-agent-v2: inv=439 err=439
- `01:33:04`     scrapeMacroData: inv=21 err=21
- `01:33:04`     daily-liquidity-report: inv=21 err=21
- `01:33:04`     fmp-stock-picks-agent: inv=90 err=90
- `01:33:04`     news-sentiment-agent: inv=439 err=439
- `01:33:04` 
  Threshold tuned: 4
## Write expectations.py

- `01:33:04` ✅   Wrote: 26,907 bytes, 78 entries
## Re-deploy + verify

- `01:33:08` ✅   Re-deployed: 8925 bytes
- `01:33:17` ✅   Invoke clean
- `01:33:17` 
  System: red
- `01:33:17`   Counts: {'green': 60, 'yellow': 2, 'red': 14, 'info': 2, 'unknown': 0}
- `01:33:17`   Total: 78
- `01:33:17` 
  Currently-RED components (real bugs to fix):
- `01:33:17`     [important   ] lambda:daily-liquidity-report                       error rate 100.0% exceeds 30%
- `01:33:17`     [important   ] lambda:ecb-data-daily-updater                       error rate 100.0% exceeds 30%
- `01:33:17`     [important   ] lambda:fmp-stock-picks-agent                        error rate 100.0% exceeds 30%
- `01:33:17`     [important   ] lambda:global-liquidity-agent-v2                    only 5 invocations in 24h (expected ≥31)
- `01:33:17`     [important   ] lambda:justhodl-data-collector                      error rate 100.0% exceeds 30%
- `01:33:17`     [important   ] lambda:justhodl-email-reports-v2                    only 0 invocations in 24h (expected ≥1)
- `01:33:17`     [important   ] lambda:news-sentiment-agent                         only 5 invocations in 24h (expected ≥31)
- `01:33:17`     [important   ] lambda:scrapeMacroData                              error rate 100.0% exceeds 30%
- `01:33:17`     [important   ] lambda:treasury-auto-updater                        only 0 invocations in 24h (expected ≥1)
- `01:33:17`     [nice_to_have] lambda:alphavantage-market-agent                    only 10 invocations in 24h (expected ≥18)
- `01:33:17`     [nice_to_have] lambda:ecb-auto-updater                             only 0 invocations in 24h (expected ≥1)
- `01:33:17`     [nice_to_have] lambda:enhanced-repo-agent                          only 23 invocations in 24h (expected ≥33)
- `01:33:17`     [nice_to_have] lambda:xccy-basis-agent                             only 23 invocations in 24h (expected ≥33)
- `01:33:17`     [nice_to_have] s3:data/khalid-config.json                          
- `01:33:17` Done
