# EventBridge rule cleanup — LIVE

**Status:** success  
**Duration:** 4.3s  
**Finished:** 2026-04-23T22:02:12+00:00  

## Data

| deleted | errors | skipped |
|---|---|---|
| 10 | 0 | 0 |

## Log
## Plan

- `22:02:08`   10 rules queued for deletion
- `22:02:08` 
- `22:02:08` 
  → justhodl-daily-8am  (expect → justhodl-daily-report-v3)  [duplicate of v9-morning]
- `22:02:08`     State: ENABLED, Targets: [justhodl-daily-report-v3]
- `22:02:08` ✅     ✓ Deleted
- `22:02:08` 
  → justhodl-daily-v3  (expect → justhodl-daily-report-v3)  [duplicate; v9-auto-refresh covers]
- `22:02:09`     State: ENABLED, Targets: [justhodl-daily-report-v3]
- `22:02:09` ✅     ✓ Deleted
- `22:02:09` 
  → justhodl-crypto-15min  (expect → justhodl-crypto-intel)  [duplicate of crypto-intel-schedule]
- `22:02:09`     State: ENABLED, Targets: [justhodl-crypto-intel]
- `22:02:09` ✅     ✓ Deleted
- `22:02:09` 
  → justhodl-ml-schedule  (expect → justhodl-ml-predictions)  [duplicate of ml-predictions-schedule]
- `22:02:09`     State: ENABLED, Targets: [justhodl-ml-predictions]
- `22:02:10` ✅     ✓ Deleted
- `22:02:10` 
  → liquidity-critical-monitor  (expect → global-liquidity-agent-v2)  [DISABLED; retired]
- `22:02:10`     State: DISABLED, Targets: [global-liquidity-agent-v2]
- `22:02:10` ✅     ✓ Deleted
- `22:02:10` 
  → liquidity-daily-8am  (expect → global-liquidity-agent-v2)  [DISABLED; retired]
- `22:02:10`     State: DISABLED, Targets: [global-liquidity-agent-v2]
- `22:02:10` ✅     ✓ Deleted
- `22:02:10` 
  → liquidity-daily-report  (expect → global-liquidity-agent-v2)  [DISABLED; retired]
- `22:02:11`     State: DISABLED, Targets: [global-liquidity-agent-v2]
- `22:02:11` ✅     ✓ Deleted
- `22:02:11` 
  → liquidity-daily-report-v2  (expect → global-liquidity-agent-v2)  [DISABLED; retired]
- `22:02:11`     State: DISABLED, Targets: [global-liquidity-agent-v2]
- `22:02:11` ✅     ✓ Deleted
- `22:02:11` 
  → liquidity-hourly-v2  (expect → global-liquidity-agent-v2)  [DISABLED; retired]
- `22:02:11`     State: DISABLED, Targets: [global-liquidity-agent-v2]
- `22:02:12` ✅     ✓ Deleted
- `22:02:12` 
  → liquidity-news-v2  (expect → global-liquidity-agent-v2)  [DISABLED; retired]
- `22:02:12`     State: DISABLED, Targets: [global-liquidity-agent-v2]
- `22:02:12` ✅     ✓ Deleted
## Summary

- `22:02:12`   Deleted: 10
- `22:02:12`   Skipped: 0
- `22:02:12`   Errors:  0
- `22:02:12` 
  Rollback reference (rules deleted this run):
- `22:02:12`     justhodl-daily-8am                       state_was=ENABLED  targets=['arn:aws:lambda:us-east-1:857687956942:function:justhodl-daily-report-v3']
- `22:02:12`     justhodl-daily-v3                        state_was=ENABLED  targets=['arn:aws:lambda:us-east-1:857687956942:function:justhodl-daily-report-v3']
- `22:02:12`     justhodl-crypto-15min                    state_was=ENABLED  targets=['arn:aws:lambda:us-east-1:857687956942:function:justhodl-crypto-intel']
- `22:02:12`     justhodl-ml-schedule                     state_was=ENABLED  targets=['arn:aws:lambda:us-east-1:857687956942:function:justhodl-ml-predictions']
- `22:02:12`     liquidity-critical-monitor               state_was=DISABLED  targets=['arn:aws:lambda:us-east-1:857687956942:function:global-liquidity-agent-v2']
- `22:02:12`     liquidity-daily-8am                      state_was=DISABLED  targets=['arn:aws:lambda:us-east-1:857687956942:function:global-liquidity-agent-v2']
- `22:02:12`     liquidity-daily-report                   state_was=DISABLED  targets=['arn:aws:lambda:us-east-1:857687956942:function:global-liquidity-agent-v2']
- `22:02:12`     liquidity-daily-report-v2                state_was=DISABLED  targets=['arn:aws:lambda:us-east-1:857687956942:function:global-liquidity-agent-v2']
- `22:02:12`     liquidity-hourly-v2                      state_was=DISABLED  targets=['arn:aws:lambda:us-east-1:857687956942:function:global-liquidity-agent-v2']
- `22:02:12`     liquidity-news-v2                        state_was=DISABLED  targets=['arn:aws:lambda:us-east-1:857687956942:function:global-liquidity-agent-v2']
- `22:02:12` Done
