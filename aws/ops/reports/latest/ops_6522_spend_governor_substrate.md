# ops 6522 - spend governor substrate

**Status:** success  
**Duration:** 16.7s  
**Finished:** 2026-10-16T20:18:17+00:00  

## Log
- `20:18:01` ✅ bucket justhodl-ops-private-857687956942 created
- `20:18:02` ✅ role justhodl-spend-governor-role created
- `20:18:03` ✅ topic arn:aws:sns:us-east-1:857687956942:justhodl-spend-governor ready (Budgets may publish)
- `20:18:03` ✅ budget end date 2027-10-01 00:00:00+00:00 -> open-ended
- `20:18:05` ✅ SNS subscriber added to: ACTUAL 100.0%, ACTUAL 50.0%, ACTUAL 80.0%, FORECASTED 100.0%
- `20:18:05` ✅ 90%% ACTUAL notification added (email + SNS)
- `20:18:06` ✅ fleet-alerts email channel: {"r***": "confirmation email sent"}
- `20:18:06` ✅ control.json written (mode=monitor, Oct override $3500)
- `20:18:07` orphan policy JustHodl-Budget-Exceeded-Deny attachments: 0 (left untouched)
- `20:18:17` ✅ wrote aws/ops/reports/6522.json
