# ops 6715 - justhodl-scheduler-role may invoke treasury-api (one inline grant)

**Status:** success  
**Duration:** 0.5s  
**Finished:** 2026-11-02T16:36:27+00:00  

## Log
- `16:36:27` role justhodl-scheduler-role trust: [{'Service': 'scheduler.amazonaws.com'}]
- `16:36:27` inline invoke-justhodl-lambdas: [["Allow", "lambda:InvokeFunction", ["arn:aws:lambda:us-east-1:857687956942:function:justhodl-*"]]]
- `16:36:27` put inline scheduler-invoke-treasury-api: {"Version": "2012-10-17", "Statement": [{"Sid": "TreasuryApiHistoryUpdate", "Effect": "Allow", "Action": "lambda:InvokeFunction", "Resource": ["arn:aws:lambda:us-east-1:857687956942:function:treasury-api", "arn:aws:lambda:us-east-1:857687956942:function:treasury-api:*"]}]}
- `16:36:27` ✅ simulation: treasury-api allowed | ofrapi (must stay denied) implicitDeny
- `16:36:27` ✅ ops 6715 complete
