# ops 4899 — 214-set failures: zero unexplained

**Status:** success  
**Duration:** 10.3s  
**Finished:** 2026-08-18T19:52:13+00:00  

## Data

| alive | detail | ids | key | n | source_empty | stage | unresolved |
|---|---|---|---|---|---|---|---|
|  |  | DD,ECB.DISS:JVC_PUB,ECB.DISS:LCI_PUB,ECB.DISS:MOBILE_KEY_4,ECB.DISS:MOBILE_KEY_5,ECB.DISS:RESR_PUB,ECB.DISS:SUR_PUB |  | 7 |  | ledger |  |
| none | {"DD": {"code": 404, "class": "SOURCE_EMPTY"}, "ECB.DISS:JVC_PUB": {"code": 404, "class": "SOURCE_EMPTY"}, "ECB.DISS:LCI_PUB": {"code": 404, "class": "SOURCE_EMPTY"}, "ECB.DISS:MOBILE_KEY_4": {"code": 404, "class": "SOURCE_EMPTY"}, "ECB.DISS:MOBILE_KEY_5": {"code": 404, "class": "SOURCE_EMPTY"}, "ECB.DISS:RESR_PUB": {"code": 404, "class": "SOURCE_EMPTY"}, "ECB.DISS:SUR_PUB": {"code": 404, "class": |  |  |  | 7 | probe |  |
|  |  |  | data/warm/ecb/failures-classified.json |  |  | written | 0 |

## Log
- `19:52:13` VERDICT: PASS
- `19:52:13` report written: aws/ops/reports/4899.json
