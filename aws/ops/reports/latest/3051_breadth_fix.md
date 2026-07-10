## 1. Wait for this push's deploy

**Status:** success  
**Duration:** 30.7s  
**Finished:** 2026-07-10T13:10:57+00:00  

## Data

| coppock_state | coppock_value | deployed_at | episodes | explainer_none | fwd12m_n | fwd12m_ret | n_fails | n_warns | need_fresh_deploy | page_no_setup | spy_history_n | verdict | whaley |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  |  |  | True |  |  |  |  |
|  |  | 2026-07-10T13:10:42.000+0000 |  |  |  |  |  |  |  |  |  |  |  |
| POSITIVE | 42.88 |  | 8 | False | 8 | 19.72 |  |  |  |  | 5026 |  | {"state": "BULLISH", "first_5d_return_pct": 0.93} |
|  |  |  |  |  |  |  |  |  |  | False |  |  |  |
|  |  |  |  |  |  |  | 0 | 1 |  |  |  | PASS |  |

## Log
## 2. Sync run + log tail

- `13:10:56` tail:
START RequestId: c84fd17f-416d-4531-9f94-dbffee24e901 Version: $LATEST
breadth cache: 46 entries, 1 newly fetched
spy_history rows: 5026
breadth-thrust: state=NULL ema=0.5109 whaley=BULLISH coppock=POSITIVE sig=25 n_hist=8
END RequestId: c84fd17f-416d-4531-9f94-dbffee24e901
REPORT RequestId: c84fd17f-416d-4531-9f94-dbffee24e901	Duration: 1126.09 ms	Billed Duration: 1675 ms	Memory Size: 512 MB	Max Memory Used: 111 MB	Init Duration: 548.91 ms	
XRAY TraceId: 1-6a50ef5e-139b4d1c2c275d8b7605d43d	SegmentId: 5d886b996f1cd6ff	Sampled: true	

## 3. Assert the five symptoms are gone

## 4. Page NO-SETUP banner (warn-level, CDN lag)

## verdict

- `13:10:57` PASS -- breadth-thrust healed
