# ops 4905 — deep CloudWatch autopsy · MIDAS hop2

**Status:** success  
**Duration:** 2.3s  
**Finished:** 2026-08-19T14:50:22+00:00  

## Data

| as_of | events_scanned | files | kept | lease_free | mode | n_complete | newest_part_age_min | next_hops | page | parts | stage | status |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 2026-08-19T12:39:35+00:00 |  |  |  | False | backfill | 30 | 4.6 |  |  | 238 | deep-now |  |
|  | 27 |  | 8 |  |  |  |  |  |  |  | autopsy |  |
|  |  | [] |  |  |  |  |  | ["/featured-topics/market-structure-analytics", "/data-research/sec-markets-data/market-structure-data-security-exchange"] | sec-markets-data |  | midas-hop2 | 200 |
|  |  | [] |  |  |  |  |  | ["/featured-topics/market-structure-analytics", "/securities-topics/market-structure-analytics/midas-market-information-data-analytics-system", "/securities-topics/market-structure-analytics/market-activity-data-visualizations", "/marketstructure/midas-system", "/featured-topics/market-structure-analytics/research-analysis-market-structur | mstr-analytics |  | midas-hop2 | 200 |

## Log
- `14:50:21` LOG[0] REPORT RequestId: fcc8876a-e66a-4a93-8a6d-0013fe724bb9	Duration: 80.30 ms	Billed Duration: 570 ms	Memory Size: 4096 MB	Max Memory Used: 97 MB	Init Duration: 488.91 ms	
XRAY TraceId: 1-6a85b706-4c00add334ddc12611a57efd	Se
- `14:50:21` LOG[1] REPORT RequestId: 276a85b3-61a7-4b68-b42e-de11b5f419e3	Duration: 900000.00 ms	Billed Duration: 900503 ms	Memory Size: 4096 MB	Max Memory Used: 537 MB	Init Duration: 502.37 ms	Status: timeout
XRAY TraceId: 1-6a85b38d-6f9d
- `14:50:21` LOG[2] REPORT RequestId: 276a85b3-61a7-4b68-b42e-de11b5f419e3	Duration: 84.77 ms	Billed Duration: 635 ms	Memory Size: 4096 MB	Max Memory Used: 97 MB	Init Duration: 549.71 ms	
XRAY TraceId: 1-6a85b38d-6f9d98433c41c43c27db5b48	Se
- `14:50:21` LOG[3] REPORT RequestId: 276a85b8-11a7-4b68-b42e-de11b5f419e3	Duration: 74.59 ms	Billed Duration: 75 ms	Memory Size: 4096 MB	Max Memory Used: 97 MB	
XRAY TraceId: 1-6a85b83d-0300aaff473d11d85c695ad6	SegmentId: 26dc16cbbd0c0bf5	
- `14:50:21` LOG[4] REPORT RequestId: 276a85bc-c1a7-4b68-b42e-de11b5f419e3	Duration: 70.75 ms	Billed Duration: 576 ms	Memory Size: 4096 MB	Max Memory Used: 97 MB	Init Duration: 504.81 ms	
XRAY TraceId: 1-6a85bced-2a574ebe6f4efb4250401214	Se
- `14:50:21` LOG[5] REPORT RequestId: 276a85ba-69a7-4b68-b42e-de11b5f419e3	Duration: 900000.00 ms	Billed Duration: 900493 ms	Memory Size: 4096 MB	Max Memory Used: 542 MB	Init Duration: 492.98 ms	Status: timeout
XRAY TraceId: 1-6a85ba95-77fd
- `14:50:21` LOG[6] REPORT RequestId: 276a85ba-69a7-4b68-b42e-de11b5f419e3	Duration: 428.59 ms	Billed Duration: 429 ms	Memory Size: 4096 MB	Max Memory Used: 97 MB	
XRAY TraceId: 1-6a85ba95-77fdc98c06ff23d257fcda8e	SegmentId: 2c33af94440ce3e
- `14:50:21` LOG[7] REPORT RequestId: 276a85bf-19a7-4b68-b42e-de11b5f419e3	Duration: 65.05 ms	Billed Duration: 66 ms	Memory Size: 4096 MB	Max Memory Used: 97 MB	
XRAY TraceId: 1-6a85bf45-69f72963358b720f2b158c31	SegmentId: 071d828eb7df1b97	
- `14:50:22` VERDICT: PASS_WITH_PENDING · {"autopsy_captured": "PASS", "deep_status": "PASS", "midas_hop2": "PENDING"}
- `14:50:22` report written: aws/ops/reports/4905.json
