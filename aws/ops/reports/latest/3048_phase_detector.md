## 1. Ensure engine

**Status:** success  
**Duration:** 34.2s  
**Finished:** 2026-07-10T05:02:32+00:00  

## Data

| analyzed | bad_dates | boards_sizes | code_age_min | completed_segments | dated_names | duration_s | env_copied | fn_exists | mega | n_fails | n_warns | need_fresh_deploy | page_live | phase_counts | polygon_vars | schedule | universe | verdict | with_events |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  |  |  |  |  |  | False |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  | True |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  | 50.7 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  | 3 |  |  |  |  |  |  |  | ["POLYGON_KEY"] |  |  |  |  |
| 683 |  | {"accumulation_beginning": 3, "accumulation_mature": 12, "accumulation_ended_markup": 3, "distribution_beginning": 4, "distribution_mature": 23, "distribution_ended_markdown": 5} |  |  |  | 11.3 |  |  |  |  |  |  |  | {"ACCUMULATION": 15, "DISTRIBUTION": 27, "MARKUP": 41, "MARKDOWN": 18, "NEUTRAL_RANGE": 21, "NEUTRAL": 561} |  |  | 700 |  |  |
|  | 0 |  |  | 473 | 119 |  |  |  | {"AAPL": {"phase": "NEUTRAL", "begin": null, "days": null}, "AMZN": {"phase": "NEUTRAL", "begin": null, "days": null}, "GOOGL": {"phase": "NEUTRAL", "begin": null, "days": null}, "MSFT": {"phase": "NEUTRAL", "begin": null, "days": null}, "NVDA": {"phase": "NEUTRAL", "begin": null, "days": null}} |  |  |  |  |  |  |  |  |  | 60 |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | created cron(10 22 * * ? *) |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  | True |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  | 0 | 0 |  |  |  |  |  |  | PASS |  |

## Log
## 1b. Repair env (deploy-lambdas created fn WITHOUT donor env -- 3047 evidence: POLYGON key missing, 3ms crash)

## 2. Segment the market (SYNC + log tail)

- `05:02:31` engine log tail:
START RequestId: 73a8a641-b9fb-4732-9281-12bdc26d7942 Version: $LATEST
[phase] 683 analyzed, counts={'ACCUMULATION': 15, 'DISTRIBUTION': 27, 'MARKUP': 41, 'MARKDOWN': 18, 'NEUTRAL_RANGE': 21, 'NEUTRAL': 561}, 11s
END RequestId: 73a8a641-b9fb-4732-9281-12bdc26d7942
REPORT RequestId: 73a8a641-b9fb-4732-9281-12bdc26d7942	Duration: 11455.17 ms	Billed Duration: 11983 ms	Memory Size: 2048 MB	Max Memory Used: 194 MB	Init Duration: 527.24 ms	
XRAY TraceId: 1-6a507cda-41ef8849326a781f7dfde971	SegmentId: d508d0a0b8c084fd	Sampled: true	

## 3. Daily schedule

## 4. Live page (warn-level)

## verdict

- `05:02:32` PASS -- 683 analyzed, counts {'ACCUMULATION': 15, 'DISTRIBUTION': 27, 'MARKUP': 41, 'MARKDOWN': 18, 'NEUTRAL_RANGE': 21, 'NEUTRAL': 561}
