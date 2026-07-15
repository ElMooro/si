## VERDICT

**Status:** success  
**Duration:** 1000.7s  
**Finished:** 2026-07-15T02:42:15+00:00  

## Data

| RESULT | issues | justhodl-52wk-quality-breakout | justhodl-buyback-scanner | justhodl-insider-sell-cluster | justhodl-rating-change-cluster | justhodl-sellside-views | justhodl-starmine |
|---|---|---|---|---|---|---|---|
|  |  |  |  |  | {'fn_error': None, 'body': '{"statusCode": 200, "body": "{\\"ok\\": true, \\"state\\": \\"CLUSTER_BUY_ACTIVE\\", \\"buy_picks\\": 2, \\"sell_picks\\": 0, \\"run_seconds\\": 1.9}"}', 'out': {'size': 3977, 'modified': '2026-07-15T02:25:39+00:00'}, 'fresh': True} |  |  |
|  |  |  |  |  |  | {'fn_error': None, 'body': '{"statusCode": 200, "headers": {"Content-Type": "application/json"}, "body": "{\\"ok\\": true, \\"median_target\\": 6900, \\"bias\\": \\"NEUTRAL\\", \\"n_firms\\": 15, \\"n_revisions\\": 0}"}', 'out': {'size': 1716, 'modified': '2026-07-15T02:25:43+00:00'}, 'fresh': True} |  |
|  |  | {'fn_error': None, 'body': '{"statusCode": 200, "body": "{\\"ok\\": true, \\"state\\": \\"QUALITY_BREAKOUT_RICH\\", \\"picks\\": 8, \\"high_quality\\": 2, \\"run_seconds\\": 2.0}"}', 'out': {'size': 9996, 'modified': '2026-07-15T02:25:50+00:00'}, 'fresh': True} |  |  |  |  |  |
|  |  |  |  |  |  |  | {'err': 'Read timeout on endpoint URL: "https://lambda.us-east-1.amazonaws.com/2015-03-31/functions/justhodl-starmine/invocations'} |
|  |  |  | {'fn_error': None, 'body': '{"statusCode": 200, "body": "{\\"state\\": \\"NORMAL\\", \\"n_fresh\\": 3, \\"n_opportunities\\": 14, \\"signal_strength\\": 100.0, \\"top\\": \\"SRXH\\"}"}', 'out': {'size': 30260, 'modified': '2026-07-15T02:42:08+00:00'}, 'fresh': True} |  |  |  |  |
|  |  |  |  | {'fn_error': None, 'body': '{"statusCode": 200, "body": "{\\"ok\\": true, \\"state\\": \\"QUIET\\", \\"n_clusters\\": 1}"}', 'out': {'size': 2986, 'modified': '2026-07-15T02:42:13+00:00'}, 'fresh': True} |  |  |  |
| PARTIAL | 1 |  |  |  |  |  |  |

## Log
- `02:42:15` ⚠ justhodl-starmine: Read timeout on endpoint URL: "https://lambda.us-east-1.amazonaws.com/2015-03-31/functions/justhodl-starmine/invocations
