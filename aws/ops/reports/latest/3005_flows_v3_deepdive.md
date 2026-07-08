## 1. IR deploy gate + flows v3

**Status:** success  
**Duration:** 116.2s  
**Finished:** 2026-07-08T02:57:21+00:00  

## Data

| engine-robustness | flows_joined | ici-flows | sample | warn |
|---|---|---|---|---|
|  | 35 |  | [{"etf": "XBI", "flow_21d_usd": 962048982.85, "flow_label": null}, {"etf": "CIBR", "flow_21d_usd": 43014000.0, "flow_label": null}, {"etf": "IBB", "flow_21d_usd": 437760339.25, "flow_label": null}, {"etf": "XLV", "flow_21d_usd": 585113585.4000001, "flow_label": null}, {"etf": "IY | fund_flows joined: 35/40 |
| {"secs": 1.1, "fn_error": null, "body": "{\"statusCode\": 200, \"body\": \"{\\\"ok\\\": true, \\\"no_action\\\": \\\"no_engines_with_sufficient_history\\\", \\\"min_snapshots_required\\\": 10}\"}"} |  |  |  |  |
|  |  | {"secs": 8.4, "fn_error": "Unhandled", "body": "{\"errorMessage\": \"no ICI data (mmf=0 ltf=0) \\u2014 seed histories\", \"errorType\": \"RuntimeError\", \"requestId\": \"e1bc21df-0230-47c3-964d-8b66cb27f83c\", \"stackTrace\": [\"  File \\\"/var/task/lambda_function.py\\\", line 281, in lambda_handl |  |  |

## Log
## 2. Suspect sync invokes (error capture)

- `02:57:21` ✅ flows 35/40 | suspects: {"justhodl-engine-robustness": "ok", "justhodl-ici-flows": "Unhandled"}
- `02:57:21` FAILS=0
