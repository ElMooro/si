# Step 83 — Deploy justhodl-health-monitor Lambda

**Status:** success  
**Duration:** 11.5s  
**Finished:** 2026-04-25T00:58:39+00:00  

## Data

| lambda_name | next_step | zip_bytes |
|---|---|---|
| justhodl-health-monitor | step 84 builds the HTML dashboard | 6523 |

## Log
- `00:58:27`   Built zip: 6,523 bytes
## Creating new Lambda

- `00:58:31` ✅   Created justhodl-health-monitor
## Test invoke (sync) — see what dashboard looks like

- `00:58:39`   Status: 200
- `00:58:39` ✅   Invoke clean
- `00:58:39`   Payload preview: {"statusCode": 200, "body": "{\"generated_at\": \"2026-04-25T00:58:32.297878+00:00\", \"checked_at_unix\": 1777078718, \"duration_sec\": 6.545604, \"system_status\": \"red\", \"counts\": {\"green\": 21, \"yellow\": 2, \"red\": 1, \"info\": 0, \"unknown\": 5}, \"total_components\": 29, \"components\": [{\"id\": \"s3:edge-data.json\", \"type\": \"s3_file\", \"key\": \"edge-data.json\", \"note\": \"Composite ML risk score, regime. edge-engine every 6h.\", \"severity\": \"critical\", \"known_broken\
## Inspect dashboard.json output

- `00:58:39`   System status: red
- `00:58:39`   Counts: {'green': 21, 'yellow': 2, 'red': 1, 'info': 0, 'unknown': 5}
- `00:58:39`   Total components: 29
- `00:58:39`   Duration: 6.5s
- `00:58:39` 
- `00:58:39`   Top issues (first 10 non-green):
- `00:58:39`     [red    ] critical     s3:edge-data.json                                  
- `00:58:39`     [yellow ] critical     s3:repo-data.json                                  
- `00:58:39`     [yellow ] important    s3:screener/data.json                              
- `00:58:39`     [unknown] critical     eb:justhodl-outcome-checker-daily                  An error occurred (AccessDeniedException) when calling the DescribeRule operatio
- `00:58:39`     [unknown] critical     eb:justhodl-outcome-checker-weekly                 An error occurred (AccessDeniedException) when calling the DescribeRule operatio
- `00:58:39`     [unknown] critical     eb:justhodl-calibrator-weekly                      An error occurred (AccessDeniedException) when calling the DescribeRule operatio
- `00:58:39`     [unknown] nice_to_have s3:predictions.json                                
- `00:58:39`     [unknown] nice_to_have s3:data.json                                       
- `00:58:39` Done
