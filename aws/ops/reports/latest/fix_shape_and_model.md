# Fix 3 broken Lambdas: khalid_index shape + retired model

**Status:** success  
**Duration:** 34.9s  
**Finished:** 2026-04-22T23:38:16+00:00  

## Data

| edits | lambda_name | retest | size | status | verdict |
|---|---|---|---|---|---|
| 2 | morning-intelligence |  |  | deployed |  |
| 2 | signal-logger |  |  | deployed |  |
| 1 | chat-api |  |  | deployed |  |
|  |  | justhodl-morning-intelligence | 469 |  | OK |
|  |  | justhodl-signal-logger | 47 |  | OK |
|  |  | justhodl-chat-api |  |  | INNER_ERROR |

## Log
## justhodl-morning-intelligence

- `23:37:41` ✅   Applied 2 edit(s)
- `23:37:45` ✅   Deployed (6 KB)
## justhodl-signal-logger

- `23:37:45` ✅   Applied 2 edit(s)
- `23:37:49` ✅   Deployed (3 KB)
## justhodl-chat-api

- `23:37:49` ✅   Applied 1 edit(s)
- `23:37:52` ✅   Deployed (1 KB)
## Re-invoking each fixed Lambda to confirm green

- `23:38:04` ✅   justhodl-morning-intelligence: green (469 bytes)
- `23:38:04`     preview: {"statusCode": 200, "body": "{\"success\": true, \"khalid\": {\"score\": 48, \"regime\": \"NEUTRAL\", \"signals\": [[\"DXY\", -12, \"118.1\"], [\"HY Spread\", 5, \"2.85%\"], [\"Une
- `23:38:13` ✅   justhodl-signal-logger: green (47 bytes)
- `23:38:13`     preview: {"statusCode": 200, "body": "{\"logged\": 27}"}
- `23:38:16` ⚠   justhodl-chat-api: 200 wrapper but inner status>=400
- `23:38:16`     {"statusCode": 500, "headers": {"Access-Control-Allow-Origin": "*", "Access-Control-Allow-Headers": "Content-Type", "Access-Control-Allow-Methods": "POST,OPTIONS"}, "body": "{\"error\": \"HTTP Error 400: Bad Request\"}"}
- `23:38:16` Done
