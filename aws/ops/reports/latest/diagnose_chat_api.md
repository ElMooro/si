# Diagnose chat-api HTTP 400

**Status:** success  
**Duration:** 6.5s  
**Finished:** 2026-04-22T23:40:47+00:00  

## Data

| detail | status |
|---|---|
| {"type":"error","error":{"type":"invalid_request_error","message":"prompt is too long: 213889 tokens > 200000 maximum"},"request_id":"req_011CaKhBjCHtkwcYber1DtDM"} | got_anthropic_error |

## Log
## Step 1: patch except to include HTTPError body

- `23:40:40` ✅   Patched — HTTPError now returns Anthropic body
## Step 2: deploy patched chat-api

- `23:40:44` ✅   Deployed (1494 bytes)
## Step 3: re-invoke with minimal message

- `23:40:47`   Outer response:
- `23:40:47`     {"statusCode": 500, "headers": {"Access-Control-Allow-Origin": "*", "Access-Control-Allow-Headers": "Content-Type", "Access-Control-Allow-Methods": "POST,OPTIONS"}, "body": "{\"error\": \"HTTP 400: Bad Request\", \"anthropic_body\": \"{\\\"type\\\":\\\"error\\\",\\\"error\\\":{\\\"type\\\":\\\"invalid_request_error\\\",\\\"message\\\":\\\"prompt is too long: 213889 tokens > 200000 maximum\\\"},\\\"request_id\\\":\\\"req_011CaKhBjCHtkwcYber1DtDM\\\"}\", \"key_prefix\": \"sk-ant-api03\"}"}
- `23:40:47` 
- `23:40:47`   Inner body parsed:
- `23:40:47`     error: HTTP 400: Bad Request
- `23:40:47`     anthropic_body: {"type":"error","error":{"type":"invalid_request_error","message":"prompt is too long: 213889 tokens > 200000 maximum"},"request_id":"req_011CaKhBjCHtkwcYber1DtDM"}
- `23:40:47`     key_prefix: sk-ant-api03
- `23:40:47` 
- `23:40:47`   ANTHROPIC ERROR: {"type":"error","error":{"type":"invalid_request_error","message":"prompt is too long: 213889 tokens > 200000 maximum"},"request_id":"req_011CaKhBjCHtkwcYber1DtDM"}
- `23:40:47` Done
