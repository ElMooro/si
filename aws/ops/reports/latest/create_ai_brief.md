# Deploy justhodl-ai-brief

**Status:** success  
**Duration:** 8.6s  
**Finished:** 2026-05-04T19:09:45+00:00  

## Log
- `19:09:37`   zip size: 5,270b
- `19:09:37` ✅   ✓ anthropic key sourced from justhodl-morning-intelligence.ANTHROPIC_KEY  (len=108)
- `19:09:38` ✅   ✓ created
# EventBridge schedule (every 4h at :05)

- `19:09:43` ✅   ✓ wired
# Smoke test (will call Claude — ~15-30s)

- `19:09:45`   status: 200  duration: 1.9s
- `19:09:45`   resp: {"statusCode": 200, "body": "{\"duration_s\": 0.91, \"brief_chars\": 54, \"snapshot_keys\": [\"as_of\", \"intelligence\", \"calibration\", \"sectors\", \"momentum\", \"allocator\", \"asymmetric_setups\", \"risk_sizer\", \"auction_stress\", \"eurodollar_stress\", \"macro_surprise\", \"insider_buys\", \"earnings_pead\", \"correlation_breaks\", \"alerts\"], \"error\": \"HTTP Error 400: Bad Request\"}
# S3 verify

- `19:09:45`   generated_at: 2026-05-04T19:09:44.914872+00:00
- `19:09:45`   duration_s: 0.91
- `19:09:45`   model: None
- `19:09:45`   brief_md_chars: 54
- `19:09:45`   usage: None
- `19:09:45`   ⚠ error: HTTP Error 400: Bad Request
- `19:09:45` 
- `19:09:45` === BRIEF PREVIEW (first 3000 chars) ===
- `19:09:45`     # Brief generation failed
- `19:09:45`     
- `19:09:45`     HTTP Error 400: Bad Request
