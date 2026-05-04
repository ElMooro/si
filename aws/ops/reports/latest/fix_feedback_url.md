# Create Function URL for justhodl-feedback + publish manifest

**Status:** success  
**Duration:** 16.5s  
**Finished:** 2026-05-04T12:50:46+00:00  

## Log
- `12:50:30` ✅   ✓ created URL: https://vmzexqk56frz3dvpo6nioe5ylm0kijlj.lambda-url.us-east-1.on.aws/
- `12:50:30` ✅   ✓ public invoke permission added
- `12:50:31` ✅   ✓ published s3://justhodl-dashboard-live/feedback-url.json
- `12:50:46`   ✓ smoke https://vmzexqk56frz3dvpo6nioe5ylm0kijlj.lambda-url.us-east-1.on.aws/signals → 200
- `12:50:46`     body[:200]: {"ok": true, "signals": [{"signal_id": "d0881bb8-1e8e-4af0-9b28-9f6a0bd6affa", "signal_type": "corr_break_top_pair", "variant": null, "signal_value": "vixcls_dgs10", "direction": null, "confidence": 0
