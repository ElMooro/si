# Diagnose ai-brief HTTP 400

**Status:** success  
**Duration:** 1.7s  
**Finished:** 2026-05-04T19:12:08+00:00  

## Log
- `19:12:07`   ✓ key from justhodl-ai-brief.ANTHROPIC_KEY, len=108, prefix=sk-ant-api03…
# Test 1: tiny prompt to validate model + key

- `19:12:07`   status: 400
- `19:12:07`   ✗ error: {"type":"error","error":{"type":"invalid_request_error","message":"Your credit balance is too low to access the Anthropic API. Please go to Plans & Billing to upgrade or purchase credits."},"request_id":"req_011Cai4QSAzPc9o25DDdbV1q"}
# Test 2: reconstruct snapshot, measure size

- `19:12:08`   total snapshot chars: 248,043
- `19:12:08`   size by source (chars):
- `19:12:08`     momentum                      80,619
- `19:12:08`     correlation_breaks            38,656
- `19:12:08`     calibration                   34,833
- `19:12:08`     earnings_pead                 29,894
- `19:12:08`     insider_buys                  15,119
- `19:12:08`     sectors                       13,441
- `19:12:08`     macro_surprise                11,259
- `19:12:08`     auction_stress                 8,992
- `19:12:08`     alerts                         5,247
- `19:12:08`     intelligence                   4,819
- `19:12:08`     allocator                      4,804
- `19:12:08`     asymmetric_setups                120
- `19:12:08`     risk_sizer                       120
- `19:12:08`     eurodollar_stress                120
- `19:12:08`   full snapshot JSON chars: 362,337
- `19:12:08`   approx total prompt chars: 364,337
# Test 3: real prompt, capture 400 body

- `19:12:08`   with max_tokens=2500: status=400
- `19:12:08`   error body: {"type":"error","error":{"type":"invalid_request_error","message":"Your credit balance is too low to access the Anthropic API. Please go to Plans & Billing to upgrade or purchase credits."},"request_id":"req_011Cai4QWvzUvczKuQ9y7hJp"}
# Test 4: with smaller max_tokens=1024

- `19:12:08`   with max_tokens=1024: status=400
- `19:12:08`   error body: {"type":"error","error":{"type":"invalid_request_error","message":"Your credit balance is too low to access the Anthropic API. Please go to Plans & Billing to upgrade or purchase credits."},"request_id":"req_011Cai4QXQGHJ58JR93btACU"}
# Test 5: minimal subset

- `19:12:08`   tiny prompt with max_tokens=2500: status=400
- `19:12:08`   error body: {"type":"error","error":{"type":"invalid_request_error","message":"Your credit balance is too low to access the Anthropic API. Please go to Plans & Billing to upgrade or purchase credits."},"request_id":"req_011Cai4QXnLUUnYw6QwSiF93"}
