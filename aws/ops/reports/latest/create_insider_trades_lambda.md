# Create justhodl-insider-trades Lambda + EB rule

**Status:** success  
**Duration:** 14.9s  
**Finished:** 2026-04-27T18:06:41+00:00  

## Log
- `18:06:26`   zip: 7513 bytes
## 1. Lambda function

- `18:06:26`   Lambda exists — updating code + config
- `18:06:29` ✅   ✓ updated Lambda justhodl-insider-trades
## 2. EB rule + permissions

- `18:06:29`   EB rule exists: state=ENABLED, schedule=rate(30 minutes)
- `18:06:29` ✅   ✓ EB target → justhodl-insider-trades
- `18:06:29` ✅   ✓ added invoke permission (AllowEB-justhodl-insider-trades-30min-1777313189)
## 3. Smoke test

- `18:06:29`   Triggering smoke-test invocation (this populates initial data)…
- `18:06:41`   StatusCode: 200
- `18:06:41` ✅   ✓ smoke test passed
- `18:06:41`      buys:        0
- `18:06:41`      value:       $0
- `18:06:41`      companies:   0
- `18:06:41`      clusters:    0
- `18:06:41`      duration:    10.4s
- `18:06:41`      errors:      0
## 4. Next steps

- `18:06:41`   - Frontend page /insiders.html consumes data/insider-trades.json
- `18:06:41`   - Health monitor will pick up the new file via expectations.py
- `18:06:41`   - Future code changes auto-deploy through deploy-lambdas.yml
