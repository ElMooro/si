# Create justhodl-insider-trades Lambda + EB rule

**Status:** success  
**Duration:** 9.6s  
**Finished:** 2026-04-27T18:03:04+00:00  

## Log
- `18:02:55`   zip: 7302 bytes
## 1. Lambda function

- `18:02:55`   Lambda missing — creating
- `18:03:00` ✅   ✓ created Lambda justhodl-insider-trades
- `18:03:00` ✅   ✓ reserved concurrency = 1
- `18:03:00` ✅   ✓ Function URL: https://n77em6c43e3y4jjpgciua332xy0wtupc.lambda-url.us-east-1.on.aws/
## 2. EB rule + permissions

- `18:03:01` ✅   ✓ created EB rule justhodl-insider-trades-30min
- `18:03:01` ✅   ✓ EB target → justhodl-insider-trades
- `18:03:01` ✅   ✓ added invoke permission (AllowEB-justhodl-insider-trades-30min-1777312981)
## 3. Smoke test

- `18:03:01`   Triggering smoke-test invocation (this populates initial data)…
- `18:03:04`   StatusCode: 200
- `18:03:04` ✅   ✓ smoke test passed
- `18:03:04`      buys:        0
- `18:03:04`      value:       $0
- `18:03:04`      companies:   0
- `18:03:04`      clusters:    0
- `18:03:04`      duration:    2.3s
- `18:03:04`      errors:      0
## 4. Next steps

- `18:03:04`   - Frontend page /insiders.html consumes data/insider-trades.json
- `18:03:04`   - Health monitor will pick up the new file via expectations.py
- `18:03:04`   - Future code changes auto-deploy through deploy-lambdas.yml
