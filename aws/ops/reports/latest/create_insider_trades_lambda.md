# Create justhodl-insider-trades Lambda + EB rule

**Status:** success  
**Duration:** 15.3s  
**Finished:** 2026-04-27T18:08:43+00:00  

## Log
- `18:08:28`   zip: 8120 bytes
## 1. Lambda function

- `18:08:28`   Lambda exists — updating code + config
- `18:08:28`   another update in progress; retrying in 1s (attempt 1/6)
- `18:08:30`   another update in progress; retrying in 2s (attempt 2/6)
- `18:08:34` ✅   ✓ updated Lambda justhodl-insider-trades
## 2. EB rule + permissions

- `18:08:35`   EB rule exists: state=ENABLED, schedule=rate(30 minutes)
- `18:08:35` ✅   ✓ EB target → justhodl-insider-trades
- `18:08:35` ✅   ✓ added invoke permission (AllowEB-justhodl-insider-trades-30min-1777313315)
## 3. Smoke test

- `18:08:35`   Triggering smoke-test invocation (this populates initial data)…
- `18:08:43`   StatusCode: 200
- `18:08:43` ✅   ✓ smoke test passed
- `18:08:43`      atom feed:   100 filings
- `18:08:43`      filings_seen:        100
- `18:08:43`      xml_fetched:         7
- `18:08:43`      no_issuer_or_ticker: 7
- `18:08:43`      no_nonderiv_table:   0
- `18:08:43`      txn_seen:            0
- `18:08:43`      skipped_not_buy:     0
- `18:08:43`      skipped_threshold:   0
- `18:08:43`      buys_kept:           0
- `18:08:43`      sells_kept:          0
- `18:08:43`      ─────────────────────────────────
- `18:08:43`      buys (window):       0
- `18:08:43`      value (window):      $0
- `18:08:43`      companies:           0
- `18:08:43`      clusters:            0
- `18:08:43`      duration:            7.3s
- `18:08:43`      errors:              93
## 4. Next steps

- `18:08:43`   - Frontend page /insiders.html consumes data/insider-trades.json
- `18:08:43`   - Health monitor will pick up the new file via expectations.py
- `18:08:43`   - Future code changes auto-deploy through deploy-lambdas.yml
