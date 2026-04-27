# Create justhodl-insider-trades Lambda + EB rule

**Status:** success  
**Duration:** 28.2s  
**Finished:** 2026-04-27T18:14:54+00:00  

## Log
- `18:14:26`   zip: 8680 bytes
## 1. Lambda function

- `18:14:26`   Lambda exists — updating code + config
- `18:14:29` ✅   ✓ updated Lambda justhodl-insider-trades
## 2. EB rule + permissions

- `18:14:29`   EB rule exists: state=ENABLED, schedule=rate(30 minutes)
- `18:14:29` ✅   ✓ EB target → justhodl-insider-trades
- `18:14:30` ✅   ✓ added invoke permission (AllowEB-justhodl-insider-trades-30min-1777313669)
## 3. Smoke test

- `18:14:30`   Triggering smoke-test invocation (this populates initial data)…
- `18:14:54`   StatusCode: 200
- `18:14:54` ✅   ✓ smoke test passed
- `18:14:54`      atom feed:   100 filings
- `18:14:54`      filings_seen:        100
- `18:14:54`      xml_fetched:         0
- `18:14:54`      no_issuer_or_ticker: 0
- `18:14:54`      no_nonderiv_table:   0
- `18:14:54`      txn_seen:            0
- `18:14:54`      skipped_not_buy:     0
- `18:14:54`      skipped_threshold:   0
- `18:14:54`      buys_kept:           0
- `18:14:54`      sells_kept:          0
- `18:14:54`      ─────────────────────────────────
- `18:14:54`      buys (window):       0
- `18:14:54`      value (window):      $0
- `18:14:54`      companies:           0
- `18:14:54`      clusters:            0
- `18:14:54`      duration:            23.5s
- `18:14:54`      errors:              100
## 4. Next steps

- `18:14:54`   - Frontend page /insiders.html consumes data/insider-trades.json
- `18:14:54`   - Health monitor will pick up the new file via expectations.py
- `18:14:54`   - Future code changes auto-deploy through deploy-lambdas.yml
