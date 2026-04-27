# Create justhodl-insider-trades Lambda + EB rule

**Status:** success  
**Duration:** 13.0s  
**Finished:** 2026-04-27T21:56:26+00:00  

## Log
- `21:56:13`   zip: 8868 bytes
## 1. Lambda function

- `21:56:14`   Lambda exists — updating code + config
- `21:56:19` ✅   ✓ updated Lambda justhodl-insider-trades
## 2. EB rule + permissions

- `21:56:19`   EB rule exists: state=ENABLED, schedule=rate(30 minutes)
- `21:56:19` ✅   ✓ EB target → justhodl-insider-trades
- `21:56:19` ✅   ✓ added invoke permission (AllowEB-justhodl-insider-trades-30min-1777326979)
## 3. Smoke test

- `21:56:19`   Triggering smoke-test invocation (this populates initial data)…
- `21:56:26`   StatusCode: 200
- `21:56:26` ✅   ✓ smoke test passed
- `21:56:26`      atom feed:   100 filings
- `21:56:26`      filings_seen:        100
- `21:56:26`      xml_fetched:         24
- `21:56:26`      no_issuer_or_ticker: 1
- `21:56:26`      no_nonderiv_table:   0
- `21:56:26`      txn_seen:            32
- `21:56:26`      skipped_not_buy:     10
- `21:56:26`      skipped_threshold:   0
- `21:56:26`      buys_kept:           2
- `21:56:26`      sells_kept:          20
- `21:56:26`      ─────────────────────────────────
- `21:56:26`      buys (window):       8
- `21:56:26`      value (window):      $7,715,890
- `21:56:26`      companies:           5
- `21:56:26`      clusters:            0
- `21:56:26`      duration:            5.9s
- `21:56:26`      errors:              76
- `21:56:26`      ─── fetch errlog ───────────────────
- `21:56:26`      http_404_index                  42
- `21:56:26`      http_429_html                   39
- `21:56:26`      http_404_html                   36
- `21:56:26`      http_429_index                  33
- `21:56:26`      no_xml_in_listing               1
## 4. Next steps

- `21:56:26`   - Frontend page /insiders.html consumes data/insider-trades.json
- `21:56:26`   - Health monitor will pick up the new file via expectations.py
- `21:56:26`   - Future code changes auto-deploy through deploy-lambdas.yml
