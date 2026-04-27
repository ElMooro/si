# Create/update justhodl-gdelt-sentiment Lambda + EB rule

**Status:** success  
**Duration:** 6.2s  
**Finished:** 2026-04-27T18:48:51+00:00  

## Log
- `18:48:45`   zip: 4419 bytes
## 1. Lambda

- `18:48:46`   Lambda exists — updating
- `18:48:48` ✅   ✓ updated justhodl-gdelt-sentiment
- `18:48:49` ✅   ✓ reserved concurrency = 1
## 2. EB rule + permissions

- `18:48:49`   rule already correct: justhodl-gdelt-sentiment-30min (rate(30 minutes))
- `18:48:49` ✅   ✓ target → justhodl-gdelt-sentiment
- `18:48:49` ✅   ✓ added invoke permission
## 3. Smoke test

- `18:48:49`   invoking justhodl-gdelt-sentiment…
- `18:48:51` ✅   ✓ smoke test passed
- `18:48:51`     articles_total           1739
- `18:48:51`     financial_articles       43
- `18:48:51`     avg_tone                 -1.31
- `18:48:51`     extreme_negative         2
- `18:48:51`     extreme_positive         0
- `18:48:51`     fetch_duration_s         1.2
