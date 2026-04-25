# Verify both Lambda fixes after CI auto-deploy

**Status:** success  
**Duration:** 591.4s  
**Finished:** 2026-04-25T23:45:26+00:00  

## Data

| ai_smoke_pass | altman_n | altman_pct | cache_updated | n_stocks |
|---|---|---|---|---|
| True | 0 | 0.0 | True | 503 |

## Log
## A. AI Lambda — wait for deploy

- `23:38:36` ⚠   ⚠ Cutoff not crossed — current LastModified: 2026-04-25T23:34:39
## B. Smoke-test AI Lambda with AAPL

- `23:38:48` ✅   ✅ Returned 200 in 12.1s
- `23:38:48`   Company: Apple Inc. (Technology)
- `23:38:48`   Price: $271.06  P/E=33.9  ROE=1.599
- `23:38:48`   Cached: False  Model: claude-haiku-4-5-20251001
- `23:38:48` 
- `23:38:48`   AI Description: Apple designs, manufactures, and sells iPhones, Macs, iPads, wearables, and accessories, along with AppleCare and cloud services. Revenue is driven by hardware sales (iPhones ~52% of total) and high-m
- `23:38:48` 
- `23:38:48`   Bull thesis:  Services segment durability, AI integration upside, and capital discipline support premium valuation despite mature hardware markets.
- `23:38:48`   Bull drivers: ['Services segment expansion (subscriptions, cloud, fintech via Apple Pay)', 'AI/on-device intelligence features driving upgrade cycles', 'Share buybacks reducing share count and accreating EPS despite modest revenue growth']
- `23:38:48`   Bear thesis:  Valuation stretched relative to growth; China exposure and iPhone saturation pose downside risks amid macro slowdown.
- `23:38:48`   Bear risks:   ['iPhone revenue decline if China demand falters or competition from Android intensifies', 'Services growth deceleration if installed base growth stalls', 'Multiple compression if macro environment weakens and discount rates rise, given 33.9x P/E']
- `23:38:48` 
- `23:38:48`   horizon_1m    : bull=$288  base=$275  bear=$255
- `23:38:48`   horizon_1q    : bull=$320  base=$305  bear=$265
- `23:38:48`   horizon_1y    : bull=$380  base=$330  bear=$240
- `23:38:48` 
- `23:38:48`   Data quality: high
## C. Screener Lambda — wait for deploy

- `23:41:53` ⚠   ⚠ Cutoff not crossed — current LastModified: 2026-04-25T23:34:47
## D. Async-invoke screener

- `23:41:54`   Pre-mtime: 2026-04-25 23:27:12+00:00
- `23:41:54` ✅   Async invoke queued (StatusCode=202)
## E. Poll S3 for screener cache update (up to 12 min)

- `23:42:24`   +30s  cache mtime: 23:27:12
- `23:42:54`   +60s  cache mtime: 23:27:12
- `23:43:25`   +91s  cache mtime: 23:27:12
- `23:43:55`   +121s  cache mtime: 23:27:12
- `23:44:25`   +151s  cache mtime: 23:27:12
- `23:44:55`   +181s  cache mtime: 23:27:12
- `23:45:26`   +212s  cache mtime: 23:45:10
- `23:45:26` ✅   ✅ Cache updated after 212s
## F. Altman Z coverage in screener cache

- `23:45:26`   Total stocks: 503
- `23:45:26`   altmanZ populated: 0/503 (0.0%)
- `23:45:26`   sma50 populated:   499/503 (99.2%)
- `23:45:26` ✗ 
  ❌ Altman Z still null on every stock — fix didn't take
- `23:45:26` Done
