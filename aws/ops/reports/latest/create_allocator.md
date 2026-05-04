# Create justhodl-allocator + 4h schedule

**Status:** success  
**Duration:** 6.7s  
**Finished:** 2026-05-04T18:39:57+00:00  

## Log
- `18:39:51`   zip size: 5,625b
- `18:39:51` ✅   ✓ created
# EventBridge schedule (every 4 hours)

- `18:39:56` ✅   ✓ wired
# Smoke test

- `18:39:57`   status: 200  duration: 1.3s
- `18:39:57`   resp: {"statusCode": 200, "body": "{\"regime\": \"BALANCED_NEUTRAL\", \"n_overweights\": 1, \"n_underweights\": 1, \"cash_pct\": 20.0, \"duration_s\": 0.6}"}
# S3 verify — current allocation

- `18:39:57`   regime_headline: BALANCED_NEUTRAL
- `18:39:57`   n_rules_applied: 10/10
- `18:39:57`   cash_buffer: 20.0%
- `18:39:57` 
- `18:39:57`   📊 ASSET SCORES (sorted highest → lowest):
- `18:39:57`     💻 QQQ   Nasdaq 100                score= +21.0  call=OVERWEIGHT    conviction=MEDIUM n=5
- `18:39:57`     📈 SPY   S&P 500                   score= +13.0  call=TILT_LONG     conviction=LOW    n=4
- `18:39:57`     🌾 DBC   Broad Commodities         score= +10.0  call=TILT_LONG     conviction=LOW    n=2
- `18:39:57`     🌍 EEM   Emerging Markets          score=  +5.0  call=TILT_LONG     conviction=LOW    n=1
- `18:39:57`     🥇 GLD   Gold                      score=  +2.0  call=NEUTRAL       conviction=FLAT   n=3
- `18:39:57`     🌐 EFA   EAFE Developed            score=  +0.0  call=NEUTRAL       conviction=FLAT   n=0
- `18:39:57`     💳 HYG   High Yield Credit         score=  +0.0  call=NEUTRAL       conviction=FLAT   n=0
- `18:39:57`     💵 UUP   US Dollar                 score=  +0.0  call=NEUTRAL       conviction=FLAT   n=0
- `18:39:57`     ₿ BTC   Bitcoin                   score=  +0.0  call=NEUTRAL       conviction=FLAT   n=0
- `18:39:57`     🏢 IWM   Russell 2000              score=  -5.0  call=TILT_SHORT    conviction=LOW    n=2
- `18:39:57`     📊 VXX   VIX Futures               score=  -5.0  call=TILT_SHORT    conviction=LOW    n=2
- `18:39:57`     📜 IEF   7-10 Year Treasury        score= -10.0  call=TILT_SHORT    conviction=LOW    n=2
- `18:39:57`     🏛️ TLT   20+ Year Treasury         score= -20.0  call=UNDERWEIGHT   conviction=MEDIUM n=2
- `18:39:57` 
- `18:39:57`   💼 RECOMMENDED WEIGHTS:
- `18:39:57`     QQQ    32.9%
- `18:39:57`     SPY    20.4%
- `18:39:57`     CASH   20.0%
- `18:39:57`     DBC    15.7%
- `18:39:57`     EEM    7.8%
- `18:39:57`     GLD    3.1%
- `18:39:57` 
- `18:39:57`   🟢 OVERWEIGHTS: QQQ
- `18:39:57`   🔴 UNDERWEIGHTS: TLT
