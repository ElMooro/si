## 1. Deploy gate (env persists per 2968 fix)

**Status:** success  
**Duration:** 84.1s  
**Finished:** 2026-07-08T01:09:59+00:00  

## Data

| absorption | body | deploy_age_s | env_vars | err | ladder_n | page_ok | regime | resilience_index | secs | soldiers | top5 | wires_ok |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  | 60 | 4 |  |  |  |  |  |  |  |  |  |
|  | {"statusCode": 200, "body": "{\"ok\": true, \"regime\": \"STRONG\", \"top\": \"CIBR\", \"absorption\": 0, \"warns\": 1}"} |  |  | None |  |  |  |  | 8.0 |  |  |  |
| [] |  |  |  |  | 33 |  | {"state": "STRONG", "spy": 747.71, "spy_above_sma20": true, "spy_above_sma50": true, "risk_regime_score": 24.2, "risk_regime": "MILD_RISK_ON"} |  |  |  | [["CIBR", 99, null], ["XBI", 98, null], ["JETS", 98, null], ["IBB", 96, null], ["IYT", 96, null]] |  |
|  |  |  |  |  |  | True |  |  |  |  |  | 2 |
|  |  |  |  |  |  |  |  |  |  | [{"etf": "CIBR", "holdings_n": 12, "reason": null, "resilient_n": 0}, {"etf": "XBI", "holdings_n": 12, "reason": null, "resilient_n": 0}, {"etf": "JETS", "holdings_n": 12, "reason": null, "resilient_n": 1}, {"etf": "IBB", "holdings_n": 12, "reason": null, "resilient_n": 0}, {"etf": "IYT", "holdings_n": 12, "reason": null, "resilient_n": 1}] |  |  |
|  |  |  |  |  |  |  |  | resilience_index: 62 tickers loaded |  |  |  |  |

## Log
## 2. Invoke

## 3. Doc verify

## 4. Pages live

## 5. Soldiers diagnostic

- `01:09:59` ✅ SOLDIERS FIX VERIFIED: regime STRONG | ladder 33 | top [["CIBR", 99, null], ["XBI", 98, null], ["JETS", 98, null]] | soldiers [{"etf": "CIBR", "holdings_n": 12, "reason": null, "resilient_n": 0}, {"etf": "XBI", "holdings_n": 12, "reason": null, "resilient_n": 0}, {"etf": "JETS", "holdings_n": 12, "reason": null, "resilient_n": 1}, {"etf": "IBB", "holdings_n": 12, "reason": null, "resilient_n": 0}, {"etf": "IYT", "holdings_n": 12, "reason": null, "resilient_n": 1}]
- `01:09:59` FAILS=0 WARNS=0
