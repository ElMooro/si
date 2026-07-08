## 1. Gates + factor-regime bootstrap

**Status:** success  
**Duration:** 139.5s  
**Finished:** 2026-07-08T02:14:48+00:00  

## Data

| appetite | breadth | confirmed | credit | crowded | factor_page | fr_body | fr_env | fr_err | fr_exists | fr_secs | ir_body | ir_env | ir_err | ir_gate | ir_page_v2 | ir_secs | ladder | leading | pairs_ok | reads | thrusts | top5 | version |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  |  |  |  |  |  | 4 |  | True |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  | True |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  | 3 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  | {"statusCode": 200, "body": "{\"ok\": true, \"risk_appetite\": 45.9, \"thrusts\": 1, \"leading\": 6}"} |  | None |  | 3.8 |  |  |  |  |  |  |  |  |  |  |  |  |  |
| 45.9 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | ["SMALL_SIZE", "EQUAL_WEIGHT_BREADTH", "QUALITY", "MINVOL_DEFENSE", "SPECULATIVE_APPETITE", "BIOTECH_RISK_APPETITE"] | 11 |  | ["USMV/SPY (MINVOL_DEFENSE)"] |  |  |
|  |  |  |  |  |  |  |  |  |  |  | {"statusCode": 200, "body": "{\"ok\": true, \"regime\": \"STRONG\", \"top\": \"CIBR\", \"absorption\": 0, \"warns\": 1}"} |  | None |  |  | 30.5 |  |  |  |  |  |  |  |
|  | [["CIBR", {"pct_above_50d": 75, "n_priced": 12, "read": "BROAD"}], ["XBI", {"pct_above_50d": 100, "n_priced": 12, "read": "BROAD"}], ["XTN", {"pct_above_50d": 100, "n_priced": 12, "read": "BROAD"}], ["XLV", {"pct_above_50d": 83, "n_priced": 12, "read": "BROAD"}], ["IBB", {"pct_above_50d": 100, "n_pr |  |  | ["XLK"] |  |  |  |  |  |  |  |  |  |  |  |  | 40 |  |  |  |  | [["CIBR", 98, 16.73, -2, false], ["XBI", 98, 13.69, 6, false], ["XTN", 95, 6.6, -43, false], ["XLV", 94, 3.13, 104, false], ["IBB", 94, 4.07, 28, false]] | 2.1 |
|  |  |  | {"CIBR": {"read": "OK", "median_z": 9.53, "distress_pct": 0, "n_scored": 8}, "XBI": {"read": "OK", "median_z": 28.44, "distress_pct": 0, "n_scored": 7}, "XTN": {"read": "DANGER", "median_z": 1.44, "distress_pct": 75, "n_scored": 8}, "XLV": {"read": "OK", "median_z": 3.38, "distress_pct": 0, "n_scored": 8}, "IBB": {"read": "OK", "median_z": 13.46, "distress_pct": 0, "n_scored": 8}, "XME": {"read":  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  | ["KWEB"] |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | {"WEAK_BOTH_WAYS": 3, "RESILIENT_LEADER": 4, "HIGH_BETA_PROFILE": 19, "DEFENSIVE_ONLY": 14} |  |  |  |
|  |  |  |  |  | True |  |  |  |  |  |  |  |  |  | True |  |  |  |  |  |  |  |  |

## Log
- `02:13:53`   rule already correct: factor-regime-daily (cron(28 21 * * ? *))
- `02:13:53` ✅   ✓ target → justhodl-factor-regime
- `02:13:53` ✅   ✓ added invoke permission
## 2. Invoke factor-regime

## 3. Invoke industry-rotation v2

## 4. Pages live

- `02:14:48` ✅ THEORY STACK LIVE: FR appetite 45.9 (RISK_ON), thrusts ["USMV/SPY (MINVOL_DEFENSE)"] | IR v2 top [["CIBR", 98, 16.73, -2, false], ["XBI", 98, 13.69, 6, false], ["XTN", 95, 6.6, -43, false]] | crowded ["XLK"]
- `02:14:48` FAILS=0 WARNS=0
