## 1. Gates + factor-regime bootstrap

**Status:** success  
**Duration:** 126.8s  
**Finished:** 2026-07-08T02:39:58+00:00  

## Data

| appetite | bands | breadth | bs_setups | confirmed | credit | credit_mentions | crowded | factor_page | flows_n | fr_body | fr_env | fr_err | fr_exists | fr_secs | full_basis | ir_body | ir_env | ir_err | ir_gate | ir_page_v2 | ir_secs | ladder | leading | min_mult | pairs_ok | reads | scorecard_n | thrusts | top5 | top6 | version | volume_n |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 4 |  | True |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  | True |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  | 3 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  | {"statusCode": 200, "body": "{\"ok\": true, \"risk_appetite\": 45.9, \"thrusts\": 1, \"leading\": 6}"} |  | None |  | 4.1 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
| 45.9 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | ["SMALL_SIZE", "EQUAL_WEIGHT_BREADTH", "QUALITY", "MINVOL_DEFENSE", "SPECULATIVE_APPETITE", "BIOTECH_RISK_APPETITE"] |  | 11 |  |  | ["USMV/SPY (MINVOL_DEFENSE)"] |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | {"statusCode": 200, "body": "{\"ok\": true, \"regime\": \"STRONG\", \"top\": \"XBI\", \"absorption\": 0, \"warns\": 1}"} |  | None |  |  | 30.9 |  |  |  |  |  |  |  |  |  |  |  |
|  |  | [["XBI", {"pct_above_50d": 100, "n_priced": 12, "read": "BROAD"}], ["CIBR", {"pct_above_50d": 75, "n_priced": 12, "read": "BROAD"}], ["XTN", {"pct_above_50d": 100, "n_priced": 12, "read": "BROAD"}], ["IBB", {"pct_above_50d": 100, "n_priced": 12, "read": "BROAD"}], ["XLV", {"pct_above_50d": 83, "n_pr |  |  |  |  | ["XLK"] |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 40 |  |  |  |  |  |  | [["XBI", 98, 13.69, 6, false], ["CIBR", 98, 16.73, -2, false], ["XTN", 95, 6.6, -43, false], ["IBB", 94, 4.07, 28, false], ["XLV", 94, 3.13, 104, false]] |  | 3.0 |  |
|  | {"LEADERSHIP": 1, "STRONG_WATCH": 8, "NEUTRAL": 13, "WEAK": 12, "AVOID": 6} |  |  |  |  |  |  |  | 0 |  |  |  |  |  | 5 |  |  |  |  |  |  |  |  |  |  |  | 40 |  |  | [["XBI", 84.3, "LEADERSHIP", "full100"], ["CIBR", 75.6, "STRONG_WATCH", "full100"], ["XTN", 72.8, "STRONG_WATCH", "full100"], ["IBB", 75.1, "STRONG_WATCH", "full100"], ["XLV", 61.0, "NEUTRAL", "full100"], ["IYT", 68.2, "STRONG_WATCH", "core85"]] |  | 40 |
|  |  |  |  |  | {"XBI": {"read": "OK", "median_z": 28.44, "distress_pct": 0, "n_scored": 7}, "CIBR": {"read": "OK", "median_z": 9.53, "distress_pct": 0, "n_scored": 8}, "XTN": {"read": "DANGER", "median_z": 1.44, "distress_pct": 75, "n_scored": 8}, "IBB": {"read": "OK", "median_z": 13.46, "distress_pct": 0, "n_scored": 8}, "XLV": {"read": "OK", "median_z": 3.38, "distress_pct": 0, "n_scored": 8}, "XME": {"read":  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  | ["KWEB"] |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | {"RESILIENT_LEADER": 4, "WEAK_BOTH_WAYS": 3, "HIGH_BETA_PROFILE": 19, "DEFENSIVE_ONLY": 14} |  |  |  |  |  |  |
|  |  |  | 50 |  |  | 0 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 0.88 |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  | True |  |  |  |  |  |  |  |  |  |  |  | True |  |  |  |  |  |  |  |  |  |  |  |  |

## Log
- `02:39:16`   rule already correct: factor-regime-daily (cron(28 21 * * ? *))
- `02:39:16` ✅   ✓ target → justhodl-factor-regime
- `02:39:16` ✅   ✓ added invoke permission
## 2. Invoke factor-regime

## 3. Invoke industry-rotation v2

## 4b. Best-setups credit-penalty path

## 4. Pages live

- `02:39:58` ✅ THEORY STACK LIVE: FR appetite 45.9 (RISK_ON), thrusts ["USMV/SPY (MINVOL_DEFENSE)"] | IR v2 top [["XBI", 98, 13.69, 6, false], ["CIBR", 98, 16.73, -2, false], ["XTN", 95, 6.6, -43, false]] | crowded ["XLK"]
- `02:39:58` FAILS=0 WARNS=0
