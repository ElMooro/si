## 1. Gates + factor-regime bootstrap

**Status:** success  
**Duration:** 115.3s  
**Finished:** 2026-07-08T01:46:29+00:00  

## Data

| action | appetite | breadth | crowded | factor_page | fr_body | fr_env | fr_err | fr_exists | fr_secs | ir_body | ir_env | ir_err | ir_gate | ir_page_v2 | ir_secs | ladder | leading | pairs_ok | thrusts | top5 | version |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  |  |  |  |  | 4 |  | True |  |  |  |  |  |  |  |  |
| ops-side create_function |  |  |  |  |  |  |  | False |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  | 3 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  | {"statusCode": 200, "body": "{\"ok\": true, \"risk_appetite\": 45.9, \"thrusts\": 1, \"leading\": 6}"} |  | None |  | 3.8 |  |  |  |  |  |  |  |  |  |  |  |  |
|  | 45.9 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | ["SMALL_SIZE", "EQUAL_WEIGHT_BREADTH", "QUALITY", "MINVOL_DEFENSE", "SPECULATIVE_APPETITE", "BIOTECH_RISK_APPETITE"] | 11 | ["USMV/SPY (MINVOL_DEFENSE)"] |  |  |
|  |  |  |  |  |  |  |  |  |  | {"statusCode": 200, "body": "{\"ok\": true, \"regime\": \"STRONG\", \"top\": \"CIBR\", \"absorption\": 0, \"warns\": 1}"} |  | None |  |  | 17.2 |  |  |  |  |  |  |
|  |  | [["CIBR", {"pct_above_50d": 75, "n_priced": 12, "read": "BROAD"}], ["XBI", {"pct_above_50d": 100, "n_priced": 12, "read": "BROAD"}], ["IBB", {"pct_above_50d": 100, "n_priced": 12, "read": "BROAD"}], ["XLV", {"pct_above_50d": 83, "n_priced": 12, "read": "BROAD"}], ["KRE", {"pct_above_50d": 100, "n_pr | ["XLK"] |  |  |  |  |  |  |  |  |  |  |  |  | 33 |  |  |  | [["CIBR", 99, 16.73, -2, false], ["XBI", 98, 13.69, 6, false], ["IBB", 95, 4.07, 28, false], ["XLV", 94, 3.13, 104, false], ["KRE", 92, 1.12, 105, false]] | 2.0 |
|  |  |  |  | True |  |  |  |  |  |  |  |  |  | True |  |  |  |  |  |  |  |

## Log
- `01:46:07` ✅   ✓ created rule factor-regime-daily
- `01:46:07` ✅   ✓ target → justhodl-factor-regime
- `01:46:07` ✅   ✓ added invoke permission
## 2. Invoke factor-regime

## 3. Invoke industry-rotation v2

## 4. Pages live

- `01:46:29` ✅ THEORY STACK LIVE: FR appetite 45.9 (RISK_ON), thrusts ["USMV/SPY (MINVOL_DEFENSE)"] | IR v2 top [["CIBR", 99, 16.73, -2, false], ["XBI", 98, 13.69, 6, false], ["IBB", 95, 4.07, 28, false]] | crowded ["XLK"]
- `01:46:29` FAILS=0 WARNS=0
