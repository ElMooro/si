# Re-probe bonds + diagnose macro-brief

**Status:** success  
**Duration:** 7.9s  
**Finished:** 2026-04-26T11:56:43+00:00  

## Log
## A. /agent/bonds — re-probe with cache-buster

- `11:56:42`   ✅ HTTP 200 len=2813
- `11:56:42`   body preview: {"timestamp": "2026-04-26T11:56:42.319411", "bond_indices": {"US_CORP_MASTER": {"current": 0.8, "date": "2026-04-23", "changes": {"1D": 0.010000000000000009, "1W": -0.010000000000000009, "1M": -0.10999999999999999, "3M": 0.0}, "signal": "CHECK_DATA"}, "US_HIGH_YIELD": {"current": 2.86, "date": "2026-04-23", "changes": {"1D": 0.020000000000000018, "1W": 0.020000000000000018, "1M": -0.31000000000000005, "3M": -0.020000000000000018}, "signal": "NORMAL"}, "AAA_CORP": {"current": 0.35, "date": "2026-04-23", "changes": {"1D": 0.019999999999999962, "1W": 0.019999999999999962, "1M": -0.10000000000000003, "3M": 0.0}, "signal": "CHECK_DATA"}, "BBB_CORP": {"current": 1.0, "date": "2026-04-23", "changes": {"1D": 0.010000000000000009, "1W": -0.020000000000000018, "1M": -0.1200000000000001, "3M": -0.020000000000000018}, "signal": "CHECK_DATA"}, "CCC_AND_LOWER": {"current": 9.15, "date": "2026-04-23", "changes": {"1D": 0.019999999999999574, "1W": -0.16000000000000014, "1M": -0.4599999999999991, "3M":
## B. macro-brief direct Function URL probe

- `11:56:43`   status: 200 fnError: none
- `11:56:43`   payload first 1500B: {"statusCode": 200, "body": "{\"status\": \"success\", \"key\": \"daily_briefs/2026-04-26_brief.json\"}"}
## C. S3 /daily_briefs/ listing

- `11:56:43`   0 briefs found:
- `11:56:43` Done
