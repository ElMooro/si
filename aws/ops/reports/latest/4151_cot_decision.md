# ops 4151 — COT wire decision

**Status:** success  
**Duration:** 0.3s  
**Finished:** 2026-07-31T01:55:52+00:00  

## Data

| code_overlap | his_markets | store_contracts | symbols_servable_if_columns | tff_group_columns | with_cftc_code |
|---|---|---|---|---|---|
|  |  | 29 |  |  | 29 |
|  |  |  |  | 0 |  |
|  | 21 |  |  |  |  |
| 7 |  |  | 130 |  |  |

## Log
- `01:55:52`   cftc_codes: [["001602", "ZW"], ["002602", "ZC"], ["005602", "ZS"], ["020601", "ZB"], ["022651", "HO"], ["023651", "NG"], ["033661", "CT"], ["042601", "ZT"], ["043602", "ZN"], ["044601", "ZF"], ["067651", "CL"], ["076651", "PL"], ["080732", "SB"], ["083731", "KC"], ["084691", "SI"], ["085692", "HG"], ["088691", "GC"], ["090741", "6C"], ["092741", "6S"], ["096742", "6B"], ["097741", "6J"], ["098662", "DX"], ["099741", "6E"], ["111
- `01:55:52`   weekly row columns: ["report_date", "open_interest", "noncommercial_long", "noncommercial_short", "net_speculator", "commercial_long", "commercial_short", "net_commercial", "managed_money_long", "managed_money_short", "net_managed_money", "speculator_long_ratio"]
- `01:55:52`   tff-ish: []
- `01:55:52`   his codes: [["020601", 16], ["020604", 16], ["045601", 7], ["067651", 12], ["088691", 14], ["088695", 1], ["092741", 10], ["097741", 2], ["098662", 9], ["099741", 67], ["112741", 2], ["132741", 140], ["133741", 35], ["133742", 11], ["134741", 16], ["134742", 11], ["146021", 28], ["232741", 3], ["240741", 1], ["299741", 8], ["399741", 10]]
- `01:55:52`   overlap: [["098662", "DX"], ["067651", "CL"], ["099741", "6E"], ["088691", "GC"], ["020601", "ZB"], ["092741", "6S"], ["097741", "6J"]]
- `01:55:52`   his field suffixes: {"CP_S": 20, "CP_L": 18, "NRP_L": 18, "AMP_SPREAD": 16, "DP_S": 15, "DP_L": 14, "NRP_S": 14, "AMP_S": 13, "LMP_L": 13, "DP_SPREAD": 13, "TAM_S": 12, "OI": 12, "TT": 12, "TAM_SPREAD": 11, "AMP_L": 10, "TTR_L": 9}
- `01:55:52` ✅ DECISION DATA — overlap 7 markets, tff cols 0
