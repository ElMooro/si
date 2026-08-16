# ops 4734 -- Census depth probe with the real key (value never printed)

**Status:** success  
**Duration:** 58.5s  
**Finished:** 2026-08-16T04:20:03+00:00  

## Data

| check | value |
|---|---|
| key_retrieved | True |
| range_query_returns_json | True |
| real_data_at_1992-01 | False |
| real_data_at_2000-01 | False |
| real_data_at_2005-01 | False |
| real_data_at_2010-01 | True |
| real_data_at_2013-01 | True |
| real_data_at_2020-01 | True |
| naics_real_data_at_2005-01 | False |
| naics_real_data_at_2013-01 | True |

## Log
## 1. Range-query syntax -- does 'time=from X to Y' work with a real key?

- `04:19:10` range probe result: status=200 is_json=True elapsed_ms=5339
- `04:19:10`   body preview: [["GEN_VAL_MO","COMM_LVL","I_COMMODITY","time"],
["1167461768","HS6","854231","2013-01"],
["1196050876","HS6","854231","2013-02"],
["1260593174","HS6","854231","2013-03"],
["1307322176","HS6","854231","2013-04"],
["1608099447","HS6","854231","2013-05"],
["1419086979","HS6","854231","2013-06"]]
## 2. How far back does real (non-null) monthly data exist -- single-month probes

- `04:19:30`   1992-01: is_json=False has_data_row=False status=204 body[:150]=
- `04:19:33`   2000-01: is_json=False has_data_row=False status=204 body[:150]=
- `04:19:52`   2005-01: is_json=False has_data_row=False status=204 body[:150]=
- `04:19:55`   2010-01: is_json=True has_data_row=True status=200 body[:150]=[["GEN_VAL_MO","COMM_LVL","I_COMMODITY","time"],
["388466943","HS6","854231","2010-01"]]
- `04:19:58`   2013-01: is_json=True has_data_row=True status=200 body[:150]=[["GEN_VAL_MO","COMM_LVL","I_COMMODITY","time"],
["1167461768","HS6","854231","2013-01"]]
- `04:20:02`   2020-01: is_json=True has_data_row=True status=200 body[:150]=[["GEN_VAL_MO","COMM_LVL","I_COMMODITY","time"],
["1624108686","HS6","854231","2020-01"]]
## 3. NAICS endpoint too -- same COMM_LVL/HS shape doesn't apply, confirm its own earliest

- `04:20:03`   NAICS 2005-01: is_json=False body[:150]=
- `04:20:03`   NAICS 2013-01: is_json=True body[:150]=[["GEN_VAL_MO","time","NAICS"],
["3031577614","2013-01","334413"]]
## Summary

- `04:20:03` Key value never printed. This gives the real earliest usable month and confirms whether range queries work, so the backfill script can be written against measured behavior instead of assumption.
