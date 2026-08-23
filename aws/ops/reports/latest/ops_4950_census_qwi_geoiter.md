## P0 per-state time-range probe

**Status:** failure  
**Duration:** 3.0s  
**Finished:** 2026-08-23T16:57:53+00:00  

## Error

```
SystemExit: 1
```

## Log
- `16:57:50`   H-from1990     HTTP 400 rows=-1    error: this dataset requires a bounded date/time range
- `16:57:51`   I-from1990Q1   HTTP 400 rows=-1    error: this dataset requires a bounded date/time range
- `16:57:52`   J-year2022     HTTP 200 rows=4     [["Emp","HirA","Sep","EarnS","time","state"], ["1940492","371102","366307","4753","2022-Q1","01"], ["1945287","434891","410889","4739","2022-Q2","01"]
- `16:57:53` P0 FAIL no per-state range form returned rows
