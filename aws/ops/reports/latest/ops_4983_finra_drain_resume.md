## P1 catalog + resume census

**Status:** failure  
**Duration:** 5046.3s  
**Finished:** 2026-08-25T21:50:42+00:00  

## Error

```
SystemExit: 1
```

## Log
- `20:26:36`   otcMarket metadata refused (HTTP Error 404: Not Found) -> seeds
- `20:26:48`   fixedIncomeMarket metadata refused (HTTP Error 404: Not Found) -> seeds
- `20:26:51`   universe=9 invalid=10 already-banked(4982)=5
## P2 drain (checkpointed)

- `20:26:51`   fixedIncomeMarket__treasuryWeeklyAggregates          FAIL Expecting value: line 1 column 1 (char 0)
- `21:03:58`   otcMarket__regShoDaily                               rows=6820408     97.5MB
- `21:03:59`   otcMarket__weeklyDownloadDetails                     rows=4988         0.0MB
- `21:44:40`   otcMarket__weeklySummary                             rows=5755000    215.5MB
- `21:44:41`   banked=8 rows=22212147 failed=1 remaining=0 phase=DRAIN
## P4 substance

- `21:44:41`   otcMarket__weeklyDownloadDetails rows=4988 span 2021-12-06 .. 2026-08-24
- `21:44:41` P4 FAIL
## P5 card

- `21:50:42` P5 PASS note=FULL Query-API warehouse (finra-full v1): 8/9 datasets · 22212147 rows since inception · 700MB · phase DRAIN · daily rediscovery
- `21:50:42` ops 4983 RED: P4
